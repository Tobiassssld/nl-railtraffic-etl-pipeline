# src/pipeline.py

import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import os

from ingestion.api_client import NSAPIClient
from storage.database import Database
from transformation.cleaners import DisruptionCleaner
from config import setup_logging


class ETLPipeline:
    """
    Extract → Transform → Load pipeline for NS disruption data.

    Supports two database backends, selected automatically via env vars:
      - PostgreSQL on AWS RDS  (AWS_RDS_HOST set)
      - Local SQLite           (fallback for development)

    The only SQL differences between backends are:
      1. Placeholder syntax:  %s (PostgreSQL) vs ? (SQLite)
      2. Upsert syntax:       ON CONFLICT DO NOTHING vs INSERT OR IGNORE
      3. Date functions:      CURRENT_DATE / NOW() vs DATE('now')

    All three are handled via self.database.mode and self.database.placeholder,
    so the rest of the code stays identical across backends.
    """

    def __init__(self):
        self.logger = setup_logging()
        self.logger.info("=" * 60)
        self.logger.info("NS Rail Traffic ETL Pipeline starting")
        self.logger.info("=" * 60)

        try:
            self.api_client = NSAPIClient()
            self.database   = Database()
            self.cleaner    = DisruptionCleaner()
            self.logger.info(" All components initialised.")
        except Exception as e:
            self.logger.error(f" Initialisation failed: {e}")
            raise

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run(self):
        try:
            self.logger.info("\n Step 1: Extract from NS API...")
            raw_data = self._extract()
            if not raw_data:
                self.logger.warning("No data retrieved — pipeline stopping.")
                return

            self.logger.info("\n Step 2: Transform / clean...")
            cleaned_data = self._transform(raw_data)
            if cleaned_data.empty:
                self.logger.warning("No valid records after cleaning — pipeline stopping.")
                return

            self.logger.info("\n Step 3: Load into database...")
            self._load(raw_data, cleaned_data)

            self.logger.info("\n Step 4: Generate report...")
            self._generate_report()

            self.logger.info("\n" + "=" * 60)
            self.logger.info(" Pipeline completed successfully.")
            self.logger.info("=" * 60)

        except Exception as e:
            self.logger.error(f"\n Pipeline failed: {e}")
            self.logger.exception("Full traceback:")
            raise

    # ------------------------------------------------------------------
    # Step 1: Extract
    # ------------------------------------------------------------------

    def _extract(self):
        try:
            disruptions = self.api_client.fetch_disruptions()
            self.logger.info(f"   Retrieved {len(disruptions)} disruption records.")
            return disruptions
        except Exception as e:
            self.logger.error(f"   Extraction failed: {e}")
            return []

    # ------------------------------------------------------------------
    # Step 2: Transform
    # ------------------------------------------------------------------

    def _transform(self, raw_data):
        try:
            cleaned_df = self.cleaner.clean(raw_data)
            self.logger.info(f"   {len(cleaned_df)} valid records after cleaning.")

            if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
                out_path = Path("/tmp") / f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            else:
                out_path = Path("data/processed") / f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                out_path.parent.mkdir(parents=True, exist_ok=True)

            cleaned_df.to_csv(out_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"   Saved to: {out_path}")
            return cleaned_df
        except Exception as e:
            self.logger.error(f"   Transform failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Step 3: Load
    # ------------------------------------------------------------------

    def _load(self, raw_data, cleaned_data):
        try:
            self.logger.info("   3a. Saving raw JSON...")
            self._save_raw_data(raw_data)

            self.logger.info("   3b. Saving cleaned records...")
            self._save_cleaned_data(cleaned_data)

            self.logger.info("   ✅ Load complete.")
        except Exception as e:
            self.logger.error(f"   Load failed: {e}")
            raise

    def _save_raw_data(self, raw_data):
        """
        Insert raw JSON into raw_disruptions, skipping duplicates.

        Duplicate handling differs by backend:
          PostgreSQL : INSERT ... ON CONFLICT (disruption_id) DO NOTHING
          SQLite     : INSERT OR IGNORE ...

        Both are idempotent — safe to re-run on the same dataset.
        """
        p = self.database.placeholder   # '%s' or '?'
        inserted = skipped = 0

        for item in raw_data:
            disruption_id = item.get('id')
            if not disruption_id:
                continue

            raw_json = json.dumps(item, ensure_ascii=False)

            try:
                if self.database.mode == 'postgres':
                    # ON CONFLICT DO NOTHING is PostgreSQL's clean upsert for
                    # "insert only if not already present" — no extra SELECT needed.
                    self.database.cursor.execute(f"""
                        INSERT INTO raw_disruptions (disruption_id, raw_json)
                        VALUES ({p}, {p})
                        ON CONFLICT (disruption_id) DO NOTHING
                    """, (disruption_id, raw_json))
                    # rowcount == 0 means the row already existed
                    if self.database.cursor.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1
                else:
                    # SQLite equivalent
                    self.database.cursor.execute(f"""
                        INSERT OR IGNORE INTO raw_disruptions (disruption_id, raw_json)
                        VALUES ({p}, {p})
                    """, (disruption_id, raw_json))
                    if self.database.cursor.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1

            except Exception as e:
                self.logger.warning(f"      Failed to save raw record {disruption_id}: {e}")

        self.database.conn.commit()
        self.logger.info(f"      Inserted {inserted}, skipped {skipped} duplicates.")

    def _save_cleaned_data(self, df):
        """
        Upsert cleaned records into the disruptions table.

        PostgreSQL-specific fixes applied here:

        1. Boolean type: pandas stores is_resolved as int (0/1).
           SQLite accepts integers as booleans; PostgreSQL does not.
           Fix: wrap with bool() before passing to psycopg2.

        2. Per-record rollback via SAVEPOINTs:
           A plain conn.rollback() undoes the entire transaction, including
           all previously successful inserts in the same batch.
           SAVEPOINTs let us roll back only the one failed record and
           continue with the rest — same behaviour as SQLite's INSERT OR IGNORE.

        3. NaT → None: pandas NaT becomes float NaN after strftime().
           PostgreSQL rejects NaN for TIMESTAMP columns.
           Fix: _safe_ts() returns None for any null-like datetime value.
        """
        p = self.database.placeholder

        def _safe_ts(val):
            """Return an ISO datetime string, or None for any null-like value."""
            if val is None:
                return None
            try:
                if pd.isna(val):
                    return None
            except (TypeError, ValueError):
                pass
            try:
                return pd.Timestamp(val).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                return None

        inserted = updated = 0

        for _, row in df.iterrows():
            # Set a savepoint before each record so a failure only rolls back
            # that one record, not the entire batch.
            if self.database.mode == 'postgres':
                self.database.cursor.execute("SAVEPOINT sp")

            try:
                self.database.cursor.execute(
                    f"SELECT id FROM disruptions WHERE disruption_id = {p}",
                    (row['disruption_id'],)
                )
                exists = self.database.cursor.fetchone()

                if exists:
                    self.database.cursor.execute(f"""
                        UPDATE disruptions SET
                            type               = {p},
                            title              = {p},
                            description        = {p},
                            start_time         = {p},
                            end_time           = {p},
                            duration_minutes   = {p},
                            impact_level       = {p},
                            affected_stations  = {p},
                            updated_at         = {p}
                        WHERE disruption_id = {p}
                    """, (
                        row.get('type'),
                        row.get('title'),
                        row.get('description'),
                        _safe_ts(row.get('start_time')),
                        _safe_ts(row.get('end_time')),
                        row.get('duration_minutes') if pd.notna(row.get('duration_minutes')) else None,
                        row.get('impact_level'),
                        row.get('affected_stations'),
                        _safe_ts(row.get('updated_at')),
                        row['disruption_id']
                    ))
                    if self.database.mode == 'postgres':
                        self.database.cursor.execute("RELEASE SAVEPOINT sp")
                    updated += 1
                else:
                    self.database.cursor.execute(f"""
                        INSERT INTO disruptions (
                            disruption_id, type, title, description,
                            start_time, end_time, duration_minutes,
                            impact_level, affected_stations,
                            is_resolved, created_at, updated_at
                        ) VALUES ({p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p},{p})
                    """, (
                        row['disruption_id'],
                        row.get('type'),
                        row.get('title'),
                        row.get('description'),
                        _safe_ts(row.get('start_time')),
                        _safe_ts(row.get('end_time')),
                        row.get('duration_minutes') if pd.notna(row.get('duration_minutes')) else None,
                        row.get('impact_level'),
                        row.get('affected_stations'),
                        bool(row.get('is_resolved', False)),  # int → bool for PostgreSQL
                        _safe_ts(row.get('created_at')),
                        _safe_ts(row.get('updated_at'))
                    ))
                    if self.database.mode == 'postgres':
                        self.database.cursor.execute("RELEASE SAVEPOINT sp")
                    inserted += 1

            except Exception as e:
                self.logger.warning(f"      Failed to save record {row['disruption_id']}: {e}")
                # Roll back only this record, keeping all previous inserts intact
                if self.database.mode == 'postgres':
                    self.database.cursor.execute("ROLLBACK TO SAVEPOINT sp")
                else:
                    self.database.conn.rollback()

        self.database.conn.commit()
        self.logger.info(f"      Inserted {inserted}, updated {updated}.")

    # ------------------------------------------------------------------
    # Step 4: Report
    # ------------------------------------------------------------------

    def _generate_report(self):
        """
        Query today's stats from the database.

        Date truncation syntax differs by backend:
          PostgreSQL : created_at::DATE = CURRENT_DATE
          SQLite     : DATE(created_at) = DATE('now')
        """
        try:
            if self.database.mode == 'postgres':
                date_filter = "created_at::DATE = CURRENT_DATE"
            else:
                date_filter = "DATE(created_at) = DATE('now')"

            self.database.cursor.execute(f"""
                SELECT
                    COUNT(*)                                                  AS total,
                    SUM(CASE WHEN type = 'disruption'  THEN 1 ELSE 0 END)   AS disruptions,
                    SUM(CASE WHEN type = 'maintenance' THEN 1 ELSE 0 END)   AS maintenance,
                    SUM(CASE WHEN type = 'calamity'    THEN 1 ELSE 0 END)   AS calamity,
                    AVG(duration_minutes)                                     AS avg_duration,
                    MAX(impact_level)                                         AS max_impact
                FROM disruptions
                WHERE {date_filter}
            """)

            stats = self.database.cursor.fetchone()

            self.logger.info("\n   📈 Today's stats:")
            self.logger.info(f"      Total records  : {stats[0]}")
            self.logger.info(f"      Disruptions    : {stats[1]}")
            self.logger.info(f"      Maintenance    : {stats[2]}")
            self.logger.info(f"      Calamities     : {stats[3]}")
            if stats[4]:
                self.logger.info(f"      Avg duration   : {stats[4]:.1f} min")
            self.logger.info(f"      Max impact     : {stats[5]}")

        except Exception as e:
            self.logger.warning(f"   Report generation failed: {e}")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

def main():
    try:
        pipeline = ETLPipeline()
        pipeline.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()