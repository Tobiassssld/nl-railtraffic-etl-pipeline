# src/storage/database.py

import os
import sqlite3
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

RDS_HOST     = os.getenv('AWS_RDS_HOST')
RDS_PORT     = os.getenv('AWS_RDS_PORT', '5432')
RDS_DBNAME   = os.getenv('AWS_RDS_DBNAME', 'postgres')
RDS_USER     = os.getenv('AWS_RDS_USER', 'postgres')
RDS_PASSWORD = os.getenv('AWS_RDS_PASSWORD')


class Database:
    """
    Database manager with automatic backend selection:
      - AWS_RDS_HOST set   → PostgreSQL on RDS (pg8000)
      - AWS_RDS_HOST unset → local SQLite (development fallback)

    pg8000 is a pure-Python PostgreSQL driver — no C extensions,
    no binary compatibility issues on Lambda.
    It implements DB-API 2.0, so cursor.execute() / conn.commit()
    calls in pipeline.py are identical to psycopg2.

    Key parameter differences vs psycopg2:
      psycopg2 dbname=          → pg8000 database=
      psycopg2 connect_timeout= → pg8000 timeout=
      psycopg2 sslmode='require'→ pg8000 ssl_context=True
    """

    def __init__(self, db_path="data/nl_rail.db"):
        if RDS_HOST and RDS_PASSWORD:
            self._init_postgres()
        else:
            self._init_sqlite(db_path)

    # ------------------------------------------------------------------
    # Backend initialisation
    # ------------------------------------------------------------------

    def _init_postgres(self, max_retries=3):
        """
        Connect to PostgreSQL on RDS using pg8000 (pure Python).
        Retries with linear backoff for cold-start delays.
        """
        import pg8000.dbapi

        self.mode = 'postgres'
        self.placeholder = '%s'

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Connecting to RDS PostgreSQL (attempt {attempt}/{max_retries})...")
                self.conn = pg8000.dbapi.connect(
                    host=RDS_HOST,
                    port=int(RDS_PORT),
                    database=RDS_DBNAME,   # pg8000 uses 'database', not 'dbname'
                    user=RDS_USER,
                    password=RDS_PASSWORD,
                    timeout=30,            # pg8000 uses 'timeout', not 'connect_timeout'
                    ssl_context=True       # pg8000 uses ssl_context=True for sslmode='require'
                )
                self.conn.autocommit = False
                self.cursor = self.conn.cursor()
                print(f"✅ Connected to RDS: {RDS_HOST}/{RDS_DBNAME}")
                return
            except Exception as e:
                if attempt < max_retries:
                    wait = 30 * attempt
                    print(f"   Connection failed, retrying in {wait}s... ({e})")
                    time.sleep(wait)
                else:
                    raise

    def _init_sqlite(self, db_path):
        """
        Fallback: local SQLite for development without cloud credentials.
        """
        self.mode = 'sqlite'
        self.placeholder = '?'

        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        print(f"✅ Connected to SQLite: {db_path}")

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def initialize_schema(self):
        """
        Run schema.sql against the active backend.
        Safe to call on every startup — IF NOT EXISTS prevents duplicates.
        """
        schema_path = Path("src/storage/schema.sql")
        schema_sql = schema_path.read_text(encoding='utf-8')

        if self.mode == 'postgres':
            statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
            for stmt in statements:
                self.cursor.execute(stmt)
            self.conn.commit()
            print("✅ PostgreSQL schema initialised.")
        else:
            self.cursor.executescript(schema_sql)
            self.conn.commit()
            print("✅ SQLite schema initialised.")

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def show_tables(self):
        if self.mode == 'postgres':
            self.cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
        else:
            self.cursor.execute("""
                SELECT name FROM sqlite_master
                WHERE type = 'table'
                ORDER BY name;
            """)

        tables = self.cursor.fetchall()
        print("\nTables in database:")
        for (name,) in tables:
            print(f"  - {name}")

    def close(self):
        self.conn.close()
        print("Database connection closed.")


# ===== Quick connectivity test =====
if __name__ == "__main__":
    db = Database()
    db.initialize_schema()
    db.show_tables()
    db.close()