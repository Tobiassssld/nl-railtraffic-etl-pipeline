# src/pipeline.py

import sys
import json
from pathlib import Path
from datetime import datetime
import pandas as pd

# 导入我们之前写的模块
from ingestion.api_client import NSAPIClient
from storage.database import Database
from transformation.cleaners import DisruptionCleaner
from config import setup_logging


class ETLPipeline:
    """
    完整的ETL流程
    Extract（提取） → Transform（转换） → Load（加载）
    """
    
    def __init__(self):
        """
        初始化pipeline组件
        """
        self.logger = setup_logging()
        self.logger.info("=" * 60)
        self.logger.info("🚀 NS Rail Traffic ETL Pipeline 启动")
        self.logger.info("=" * 60)
        
        # 初始化各个组件
        try:
            self.api_client = NSAPIClient()
            self.database = Database()
            self.cleaner = DisruptionCleaner()
            self.logger.info("✅ 所有组件初始化成功")
        except Exception as e:
            self.logger.error(f"❌ 初始化失败: {e}")
            raise
    
    def run(self):
        """
        执行完整的ETL流程
        """
        try:
            # ===== 步骤1: Extract（提取）=====
            self.logger.info("\n📥 步骤1: 从NS API提取数据...")
            raw_data = self._extract()
            
            if not raw_data:
                self.logger.warning("⚠️  没有获取到数据，pipeline终止")
                return
            
            # ===== 步骤2: Transform（转换）=====
            self.logger.info("\n🔄 步骤2: 清洗和转换数据...")
            cleaned_data = self._transform(raw_data)
            
            if cleaned_data.empty:
                self.logger.warning("⚠️  清洗后没有有效数据，pipeline终止")
                return
            
            # ===== 步骤3: Load（加载）=====
            self.logger.info("\n💾 步骤3: 加载数据到数据库...")
            self._load(raw_data, cleaned_data)
            
            # ===== 步骤4: 生成报告 =====
            self.logger.info("\n📊 步骤4: 生成统计报告...")
            self._generate_report()
            
            self.logger.info("\n" + "=" * 60)
            self.logger.info("✅ Pipeline执行成功！")
            self.logger.info("=" * 60)
            
        except Exception as e:
            self.logger.error(f"\n❌ Pipeline执行失败: {e}")
            self.logger.exception("详细错误信息：")
            raise
    
    def _extract(self):
        """
        步骤1: 从API提取数据
        
        返回: list - 原始数据
        """
        try:
            disruptions = self.api_client.fetch_disruptions()
            self.logger.info(f"   获取到 {len(disruptions)} 条延误记录")
            return disruptions
        except Exception as e:
            self.logger.error(f"   数据提取失败: {e}")
            return []
    
    def _transform(self, raw_data):
        """
        步骤2: 清洗数据
        
        参数:
            raw_data: list - 原始数据
        
        返回:
            pd.DataFrame - 清洗后的数据
        """
        try:
            cleaned_df = self.cleaner.clean(raw_data)
            self.logger.info(f"   清洗后保留 {len(cleaned_df)} 条有效记录")
            
            # 保存清洗后的数据到CSV（用于检查）
            output_path = Path("data/processed") / f"cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned_df.to_csv(output_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"   清洗后的数据已保存到: {output_path}")
            
            return cleaned_df
            
        except Exception as e:
            self.logger.error(f"   数据清洗失败: {e}")
            return pd.DataFrame()
    
    def _load(self, raw_data, cleaned_data):
        """
        步骤3: 加载数据到数据库
        
        参数:
            raw_data: list - 原始数据
            cleaned_data: pd.DataFrame - 清洗后的数据
        """
        try:
            # 3.1 保存原始数据到 raw_disruptions 表
            self.logger.info("   3.1 保存原始数据...")
            self._save_raw_data(raw_data)
            
            # 3.2 保存清洗后的数据到 disruptions 表
            self.logger.info("   3.2 保存清洗后的数据...")
            self._save_cleaned_data(cleaned_data)
            
            self.logger.info("   ✅ 数据加载完成")
            
        except Exception as e:
            self.logger.error(f"   数据加载失败: {e}")
            raise
    
    def _save_raw_data(self, raw_data):
        """
        保存原始JSON到数据库
        
        使用 INSERT OR IGNORE 避免重复
        """
        inserted = 0
        skipped = 0
        
        for item in raw_data:
            try:
                disruption_id = item.get('id')
                if not disruption_id:
                    continue
                
                # 转成JSON字符串
                raw_json = json.dumps(item, ensure_ascii=False)
                
                # 插入数据库（如果已存在则忽略）
                self.database.cursor.execute("""
                    INSERT OR IGNORE INTO raw_disruptions 
                    (disruption_id, raw_json) 
                    VALUES (?, ?)
                """, (disruption_id, raw_json))
                
                # 检查是否真的插入了（rowcount=1表示插入成功）
                if self.database.cursor.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
                    
            except Exception as e:
                self.logger.warning(f"      保存原始数据失败 (ID: {disruption_id}): {e}")
        
        self.database.conn.commit()
        self.logger.info(f"      插入 {inserted} 条，跳过 {skipped} 条重复数据")
    
    def _save_cleaned_data(self, df):
        """
        保存清洗后的数据到数据库
        
        使用 UPSERT 逻辑：
        - 如果disruption_id已存在 → 更新
        - 如果不存在 → 插入
        """
        # 确保时间列格式正确
        datetime_columns = ['start_time', 'end_time', 'created_at', 'updated_at']
        for col in datetime_columns:
            if col in df.columns:
                # 转成字符串格式（SQLite兼容）
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # 使用pandas的to_sql方法
        # if_exists='append': 如果表存在则追加
        # 但这会导致重复，所以我们用手动的UPSERT
        
        inserted = 0
        updated = 0
        
        for idx, row in df.iterrows():
            try:
                # 检查记录是否已存在
                self.database.cursor.execute(
                    "SELECT id FROM disruptions WHERE disruption_id = ?",
                    (row['disruption_id'],)
                )
                exists = self.database.cursor.fetchone()
                
                if exists:
                    # 更新现有记录
                    self.database.cursor.execute("""
                        UPDATE disruptions SET
                            type = ?,
                            title = ?,
                            description = ?,
                            start_time = ?,
                            end_time = ?,
                            duration_minutes = ?,
                            impact_level = ?,
                            affected_stations = ?,
                            updated_at = ?
                        WHERE disruption_id = ?
                    """, (
                        row.get('type'),
                        row.get('title'),
                        row.get('description'),
                        row.get('start_time'),
                        row.get('end_time'),
                        row.get('duration_minutes') if pd.notna(row.get('duration_minutes')) else None,
                        row.get('impact_level'),
                        row.get('affected_stations'),
                        row.get('updated_at'),
                        row['disruption_id']
                    ))
                    updated += 1
                else:
                    # 插入新记录
                    self.database.cursor.execute("""
                        INSERT INTO disruptions (
                            disruption_id, type, title, description,
                            start_time, end_time, duration_minutes,
                            impact_level, affected_stations,
                            is_resolved, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        row['disruption_id'],
                        row.get('type'),
                        row.get('title'),
                        row.get('description'),
                        row.get('start_time'),
                        row.get('end_time'),
                        row.get('duration_minutes') if pd.notna(row.get('duration_minutes')) else None,
                        row.get('impact_level'),
                        row.get('affected_stations'),
                        row.get('is_resolved', 0),
                        row.get('created_at'),
                        row.get('updated_at')
                    ))
                    inserted += 1
                    
            except Exception as e:
                self.logger.warning(f"      保存记录失败 (ID: {row['disruption_id']}): {e}")
        
        self.database.conn.commit()
        self.logger.info(f"      插入 {inserted} 条，更新 {updated} 条")
    
    def _generate_report(self):
        """
        生成统计报告
        """
        try:
            # 查询当前数据库统计
            self.database.cursor.execute("""
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN type = 'disruption' THEN 1 ELSE 0 END) as disruptions,
                    SUM(CASE WHEN type = 'maintenance' THEN 1 ELSE 0 END) as maintenance,
                    SUM(CASE WHEN type = 'calamity' THEN 1 ELSE 0 END) as calamity,
                    AVG(duration_minutes) as avg_duration,
                    MAX(impact_level) as max_impact
                FROM disruptions
                WHERE DATE(created_at) = DATE('now')
            """)
            
            stats = self.database.cursor.fetchone()
            
            self.logger.info("\n   📈 今日数据统计：")
            self.logger.info(f"      总记录数: {stats[0]}")
            self.logger.info(f"      延误(disruption): {stats[1]}")
            self.logger.info(f"      维护(maintenance): {stats[2]}")
            self.logger.info(f"      灾难(calamity): {stats[3]}")
            self.logger.info(f"      平均持续时间: {stats[4]:.1f} 分钟" if stats[4] else "      平均持续时间: N/A")
            self.logger.info(f"      最高影响级别: {stats[5]}")
            
        except Exception as e:
            self.logger.warning(f"   生成报告失败: {e}")


# ===== 主程序入口 =====
def main():
    """
    主函数
    """
    try:
        pipeline = ETLPipeline()
        pipeline.run()
        
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断程序")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ 程序执行失败: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()