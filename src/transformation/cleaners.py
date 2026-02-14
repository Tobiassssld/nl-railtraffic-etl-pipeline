# src/transformation/cleaners.py

import pandas as pd
import json
from datetime import datetime
import re

class DisruptionCleaner:
    """
    清洗NS API返回的延误数据
    """
    
    def __init__(self):
        """
        初始化清洗器
        """
        # 定义类型映射（NS API可能返回不同的type值）
        self.type_mapping = {
            'verstoring': 'disruption',
            'werkzaamheden': 'maintenance',
            'calamiteit': 'calamity',
            'storing': 'disruption'
        }
    
    def clean(self, raw_data):
        """
        主清洗函数
        
        参数:
            raw_data: list - API返回的原始数据（字典列表）
        
        返回:
            pd.DataFrame - 清洗后的数据
        """
        if not raw_data:
            print("⚠️  没有数据需要清洗")
            return pd.DataFrame()
        
        print(f"🧹 开始清洗 {len(raw_data)} 条记录...")
        
        # 步骤1: 转成DataFrame
        df = pd.DataFrame(raw_data)
        
        # 步骤2: 提取基本字段
        df = self._extract_basic_fields(df)
        
        # 步骤3: 处理时间字段
        df = self._process_timestamps(df)
        
        # 步骤4: 计算派生字段
        df = self._calculate_metrics(df)
        
        # 步骤5: 提取受影响车站
        df = self._extract_stations(df)
        
        # 步骤6: 数据验证和清理
        df = self._validate_and_clean(df)
        
        print(f"✅ 清洗完成！保留 {len(df)} 条有效记录")
        
        return df
    
    def _extract_basic_fields(self, df):
        """
        步骤1: 提取基本字段
        """
        print("  📋 提取基本字段...")
        
        # 重命名列（如果需要）
        if 'id' in df.columns:
            df = df.rename(columns={'id': 'disruption_id'})
        
        # 标准化type字段（转小写，映射到统一名称）
        if 'type' in df.columns:
            df['type'] = df['type'].str.lower()
            df['type'] = df['type'].map(self.type_mapping).fillna(df['type'])
        
        # 清理title字段（去除多余空格）
        if 'title' in df.columns:
            df['title'] = df['title'].str.strip()
            # 删除过短的标题（可能是测试数据）
            df.loc[df['title'].str.len() < 5, 'title'] = None
        
        return df
    
    def _process_timestamps(self, df):
        """
        步骤2: 处理时间字段
        """
        print("  ⏰ 处理时间戳...")
        
        # 转换开始时间（统一转成UTC）
        if 'start' in df.columns:
            df['start_time'] = pd.to_datetime(df['start'], errors='coerce', utc=True)
        
        # 转换结束时间
        if 'end' in df.columns:
            df['end_time'] = pd.to_datetime(df['end'], errors='coerce', utc=True)
            
            # 标记进行中的延误（没有结束时间）
            df['is_ongoing'] = df['end_time'].isna()
            
            # 对于进行中的延误，设置临时结束时间为"现在+2小时"
            now = pd.Timestamp.now(tz='UTC')
            df.loc[df['is_ongoing'], 'end_time'] = now + pd.Timedelta(hours=2)
        
        return df
    
    def _calculate_metrics(self, df):
        """
        步骤3: 计算业务指标
        """
        print("  🔢 计算业务指标...")
        
        # 计算持续时间（分钟）- 使用float64类型
        if 'start_time' in df.columns and 'end_time' in df.columns:
            valid_times = df['start_time'].notna() & df['end_time'].notna()
            
            # 直接用float64类型（支持NaN）
            df['duration_minutes'] = (
                (df['end_time'] - df['start_time']).dt.total_seconds() / 60
            )
            
            # 清理无效值
            df.loc[~valid_times, 'duration_minutes'] = None
            df.loc[df['duration_minutes'] < 0, 'duration_minutes'] = None
        
        # 计算影响级别（1-5）
        df['impact_level'] = df.apply(self._calculate_impact_level, axis=1)
        
        return df
    
    def _calculate_impact_level(self, row):
        """
        业务逻辑：根据类型和持续时间计算影响级别
        
        规则：
        - 取消（cancellation）: 5级
        - 灾难（calamity）: 5级
        - 维护（maintenance）且>4小时: 4级
        - 维护（maintenance）且<4小时: 3级
        - 延误（disruption）且>2小时: 4级
        - 延误（disruption）且>1小时: 3级
        - 其他: 2级
        """
        disruption_type = row.get('type', '')
        duration = row.get('duration_minutes', 0)
        
        # 处理缺失值
        if pd.isna(duration):
            duration = 0
        
        # 应用规则
        if disruption_type == 'calamity':
            return 5
        elif 'cancel' in str(disruption_type).lower():
            return 5
        elif disruption_type == 'maintenance':
            if duration > 240:  # 4小时
                return 4
            else:
                return 3
        elif disruption_type == 'disruption':
            if duration > 120:  # 2小时
                return 4
            elif duration > 60:  # 1小时
                return 3
            else:
                return 2
        else:
            return 2
    
    def _extract_stations(self, df):
        """
        步骤4: 提取受影响的车站
        """
        print("  🚉 提取受影响车站...")
        
        affected_stations_list = []
        
        for idx, row in df.iterrows():
            stations = set()  # 用set避免重复
            
            try:
                # 方法1: 从'section'字段提取
                if 'section' in row:
                    section = row['section']
                    # 安全的检查方式
                    if section is not None and not (isinstance(section, float) and pd.isna(section)):
                        if isinstance(section, dict):
                            # 提取起点和终点
                            if 'stations' in section and section['stations']:
                                for station in section['stations']:
                                    if isinstance(station, dict) and 'uicCode' in station:
                                        stations.add(station['uicCode'])
                
                # 方法2: 从'timespans'字段提取
                if 'timespans' in row:
                    timespans = row['timespans']
                    if timespans is not None and not (isinstance(timespans, float) and pd.isna(timespans)):
                        if isinstance(timespans, list):
                            for timespan in timespans:
                                if isinstance(timespan, dict) and 'situation' in timespan:
                                    situation = timespan['situation']
                                    if isinstance(situation, dict) and 'stations' in situation:
                                        for station in situation['stations']:
                                            if isinstance(station, dict):
                                                code = station.get('stationCode', '')
                                                if code:
                                                    stations.add(code)
                
                # 方法3: 从title中提取（作为备选）
                if not stations and 'title' in row:
                    title = row.get('title', '')
                    if isinstance(title, str):
                        # 简单的正则匹配大写字母组合
                        potential_codes = re.findall(r'\b[A-Z]{2,5}\b', title)
                        stations.update(potential_codes)
            
            except Exception as e:
                # 单条记录失败不影响整体
                pass
            
            # 转成逗号分隔的字符串
            affected_stations_list.append(','.join(sorted(stations)) if stations else None)
        
        df['affected_stations'] = affected_stations_list
        
        return df
    
    def _validate_and_clean(self, df):
        """
        步骤6: 数据验证和最终清理
        """
        print("  ✓ 验证数据质量...")
        
        # 删除没有disruption_id的记录
        if 'disruption_id' in df.columns:
            before_count = len(df)
            df = df[df['disruption_id'].notna()]
            removed = before_count - len(df)
            if removed > 0:
                print(f"    ⚠️  删除了 {removed} 条缺少ID的记录")
        
        # 确保impact_level在1-5范围内
        if 'impact_level' in df.columns:
            df['impact_level'] = df['impact_level'].clip(lower=1, upper=5)
        
        # 添加元数据列
        df['is_resolved'] = 0  # 新数据默认未解决
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # 只保留需要的列（删除API返回的原始嵌套字段）
        required_columns = [
            'disruption_id', 'type', 'title', 'description',
            'start_time', 'end_time', 'duration_minutes',
            'impact_level', 'affected_stations',
            'is_resolved', 'created_at', 'updated_at'
        ]
        
        # 保留存在的列
        existing_columns = [col for col in required_columns if col in df.columns]
        df = df[existing_columns]
        
        return df


# ===== 测试代码 =====
if __name__ == "__main__":
    print("=== DisruptionCleaner 测试 ===\n")
    
    # 模拟API返回的数据（简化版）
    sample_data = [
        {
            'id': 'prio-12345',
            'type': 'verstoring',
            'title': 'Storing tussen Amsterdam en Utrecht',
            'start': '2025-02-14T08:30:00+0100',
            'end': '2025-02-14T10:00:00+0100',
            'description': 'Door een sein storing...',
            'timespans': [
                {
                    'situation': {
                        'stations': [
                            {'stationCode': 'ASD'},
                            {'stationCode': 'UTR'}
                        ]
                    }
                }
            ]
        },
        {
            'id': 'prio-67890',
            'type': 'werkzaamheden',
            'title': 'Werkzaamheden Rotterdam',
            'start': '2025-02-14T06:00:00+0100',
            'end': '2025-02-14T18:00:00+0100',
            'description': 'Onderhoud spoor...'
        }
    ]
    
    # 创建清洗器并运行
    cleaner = DisruptionCleaner()
    cleaned_df = cleaner.clean(sample_data)
    
    # 显示结果
    print("\n📊 清洗结果预览：")
    print(cleaned_df[['disruption_id', 'type', 'duration_minutes', 'impact_level']])
    
    print("\n📋 数据类型：")
    print(cleaned_df.dtypes)