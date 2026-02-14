# src/config.py

import logging
from pathlib import Path
from datetime import datetime

def setup_logging():
    """
    配置日志系统
    """
    # 确保logs文件夹存在
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # 日志文件名（按日期）
    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"
    
    # 配置日志格式
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),  # 写入文件
            logging.StreamHandler()  # 同时打印到终端
        ]
    )
    
    return logging.getLogger(__name__)