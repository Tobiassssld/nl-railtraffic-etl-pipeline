# src/ingestion/api_client.py

import requests
import json
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv

# 加载.env文件里的配置
load_dotenv()

class NSAPIClient:
    """
    这个类用来从NS（荷兰铁路）API下载数据
    """
    
    def __init__(self):
        """
        初始化：读取API密钥，设置基础URL
        """
        # 从.env文件读取你的API密钥
        self.api_key = os.getenv('NS_API_KEY')
        
        # 检查密钥是否存在
        if not self.api_key:
            raise ValueError("错误！在.env文件里找不到NS_API_KEY")
        
        # NS API的基础网址
        self.base_url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v3"
        
        # 设置请求头（API要求的格式）
        self.headers = {
            'Ocp-Apim-Subscription-Key': self.api_key
        }
    
    def fetch_disruptions(self, max_retries=3):
        """
        下载延误数据，带重试机制
        """
        url = f"{self.base_url}/disruptions"
        
        for attempt in range(1, max_retries + 1):
            try:
                print(f"尝试 {attempt}/{max_retries}...")
                
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                print(f"✅ 成功！")
                self._save_raw_data(data)
                return data
                
            except requests.exceptions.Timeout:
                print(f"⏱️  网络超时")
                if attempt < max_retries:
                    wait_time = 2 ** attempt  # 指数退避：2秒, 4秒, 8秒
                    print(f"   等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    print("❌ 重试次数用尽，放弃")
                    return []
            
            except requests.exceptions.HTTPError as e:
                print(f"❌ API错误: {e}")
                if e.response.status_code == 401:
                    print("   ⚠️  API密钥无效，请检查.env文件")
                elif e.response.status_code == 429:
                    print("   ⚠️  请求频率过高，请稍后再试")
                # 不重试，直接返回
                return []
            
            except Exception as e:
                print(f"❌ 未知错误: {type(e).__name__} - {e}")
                return []
    
    def _save_raw_data(self, data):
        """
        把原始JSON数据保存到data/raw/文件夹
        文件名格式：disruptions_20250214_153045.json
        """
        # 生成时间戳（年月日_时分秒）
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 构建文件路径
        filename = f"disruptions_{timestamp}.json"
        filepath = Path("data/raw") / filename
        
        # 确保文件夹存在
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # 写入文件（indent=2让JSON更易读）
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 原始数据已保存到: {filepath}")


# ===== 测试代码 =====
if __name__ == "__main__":
    """
    这段代码只有在直接运行这个文件时才会执行
    用来测试我们的代码是否正常工作
    """
    print("=== NS API 客户端测试 ===\n")
    
    # 创建客户端对象
    client = NSAPIClient()
    
    # 下载数据
    disruptions = client.fetch_disruptions()
    
    # 显示前3条数据（如果有的话）
    if disruptions:
        print("\n📋 前3条延误信息预览：")
        for i, item in enumerate(disruptions[:3], 1):
            print(f"\n{i}. {item.get('title', '无标题')}")
            print(f"   类型: {item.get('type', '未知')}")
            print(f"   开始时间: {item.get('start', '未知')}")