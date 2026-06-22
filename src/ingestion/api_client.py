# src/ingestion/api_client.py

import requests
import json
import time
from datetime import datetime
from pathlib import Path
import os
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError

load_dotenv()


class NSAPIClient:
    """
    Fetches disruption data from the NS (Dutch Railways) API.
    Raw JSON is archived locally and to AWS S3.
    """

    def __init__(self):
        # --- NS API ---
        self.api_key = os.getenv('NS_API_KEY')
        if not self.api_key:
            raise ValueError("NS_API_KEY not found in environment variables.")

        self.base_url = "https://gateway.apiportal.ns.nl/reisinformatie-api/api/v3"
        self.headers = {'Ocp-Apim-Subscription-Key': self.api_key}

        # --- AWS S3 ---
        # boto3 automatically reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
        # and AWS_DEFAULT_REGION from environment variables (loaded via dotenv).
        # No explicit credentials needed here — same pattern as Azure's
        # DefaultAzureCredential, just env-var driven.
        self.s3_bucket = os.getenv('AWS_S3_BUCKET', 'nl-rail-raw-disruptions-tl')
        self.s3_client = None

        try:
            self.s3_client = boto3.client('s3')
            # Lightweight check: verify the bucket is accessible
            self.s3_client.head_bucket(Bucket=self.s3_bucket)
            print(f"  S3 connected: s3://{self.s3_bucket}")
        except ClientError as e:
            # head_bucket raises ClientError if bucket not found or no access
            print(f"  S3 unavailable ({e}), will save locally only.")
            self.s3_client = None
        except Exception as e:
            print(f"  S3 init failed ({e}), will save locally only.")
            self.s3_client = None

    def fetch_disruptions(self, max_retries=3):
        """
        Download disruption data with retry / exponential backoff.
        """
        url = f"{self.base_url}/disruptions"

        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempt {attempt}/{max_retries}...")
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                print(" Fetch successful.")
                self._save_raw_data(data)
                return data

            except requests.exceptions.Timeout:
                print("  Request timed out.")
                if attempt < max_retries:
                    wait_time = 2 ** attempt      # 2s, 4s, 8s
                    print(f"   Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(" Max retries reached.")
                    return []

            except requests.exceptions.HTTPError as e:
                print(f" HTTP error: {e}")
                if e.response.status_code == 401:
                    print("     Invalid API key — check NS_API_KEY in .env")
                elif e.response.status_code == 429:
                    print("     Rate limited — try again later.")
                return []

            except Exception as e:
                print(f" Unexpected error: {type(e).__name__} — {e}")
                return []

    def _save_raw_data(self, data):
        """
        Persist raw JSON in two places:
          1. Local filesystem  → data/raw/<timestamp>.json
          2. AWS S3            → s3://<bucket>/year/month/day/<timestamp>.json

        The S3 key structure (year/month/day/) mirrors what we had on
        Azure Blob Storage, so the hierarchical layout stays identical.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"disruptions_{timestamp}.json"
        json_content = json.dumps(data, indent=2, ensure_ascii=False)

        # 1. Local save (unchanged)
        filepath = Path("data/raw") / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(json_content)
        print(f" Local:  {filepath}")

        # 2. S3 upload
        # Key format:  2026/06/22/disruptions_20260622_060000.json
        # This is the S3 equivalent of Azure Blob's hierarchical path.
        # put_object() is the simplest upload method for strings/bytes.
        # For large files (>100 MB) you'd use upload_file() with multipart,
        # but JSON payloads here are well under 1 MB.
        if self.s3_client:
            s3_key = f"{datetime.now().strftime('%Y/%m/%d')}/{filename}"
            try:
                self.s3_client.put_object(
                    Bucket=self.s3_bucket,
                    Key=s3_key,
                    Body=json_content.encode('utf-8'),   # S3 expects bytes
                    ContentType='application/json'
                )
                print(f"  S3:     s3://{self.s3_bucket}/{s3_key}")
            except ClientError as e:
                # Don't crash the pipeline if cloud upload fails —
                # same defensive pattern as the Azure version.
                print(f"  S3 upload failed (continuing): {e}")


# ===== Quick smoke test =====
if __name__ == "__main__":
    print("=== NSAPIClient smoke test ===\n")
    client = NSAPIClient()
    disruptions = client.fetch_disruptions()
    if disruptions:
        print(f"\n First 3 disruptions:")
        for i, item in enumerate(disruptions[:3], 1):
            print(f"  {i}. [{item.get('type', '?')}] {item.get('title', 'No title')}")