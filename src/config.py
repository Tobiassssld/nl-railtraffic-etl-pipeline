# src/config.py

import logging
import os
from pathlib import Path
from datetime import datetime

def setup_logging():
    """
    Configure logging.
    Lambda's filesystem is read-only except for /tmp,
    so we write logs there when running in Lambda.
    """
    # AWS_LAMBDA_FUNCTION_NAME is auto-injected by the Lambda runtime.
    # Locally and on GitHub Actions it doesn't exist.
    if os.getenv('AWS_LAMBDA_FUNCTION_NAME'):
        log_dir = Path('/tmp/logs')
    else:
        log_dir = Path('logs')

    log_dir.mkdir(exist_ok=True)

    log_file = log_dir / f"pipeline_{datetime.now().strftime('%Y%m%d')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)