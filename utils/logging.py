# logging_config.py
import logging
import os
from pathlib import Path
from datetime import datetime
from uuid import uuid4
def setup_logging(
    user_id:str,
    log_level: str = "INFO", 
    base_path: str = None
):
    """전역 로깅 설정 (한 번만 호출)"""
    if logging.getLogger().handlers:
        return  # 이미 설정됨
    
    # 포맷터
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 파일 핸들러 (optional)
    if base_path:
        log_dir = Path(base_path) / "logs"
        log_dir.mkdir(mode=0o777, parents=True, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        uid = str(uuid4())[:8]
        log_file = log_dir / f"{date_str}_{user_id}_{uid}.log"
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        
        logging.getLogger().addHandler(file_handler)
    
    # 루트 로거 설정
    logging.getLogger().addHandler(console_handler)
    logging.getLogger().setLevel(getattr(logging, log_level.upper()))