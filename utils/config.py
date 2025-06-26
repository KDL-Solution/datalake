# utils/config.py
from dataclasses import dataclass
from pathlib import Path
import yaml

@dataclass  
class Config:
    user_id: str = "user"
    base_path: str = "/mnt/AI_NAS/datalake"
    server_url: str = "http://192.168.20.62:8091" 
    log_level: str = "INFO"
    num_proc: int = 8

    @classmethod
    def load(cls):
        config_file = Path("config.yaml")
        if config_file.exists():
            with open(config_file, encoding='utf-8') as f:
                data = yaml.safe_load(f)
                return cls(**data)
        
        # 없으면 기본값
        return cls()

config = Config.load()