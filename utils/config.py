# utils/config.py
from dataclasses import dataclass
from pathlib import Path
import json

@dataclass  
class Config:
    base_path: str = "/mnt/AI_NAS/datalake"
    nas_url: str = "http://192.168.20.62:8091" 
    log_level: str = "INFO"
    num_proc: int = 8

    @classmethod
    def load(cls):
        config_file = Path("config.json")
        if config_file.exists():
            with open(config_file) as f:
                data = json.load(f)
                return cls(**data)
    
        return cls()
