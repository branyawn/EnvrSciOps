import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass(frozen=True)
class Settings:
    db_path: str = os.getenv("ENVRSCIOPS_DB_PATH", "./data/envrsciops.sqlite3")
    usgs_site: str = os.getenv("USGS_SITE", "02177000")
    usgs_parameter: str = os.getenv("USGS_PARAMETER", "00060")

def get_settings() -> Settings:
    return Settings()
