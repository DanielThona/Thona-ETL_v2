from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os

from dotenv import load_dotenv

from etl.config.loader import load_yaml

@dataclass(frozen=True)
class Settings:
    app_env: str

    oracle_dsn: str
    oracle_user: str
    oracle_password: str

    redshift_host: str
    redshift_port: int
    redshift_db: str
    redshift_user: str
    redshift_password: str

    thona_api_base_url: str
    thona_api_timeout_seconds: int
    thona_api_page_size: int

    redshift_log_schema: str
    redshift_log_table: str

    app_cfg: dict

def get_settings(config_path: str = "configs/app.yaml") -> Settings:
    load_dotenv()

    app_cfg = load_yaml(Path(config_path))

    def req(name: str) -> str:
        v = os.getenv(name)
        if not v:
            raise ValueError(f"Missing required env var: {name}")
        return v

    return Settings(
        app_env=os.getenv("APP_ENV", "dev"),

        oracle_dsn=req("ORACLE_DSN"),
        oracle_user=req("ORACLE_USER"),
        oracle_password=req("ORACLE_PASSWORD"),

        redshift_host=req("REDSHIFT_HOST"),
        redshift_port=int(os.getenv("REDSHIFT_PORT", "5439")),
        redshift_db=req("REDSHIFT_DB"),
        redshift_user=req("REDSHIFT_USER"),
        redshift_password=req("REDSHIFT_PASSWORD"),

        thona_api_base_url=req("THONA_API_BASE_URL").rstrip("/"),
        thona_api_timeout_seconds=int(os.getenv("THONA_API_TIMEOUT_SECONDS", "60")),
        thona_api_page_size=int(os.getenv("THONA_API_PAGE_SIZE", "5000")),

        redshift_log_schema=os.getenv("REDSHIFT_LOG_SCHEMA", "logs"),
        redshift_log_table=os.getenv("REDSHIFT_LOG_TABLE", "etl_log_cargas"),

        app_cfg=app_cfg,
    )