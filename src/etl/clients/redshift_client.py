from __future__ import annotations

from sqlalchemy import create_engine, text

class RedshiftClient:
    def __init__(self, host: str, port: int, db: str, user: str, password: str):
        # Redshift habla PostgreSQL; psycopg3 funciona bien con SQLAlchemy 2
        self._engine = create_engine(
            f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}",
            pool_pre_ping=True,
        )

    def execute(self, sql: str, params: dict | None = None) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(sql), params or {})

    def fetch_scalar(self, sql: str, params: dict | None = None):
        with self._engine.begin() as conn:
            return conn.execute(text(sql), params or {}).scalar()

    @property
    def engine(self):
        return self._engine