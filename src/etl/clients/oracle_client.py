from __future__ import annotations

import oracledb

class OracleClient:
    def __init__(self, dsn: str, user: str, password: str):
        self.dsn = dsn
        self.user = user
        self.password = password

    def fetchall(self, sql: str, binds: dict) -> list[tuple]:
        """
        Regla: usar binds :dFecDesde y :dFecHasta con BETWEEN directo (sin TRUNC).
        """
        with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, binds)
                return cur.fetchall()

    def fetch_df(self, sql: str, binds: dict):
        import pandas as pd
        with oracledb.connect(user=self.user, password=self.password, dsn=self.dsn) as conn:
            return pd.read_sql(sql, conn, params=binds)