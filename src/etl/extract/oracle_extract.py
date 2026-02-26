from __future__ import annotations

import pandas as pd
from etl.clients.oracle_client import OracleClient

def extract_oracle_df(client: OracleClient, sql: str, dFecDesde: str, dFecHasta: str) -> pd.DataFrame:
    binds = {"dFecDesde": dFecDesde, "dFecHasta": dFecHasta}
    return client.fetch_df(sql, binds)