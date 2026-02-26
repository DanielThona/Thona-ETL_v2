from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from etl.clients.redshift_client import RedshiftClient

def load_dataframe_append(rs: RedshiftClient, df: pd.DataFrame, full_table_name: str) -> int:
    """
    MVP: append directo con to_sql (sirve para arranque).
    En producción: COPY desde S3 o bulk insert con staging.
    """
    if df.empty:
        return 0
    schema, table = full_table_name.split(".", 1)
    df.to_sql(table, rs.engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=5000)
    return len(df)