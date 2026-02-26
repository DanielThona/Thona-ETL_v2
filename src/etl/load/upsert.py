from __future__ import annotations

import pandas as pd
from etl.clients.redshift_client import RedshiftClient
from etl.load.redshift_load import load_dataframe_append

def upsert_delete_insert(
    rs: RedshiftClient,
    df: pd.DataFrame,
    target_table: str,
    key_cols: list[str],
    tmp_table: str,
) -> int:
    """
    Estrategia robusta Redshift MVP:
    1) cargar a tmp
    2) delete target where exists tmp keys
    3) insert into target select * from tmp
    """
    if df.empty:
        return 0

    # crear tmp (simple: si ya existe, trunc)
    rs.execute(f"create table if not exists {tmp_table} (like {target_table});")
    rs.execute(f"truncate table {tmp_table};")

    load_dataframe_append(rs, df, tmp_table)

    # delete por join
    join_cond = " AND ".join([f"t.{c} = s.{c}" for c in key_cols])
    rs.execute(
        f"""
        delete from {target_table} t
        using {tmp_table} s
        where {join_cond};
        """
    )

    rs.execute(f"insert into {target_table} select * from {tmp_table};")
    return len(df)