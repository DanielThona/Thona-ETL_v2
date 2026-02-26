import pandas as pd
from etl.transform.validators import validate_dates, split_ok_err

def test_validate_dates_and_split():
    df = pd.DataFrame([{"d": "2025-01-01"}, {"d": "bad"}])
    df = validate_dates(df, ["d"])
    res = split_ok_err(df, required_cols=["d"])
    assert len(res.df_ok) == 1
    assert len(res.df_err) == 1