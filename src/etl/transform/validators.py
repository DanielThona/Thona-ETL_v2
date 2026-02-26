from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from datetime import datetime
import pandas as pd

@dataclass
class ValidationResult:
    df_ok: pd.DataFrame
    df_err: pd.DataFrame

def validate_strings(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string").fillna("").str.strip()
    return df

def validate_dates(df: pd.DataFrame, cols: list[str], fmt: str | None = None) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", format=fmt)
    return df

def validate_decimals(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    def to_decimal(x):
        if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and x.strip() == ""):
            return None
        try:
            return Decimal(str(x))
        except (InvalidOperation, ValueError):
            return None

    for c in cols:
        if c in df.columns:
            df[c] = df[c].map(to_decimal)
    return df

def split_ok_err(df: pd.DataFrame, required_cols: list[str]) -> ValidationResult:
    """
    MVP: si cualquier required_col viene nula -> error.
    Más adelante: reglas por columna + mensajes por fila.
    """
    missing_mask = False
    for c in required_cols:
        if c in df.columns:
            missing_mask = missing_mask | df[c].isna()
    df_err = df[missing_mask].copy() if isinstance(missing_mask, pd.Series) else df.iloc[0:0].copy()
    df_ok = df[~missing_mask].copy() if isinstance(missing_mask, pd.Series) else df.copy()
    return ValidationResult(df_ok=df_ok, df_err=df_err)