from __future__ import annotations

from datetime import datetime, timezone
import pandas as pd

from prefect import flow, task, get_run_logger

from etl.config.settings import get_settings
from etl.clients.redshift_client import RedshiftClient
from etl.clients.thona_api_client import ThonaApiClient
from etl.logging.etl_log_repo import EtlLogRepo
from etl.logging.models import EtlLog
from etl.utils.ids import new_run_id

from etl.transform.validators import validate_strings, validate_dates, validate_decimals, split_ok_err
from etl.load.redshift_load import load_dataframe_append

@task
def build_dummy_dataframe() -> pd.DataFrame:
    return pd.DataFrame([
        {"recibo_numero": "123", "fecha_emision": "2025-01-10", "monto": "100.50"},
        {"recibo_numero": None, "fecha_emision": "bad-date", "monto": "xx"},
    ])

@flow(name="recibos_flow")
def recibos_flow(
    fecha_desde: str,
    fecha_hasta: str,
    vista: str = "recibos",
    tabla_destino: str = "stage.etl_recibos_dummy",
    modo: str = "incremental",
    origen: str = "API",
    usuario: str | None = None,
):
    logger = get_run_logger()
    settings = get_settings()

    rs = RedshiftClient(
        host=settings.redshift_host,
        port=settings.redshift_port,
        db=settings.redshift_db,
        user=settings.redshift_user,
        password=settings.redshift_password,
    )

    log_repo = EtlLogRepo(rs, settings.redshift_log_schema, settings.redshift_log_table)

    run_id = new_run_id()
    start = datetime.now(timezone.utc)

    log_repo.insert_running(EtlLog(
        run_id=run_id,
        flow_name="recibos_flow",
        origen=origen,
        modo=modo,
        tabla_destino=tabla_destino,
        fecha_inicio=start,
        usuario=usuario,
    ))

    registros_extraidos = registros_ok = registros_fallidos = 0
    error_resumen = None
    archivo_errores = None

    try:
        # MVP: dummy df (cambiaremos a extract_api_view / extract_oracle_df en el siguiente paso)
        df = build_dummy_dataframe()
        registros_extraidos = len(df)

        df = validate_strings(df, ["recibo_numero"])
        df = validate_dates(df, ["fecha_emision"])
        df = validate_decimals(df, ["monto"])

        result = split_ok_err(df, required_cols=["recibo_numero", "fecha_emision", "monto"])
        registros_ok = len(result.df_ok)
        registros_fallidos = len(result.df_err)

        # MVP: carga append (si tu tabla dummy no existe, créala o apunta a una que ya tengas)
        if registros_ok > 0:
            loaded = load_dataframe_append(rs, result.df_ok, tabla_destino)
            logger.info(f"Cargados: {loaded} registros en {tabla_destino}")

        # Si hay errores, aquí luego guardaremos CSV/XLSX y mandaremos correo (siguiente iteración)
        if registros_fallidos > 0:
            error_resumen = "Existen registros inválidos (MVP)."
            # archivo_errores = "path/al/archivo.csv"

        status = "SUCCESS"

    except Exception as ex:
        status = "FAILED"
        error_resumen = str(ex)
        raise
    finally:
        end = datetime.now(timezone.utc)
        duracion = int((end - start).total_seconds())
        log_repo.finish(
            run_id=run_id,
            status=status,
            fecha_fin=end,
            duracion_seg=duracion,
            registros_extraidos=registros_extraidos,
            registros_ok=registros_ok,
            registros_fallidos=registros_fallidos,
            error_resumen=error_resumen,
            archivo_errores=archivo_errores,
        )

if __name__ == "__main__":
    recibos_flow(fecha_desde="2025-01-01", fecha_hasta="2025-01-31")