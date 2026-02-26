from __future__ import annotations

from etl.clients.redshift_client import RedshiftClient
from etl.logging.models import EtlLog

class EtlLogRepo:
    def __init__(self, rs: RedshiftClient, schema: str, table: str):
        self.rs = rs
        self.full_table = f"{schema}.{table}"

    def insert_running(self, log: EtlLog) -> None:
        self.rs.execute(
            f"""
            insert into {self.full_table}(
              run_id, flow_name, origen, modo, tabla_destino,
              fecha_inicio, registros_extraidos, registros_ok, registros_fallidos,
              status, error_resumen, archivo_errores, usuario
            )
            values (
              :run_id, :flow_name, :origen, :modo, :tabla_destino,
              :fecha_inicio, :registros_extraidos, :registros_ok, :registros_fallidos,
              :status, :error_resumen, :archivo_errores, :usuario
            );
            """,
            {
                "run_id": log.run_id,
                "flow_name": log.flow_name,
                "origen": log.origen,
                "modo": log.modo,
                "tabla_destino": log.tabla_destino,
                "fecha_inicio": log.fecha_inicio,
                "registros_extraidos": log.registros_extraidos,
                "registros_ok": log.registros_ok,
                "registros_fallidos": log.registros_fallidos,
                "status": log.status,
                "error_resumen": log.error_resumen,
                "archivo_errores": log.archivo_errores,
                "usuario": log.usuario,
            },
        )

    def finish(self, run_id: str, status: str, fecha_fin, duracion_seg: int,
               registros_extraidos: int, registros_ok: int, registros_fallidos: int,
               error_resumen: str | None = None, archivo_errores: str | None = None) -> None:
        self.rs.execute(
            f"""
            update {self.full_table}
            set
              fecha_fin = :fecha_fin,
              duracion_seg = :duracion_seg,
              registros_extraidos = :registros_extraidos,
              registros_ok = :registros_ok,
              registros_fallidos = :registros_fallidos,
              status = :status,
              error_resumen = :error_resumen,
              archivo_errores = :archivo_errores
            where run_id = :run_id;
            """,
            {
                "run_id": run_id,
                "fecha_fin": fecha_fin,
                "duracion_seg": duracion_seg,
                "registros_extraidos": registros_extraidos,
                "registros_ok": registros_ok,
                "registros_fallidos": registros_fallidos,
                "status": status,
                "error_resumen": error_resumen,
                "archivo_errores": archivo_errores,
            },
        )