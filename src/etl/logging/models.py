from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

@dataclass
class EtlLog:
    run_id: str
    flow_name: str
    origen: str
    modo: str
    tabla_destino: str
    fecha_inicio: datetime
    fecha_fin: datetime | None = None
    duracion_seg: int | None = None
    registros_extraidos: int = 0
    registros_ok: int = 0
    registros_fallidos: int = 0
    status: str = "RUNNING"
    error_resumen: str | None = None
    archivo_errores: str | None = None
    usuario: str | None = None