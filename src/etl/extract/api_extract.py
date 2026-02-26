from __future__ import annotations

import pandas as pd
from etl.clients.thona_api_client import ThonaApiClient

def extract_api_view(client: ThonaApiClient, vista: str, fecha_desde: str, fecha_hasta: str) -> pd.DataFrame:
    # MVP: soporta una página. Luego lo extendemos a paginación/reintentos.
    data = client.fetch_view(vista=vista, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta, page=1)

    # Supuesto común: { items: [...], total: n } o directamente [...]
    items = data.get("items", data if isinstance(data, list) else [])
    return pd.DataFrame(items)