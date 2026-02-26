from __future__ import annotations

import httpx

class ThonaApiClient:
    def __init__(self, base_url: str, timeout_seconds: int = 60, page_size: int = 5000):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_seconds
        self.page_size = page_size

    def fetch_view(self, vista: str, fecha_desde: str, fecha_hasta: str, page: int = 1) -> dict:
        """
        Contrato sugerido:
        GET {base_url}/{vista}?fechaDesde=YYYY-MM-DD&fechaHasta=YYYY-MM-DD&page=1&pageSize=5000
        """
        url = f"{self.base_url}/{vista}"
        params = {
            "fechaDesde": fecha_desde,
            "fechaHasta": fecha_hasta,
            "page": page,
            "pageSize": self.page_size,
        }
        with httpx.Client(timeout=self.timeout) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            return r.json()