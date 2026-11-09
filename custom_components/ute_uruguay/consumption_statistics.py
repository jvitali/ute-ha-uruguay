"""Importa el consumo por hora del medidor como estadística externa de Home Assistant.

El sensor "Consumo del período" es un acumulado que UTE actualiza una vez por día y que
vuelve a cero al cambiar el período de facturación: en el panel de Energía eso aparece como
un pico el día del cambio (HA suma de golpe los primeros días del período nuevo) y cada
barra muestra lo que UTE informó ese día, no lo consumido.

Acá se usa `miConsumptionCurve` con granularidad `H`: cada hora queda en su hora real, con
3 decimales. La estadística `ute_uruguay:consumo_horario_<servicePoint>` se puede elegir en
Energía → Red eléctrica → Consumo. La primera vez se cargan `BACKFILL_DAYS` hacia atrás;
después, cada `IMPORT_INTERVAL`, sólo las horas posteriores a la última importada.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.util import dt as dt_util

from .const import DOMAIN

if TYPE_CHECKING:
    from .coordinator import UteCoordinator
    from .models import CurvePoint, Service

_LOGGER = logging.getLogger(__name__)

IMPORT_INTERVAL = timedelta(hours=3)
BACKFILL_DAYS = 365
CHUNK_DAYS = 30  # un mes por hora entra en un solo pedido
MAGNITUDE_ACTIVE = "IMPORT_ACTIVE_ENERGY"


def statistic_id(service_point_id: str) -> str:
    """Id de la estadística externa de un suministro."""
    return f"{DOMAIN}:consumo_horario_{re.sub(r'[^a-z0-9_]', '_', service_point_id.lower())}"


def build_hourly_stats(
    points: list[CurvePoint], after: datetime | None, start_sum: float
) -> tuple[list[dict[str, Any]], float, datetime | None]:
    """Convierte puntos `H` en filas de estadística con suma acumulada.

    Ignora la energía reactiva y las horas que no son posteriores a `after` (ya
    importadas). Devuelve las filas, la suma final y el inicio de la última hora.
    """
    hours: list[tuple[datetime, float]] = []
    for p in points:
        if p.magnitude != MAGNITUDE_ACTIVE:
            continue
        day = dt_util.parse_datetime(p.date)
        if day is None:
            continue
        start = day + timedelta(hours=p.period - 1)
        if after is not None and start <= after:
            continue
        hours.append((start, p.value))
    hours.sort()

    rows: list[dict[str, Any]] = []
    total = start_sum
    for start, value in hours:
        total += value
        rows.append({"start": start, "sum": total})
    return rows, total, (hours[-1][0] if hours else after)


def _metadata(stat_id: str, name: str) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "has_sum": True,
        "name": name,
        "source": DOMAIN,
        "statistic_id": stat_id,
        "unit_of_measurement": "kWh",
    }
    try:  # HA 2025.10+: mean_type y unit_class reemplazan a has_mean
        from homeassistant.components.recorder.models import StatisticMeanType

        meta["mean_type"] = StatisticMeanType.NONE
        meta["unit_class"] = "energy"
    except ImportError:
        meta["has_mean"] = False
    return meta


class ConsumptionImporter:
    """Importa periódicamente la curva horaria de cada suministro con medidor inteligente."""

    def __init__(self, hass: HomeAssistant, coordinator: UteCoordinator) -> None:
        self._hass = hass
        self._coordinator = coordinator
        self._lock = asyncio.Lock()
        self._last_run: datetime | None = None

    @callback
    def async_schedule(self, entry: ConfigEntry) -> None:
        """Lanzar una importación si toca. Se llama después de cada refresh."""
        if "recorder" not in self._hass.config.components or self._lock.locked():
            return
        now = dt_util.utcnow()
        if self._last_run is not None and now - self._last_run < IMPORT_INTERVAL:
            return
        self._last_run = now
        entry.async_create_background_task(
            self._hass, self.async_run(), f"{DOMAIN}_consumption_statistics"
        )

    async def async_run(self) -> None:
        """Importar todos los suministros con AMI."""
        async with self._lock:
            data = self._coordinator.data
            client = self._coordinator.client
            if data is None or client is None:
                return
            for services in data.services_by_account.values():
                for sd in services:
                    if not sd.service.ami_present:
                        continue
                    try:
                        await self._async_import_service(client, sd.service)
                    except Exception as e:  # noqa: BLE001 — se reintenta en la próxima corrida
                        _LOGGER.warning(
                            "No se pudo importar la curva horaria de sp=%s: %s",
                            sd.service.service_point_id,
                            e,
                        )

    async def _async_import_service(self, client: Any, svc: Service) -> None:
        from homeassistant.components.recorder import get_instance
        from homeassistant.components.recorder.statistics import (
            async_add_external_statistics,
            get_last_statistics,
        )

        stat_id = statistic_id(svc.service_point_id)
        name = f"UTE consumo por hora ({svc.short_address or svc.service_point_id})"
        last = await get_instance(self._hass).async_add_executor_job(
            get_last_statistics, self._hass, 1, stat_id, False, {"sum"}
        )
        today = dt_util.now().date()
        if rows := last.get(stat_id):
            after: datetime | None = dt_util.utc_from_timestamp(rows[0]["start"])
            total = float(rows[0].get("sum") or 0.0)
            # Volver a pedir el día de la última hora: puede haber quedado incompleto.
            day: date = dt_util.as_local(after).date()
        else:
            after, total = None, 0.0
            day = today - timedelta(days=BACKFILL_DAYS)

        imported = 0
        while day <= today:
            end = min(day + timedelta(days=CHUNK_DAYS - 1), today)
            points = await client.consumption_curve(
                svc.service_point_id, "H", day.isoformat(), end.isoformat()
            )
            stats, total, after = build_hourly_stats(points, after, total)
            if stats:
                async_add_external_statistics(self._hass, _metadata(stat_id, name), stats)
                imported += len(stats)
            day = end + timedelta(days=1)
            if day <= today:
                await asyncio.sleep(1)  # no apurar a la API de UTE en la carga inicial
        if imported:
            _LOGGER.info("Importadas %s horas de consumo en %s", imported, stat_id)
