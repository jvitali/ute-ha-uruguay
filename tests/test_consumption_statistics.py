"""Tests de la importación de la curva horaria como estadística externa."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from homeassistant.components.recorder import Recorder
from homeassistant.components.recorder.statistics import get_last_statistics, statistics_during_period
from homeassistant.core import HomeAssistant
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.components.recorder.common import (
    async_wait_recording_done,
)

from custom_components.ute_uruguay.consumption_statistics import (
    ConsumptionImporter,
    build_hourly_stats,
    statistic_id,
)
from custom_components.ute_uruguay.models import CurvePoint

SP = "1234567890"


def _day_points(day: date, value: float = 0.5) -> list[CurvePoint]:
    """24 horas de activa (+ reactiva, que se debe ignorar) como las devuelve UTE."""
    pts = []
    for period in range(1, 25):
        for magnitude in ("IMPORT_ACTIVE_ENERGY", "Q1_REACTIVE_ENERGY"):
            pts.append(
                CurvePoint(
                    date=f"{day.isoformat()}T00:00:00-03:00",
                    period=period,
                    value=value if magnitude == "IMPORT_ACTIVE_ENERGY" else 9.9,
                    label=f"{period - 1:02d}:00",
                    magnitude=magnitude,
                )
            )
    return pts


def test_build_hourly_stats() -> None:
    """Horas en su hora real, sólo activa, suma acumulada y sin repetir lo importado."""
    pts = _day_points(date(2026, 11, 4), 0.25)
    rows, total, last = build_hourly_stats(pts, None, 10.0)
    assert len(rows) == 24
    assert rows[0]["start"] == datetime.fromisoformat("2026-11-04T00:00:00-03:00")
    assert rows[23]["start"] == datetime.fromisoformat("2026-11-04T23:00:00-03:00")
    assert rows[0]["sum"] == 10.25
    assert total == 16.0
    assert last == rows[23]["start"]

    # Pedir de nuevo el mismo día: sólo entra lo posterior a la última hora importada.
    rows2, total2, last2 = build_hourly_stats(pts, rows[11]["start"], 13.0)
    assert len(rows2) == 12
    assert rows2[0]["start"] == rows[12]["start"]
    assert total2 == 16.0
    assert last2 == last


def test_statistic_id() -> None:
    assert statistic_id("ABC-12") == "ute_uruguay:consumo_horario_abc_12"


class FakeClient:
    """Devuelve 24 horas por cada día pedido, hasta `available_until` (atraso de UTE)."""

    def __init__(self, available_until: date) -> None:
        self.available_until = available_until
        self.calls: list[tuple[str, str]] = []

    async def consumption_curve(self, sp, granularity, date_from, date_to):
        assert granularity == "H"
        self.calls.append((date_from, date_to))
        day, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        pts = []
        while day <= min(end, self.available_until):
            pts.extend(_day_points(day))
            day += timedelta(days=1)
        return pts


def _coordinator(client) -> SimpleNamespace:
    svc = SimpleNamespace(service_point_id=SP, short_address="Casa", ami_present=True)
    no_ami = SimpleNamespace(service_point_id="999", short_address="Otro", ami_present=False)
    data = SimpleNamespace(
        services_by_account={"acc": [SimpleNamespace(service=svc), SimpleNamespace(service=no_ami)]}
    )
    return SimpleNamespace(data=data, client=client)


async def _stats(hass: HomeAssistant) -> list[dict]:
    from homeassistant.components.recorder import get_instance

    start = dt_util.utcnow() - timedelta(days=30)
    result = await get_instance(hass).async_add_executor_job(
        statistics_during_period, hass, start, None, {statistic_id(SP)}, "hour", None, {"sum"}
    )
    return result.get(statistic_id(SP), [])


async def test_import_backfill_then_incremental(
    recorder_mock: Recorder, hass: HomeAssistant
) -> None:
    """Primera corrida: historial; siguientes: sólo horas nuevas, sin cortar la suma."""
    today = dt_util.now().date()
    client = FakeClient(available_until=today - timedelta(days=2))
    importer = ConsumptionImporter(hass, _coordinator(client))

    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 5), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        await importer.async_run()
        await async_wait_recording_done(hass)
        rows = await _stats(hass)
        # Días today-5 .. today-2 → 4 días de 24 horas, a 0.5 kWh.
        assert len(rows) == 4 * 24
        assert rows[-1]["sum"] == 4 * 24 * 0.5

        # Al día siguiente UTE publica un día más.
        client.available_until = today - timedelta(days=1)
        await importer.async_run()
        await async_wait_recording_done(hass)
        rows = await _stats(hass)
        assert len(rows) == 5 * 24
        assert rows[-1]["sum"] == 5 * 24 * 0.5
        # La segunda corrida empezó por el día de la última hora, no por el backfill.
        assert client.calls[-1][0] == (today - timedelta(days=2)).isoformat()

    from homeassistant.components.recorder import get_instance

    last = await get_instance(hass).async_add_executor_job(
        get_last_statistics, hass, 1, statistic_id(SP), False, {"sum"}
    )
    assert last[statistic_id(SP)][0]["sum"] == 60.0


async def test_schedule_throttled(recorder_mock: Recorder, hass: HomeAssistant) -> None:
    """No se lanza más de una importación por IMPORT_INTERVAL."""
    importer = ConsumptionImporter(hass, _coordinator(FakeClient(date(2000, 1, 1))))
    entry = MagicMock()
    importer.async_schedule(entry)
    importer.async_schedule(entry)
    assert entry.async_create_background_task.call_count == 1
    entry.async_create_background_task.call_args.args[1].close()  # coroutine no awaited
