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
        self.values: dict[date, float] = {}
        self.missing: set[date] = set()

    async def consumption_curve(self, sp, granularity, date_from, date_to):
        assert granularity == "H"
        self.calls.append((date_from, date_to))
        day, end = date.fromisoformat(date_from), date.fromisoformat(date_to)
        pts = []
        while day <= min(end, self.available_until):
            if day not in self.missing:
                pts.extend(_day_points(day, self.values.get(day, 0.5)))
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
        # La segunda corrida no repite el backfill: arranca cerca de la ventana de refresco.
        assert date.fromisoformat(client.calls[-1][0]) >= today - timedelta(days=6)

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


def _assert_monotonic(rows: list[dict]) -> None:
    sums = [r["sum"] for r in rows]
    assert all(b >= a for a, b in zip(sums, sums[1:])), "la suma acumulada bajó"


async def test_late_correction_rewritten(recorder_mock: Recorder, hass: HomeAssistant) -> None:
    """Si UTE corrige un día reciente, la siguiente corrida lo reescribe y ajusta las sumas."""
    today = dt_util.now().date()
    client = FakeClient(available_until=today - timedelta(days=1))
    importer = ConsumptionImporter(hass, _coordinator(client))
    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 5), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        await importer.async_run()
        await async_wait_recording_done(hass)
        client.values[today - timedelta(days=1)] = 1.0  # UTE corrige ayer: 1.0 por hora
        await importer.async_run()
        await async_wait_recording_done(hass)
    rows = await _stats(hass)
    assert len(rows) == 5 * 24
    assert rows[-1]["sum"] == 4 * 24 * 0.5 + 24 * 1.0
    _assert_monotonic(rows)


async def test_incomplete_response_does_not_corrupt(
    recorder_mock: Recorder, hass: HomeAssistant
) -> None:
    """Si UTE no devuelve una hora que ya estaba, no se reescribe la ventana (sumas intactas)."""
    today = dt_util.now().date()
    client = FakeClient(available_until=today - timedelta(days=2))
    importer = ConsumptionImporter(hass, _coordinator(client))
    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 5), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        await importer.async_run()
        await async_wait_recording_done(hass)
        client.missing = {today - timedelta(days=3)}  # hueco en la respuesta
        client.available_until = today - timedelta(days=1)
        await importer.async_run()
        await async_wait_recording_done(hass)
    rows = await _stats(hass)
    assert len(rows) == 5 * 24
    assert rows[-1]["sum"] == 5 * 24 * 0.5
    _assert_monotonic(rows)


def test_metadata_follows_ha_version() -> None:
    """Sólo se mandan los campos que la versión de HA conoce."""
    from homeassistant.components.recorder.models import StatisticMetaData

    from custom_components.ute_uruguay.consumption_statistics import _metadata

    fields = set(StatisticMetaData.__annotations__)
    meta = _metadata("ute_uruguay:x", "X")
    # Con los campos de la versión instalada: nunca uno que no exista.
    assert set(meta) <= fields | {"has_mean"}
    assert ("mean_type" in meta) == ("mean_type" in fields)
    assert ("unit_class" in meta) == ("unit_class" in fields)
    assert ("has_mean" in meta) == ("mean_type" not in fields)

    # Simular versiones anteriores quitando campos (sólo tiene sentido si existen).
    original = StatisticMetaData.__annotations__
    if "mean_type" not in original:
        return
    try:
        StatisticMetaData.__annotations__ = {k: v for k, v in original.items() if k != "unit_class"}
        meta = _metadata("ute_uruguay:x", "X")
        assert "unit_class" not in meta and "mean_type" in meta
        StatisticMetaData.__annotations__ = {
            k: v for k, v in original.items() if k not in ("unit_class", "mean_type")
        }
        meta = _metadata("ute_uruguay:x", "X")
        assert meta["has_mean"] is False and "mean_type" not in meta
    finally:
        StatisticMetaData.__annotations__ = original


def test_duplicate_hours_counted_once() -> None:
    """Una hora repetida en la respuesta (o en pedidos solapados) se cuenta una sola vez."""
    day = _day_points(date(2026, 11, 4), 0.5)
    repeated = day + day[:10]  # UTE devuelve de nuevo las primeras horas
    rows, total, _ = build_hourly_stats(repeated, None, 0.0)
    assert len(rows) == 24
    assert total == 12.0
    assert len({r["start"] for r in rows}) == 24


async def test_rerunning_is_idempotent(recorder_mock: Recorder, hass: HomeAssistant) -> None:
    """Correr el importador varias veces sin datos nuevos no cambia nada."""
    today = dt_util.now().date()
    client = FakeClient(available_until=today - timedelta(days=1))
    importer = ConsumptionImporter(hass, _coordinator(client))
    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 5), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        totals = []
        for _ in range(4):
            await importer.async_run()
            await async_wait_recording_done(hass)
            rows = await _stats(hass)
            totals.append((len(rows), rows[-1]["sum"]))
    assert totals == [(5 * 24, 5 * 24 * 0.5)] * 4


async def test_overlapping_chunks_counted_once(
    recorder_mock: Recorder, hass: HomeAssistant
) -> None:
    """Si UTE devuelve días de más (fuera del rango pedido), no se duplican."""
    today = dt_util.now().date()

    class OverlappingClient(FakeClient):
        async def consumption_curve(self, sp, granularity, date_from, date_to):
            # Devuelve también el día anterior al pedido, que ya vino en el pedido previo.
            prev = (date.fromisoformat(date_from) - timedelta(days=1)).isoformat()
            return await super().consumption_curve(sp, granularity, prev, date_to)

    client = OverlappingClient(available_until=today - timedelta(days=1))
    importer = ConsumptionImporter(hass, _coordinator(client))
    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 40), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        await importer.async_run()
        await async_wait_recording_done(hass)
    from homeassistant.components.recorder import get_instance

    last = await get_instance(hass).async_add_executor_job(
        get_last_statistics, hass, 1, statistic_id(SP), False, {"sum"}
    )
    # Días distintos recibidos: del día anterior al primer pedido (today-41) a ayer = 41.
    # El día que se repite entre los dos tramos de 30 días no se cuenta dos veces (serían 42).
    assert last[statistic_id(SP)][0]["sum"] == 41 * 24 * 0.5
    rows = await _stats(hass)
    assert len({r["start"] for r in rows}) == len(rows)


def test_empty_and_future_hours_skipped() -> None:
    """Horas sin medir (value None) o todavía en curso no se importan."""
    pts = _day_points(date(2026, 11, 4), 0.5)
    pts[0].value = None  # 00:00 sin medir
    now = dt_util.now()
    today = now.date()
    future = _day_points(today, 0.5)  # incluye horas que todavía no terminaron
    rows, total, _ = build_hourly_stats(pts + future, None, 0.0)
    starts = {r["start"] for r in rows}
    assert datetime.fromisoformat("2026-11-04T00:00:00-03:00") not in starts
    assert all(s + timedelta(hours=1) <= dt_util.utcnow() for s in starts)


async def test_backfill_tolerates_rejected_old_chunks(
    recorder_mock: Recorder, hass: HomeAssistant
) -> None:
    """Si UTE rechaza los tramos más viejos (medidor nuevo), la carga inicial sigue igual."""
    from custom_components.ute_uruguay.api import UteApiError

    today = dt_util.now().date()
    meter_since = today - timedelta(days=20)

    class NewMeterClient(FakeClient):
        async def consumption_curve(self, sp, granularity, date_from, date_to):
            if date.fromisoformat(date_from) < meter_since:
                raise UteApiError("GET ... → 400: fecha anterior al medidor")
            return await super().consumption_curve(sp, granularity, date_from, date_to)

    client = NewMeterClient(available_until=today - timedelta(days=1))
    importer = ConsumptionImporter(hass, _coordinator(client))
    with patch("custom_components.ute_uruguay.consumption_statistics.BACKFILL_DAYS", 70), patch(
        "custom_components.ute_uruguay.consumption_statistics.asyncio.sleep"
    ):
        await importer.async_run()
        await async_wait_recording_done(hass)
    rows = await _stats(hass)
    assert rows, "la carga inicial no debe trabarse por los tramos viejos"
    assert rows[-1]["sum"] > 0
