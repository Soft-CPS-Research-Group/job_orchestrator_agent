from __future__ import annotations

import json
import os
import shutil
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from fastapi import HTTPException

from app.config import settings
from app.utils import mongo_utils

_SYSTEM_DATABASES = {"admin", "local", "config"}

BUILDING_COLUMNS = [
    "month",
    "hour",
    "day_type",
    "daylight_savings_status",
    "indoor_dry_bulb_temperature",
    "average_unmet_cooling_setpoint_difference",
    "indoor_relative_humidity",
    "non_shiftable_load",
    "dhw_demand",
    "cooling_demand",
    "heating_demand",
    "solar_generation",
]

PRICING_COLUMNS = [
    "electricity_pricing",
    "electricity_pricing_predicted_1",
    "electricity_pricing_predicted_2",
    "electricity_pricing_predicted_3",
]

WEATHER_COLUMNS = [
    "outdoor_dry_bulb_temperature",
    "outdoor_relative_humidity",
    "diffuse_solar_irradiance",
    "direct_solar_irradiance",
    "outdoor_dry_bulb_temperature_predicted_1",
    "outdoor_dry_bulb_temperature_predicted_2",
    "outdoor_dry_bulb_temperature_predicted_3",
    "outdoor_relative_humidity_predicted_1",
    "outdoor_relative_humidity_predicted_2",
    "outdoor_relative_humidity_predicted_3",
    "diffuse_solar_irradiance_predicted_1",
    "diffuse_solar_irradiance_predicted_2",
    "diffuse_solar_irradiance_predicted_3",
    "direct_solar_irradiance_predicted_1",
    "direct_solar_irradiance_predicted_2",
    "direct_solar_irradiance_predicted_3",
]

CARBON_COLUMNS = ["carbon_intensity"]

CHARGER_COLUMNS = [
    "electric_vehicle_charger_state",
    "electric_vehicle_id",
    "electric_vehicle_departure_time",
    "electric_vehicle_required_soc_departure",
    "electric_vehicle_estimated_arrival_time",
    "electric_vehicle_estimated_soc_arrival",
]

WASHING_MACHINE_COLUMNS = [
    "day_type",
    "hour",
    "wm_start_time_step",
    "wm_end_time_step",
    "load_profile",
]

DEFAULTS_TEMPLATE = {
    "building": {
        "indoor_dry_bulb_temperature": 20.0,
        "average_unmet_cooling_setpoint_difference": 0.0,
        "indoor_relative_humidity": 50.0,
        "non_shiftable_load": 0.0,
        "dhw_demand": 0.0,
        "cooling_demand": 0.0,
        "heating_demand": 0.0,
        "solar_generation": 0.0,
    },
    "pricing": {
        "electricity_pricing": 0.0,
        "electricity_pricing_predicted_1": 0.0,
        "electricity_pricing_predicted_2": 0.0,
        "electricity_pricing_predicted_3": 0.0,
    },
    "weather": {
        "outdoor_dry_bulb_temperature": 20.0,
        "outdoor_relative_humidity": 50.0,
        "diffuse_solar_irradiance": 0.0,
        "direct_solar_irradiance": 0.0,
        "outdoor_dry_bulb_temperature_predicted_1": 20.0,
        "outdoor_dry_bulb_temperature_predicted_2": 20.0,
        "outdoor_dry_bulb_temperature_predicted_3": 20.0,
        "outdoor_relative_humidity_predicted_1": 50.0,
        "outdoor_relative_humidity_predicted_2": 50.0,
        "outdoor_relative_humidity_predicted_3": 50.0,
        "diffuse_solar_irradiance_predicted_1": 0.0,
        "diffuse_solar_irradiance_predicted_2": 0.0,
        "diffuse_solar_irradiance_predicted_3": 0.0,
        "direct_solar_irradiance_predicted_1": 0.0,
        "direct_solar_irradiance_predicted_2": 0.0,
        "direct_solar_irradiance_predicted_3": 0.0,
    },
    "carbon_intensity": {
        "carbon_intensity": 0.0,
    },
    "charger": {
        "electric_vehicle_charger_state": 1,
        "electric_vehicle_departure_time": None,
        "electric_vehicle_required_soc_departure": None,
        "electric_vehicle_estimated_arrival_time": None,
        "electric_vehicle_estimated_soc_arrival": None,
    },
    "electric_vehicle": {
        "battery_capacity": 50.0,
        "nominal_power": 50.0,
        "initial_soc": 0.5,
        "depth_of_discharge": 0.9,
    },
    "charger_attributes": {
        "nominal_power": 7.4,
        "efficiency": 0.95,
        "charger_type": 1,
        "max_charging_power": 7.4,
        "min_charging_power": 0.0,
        "max_discharging_power": 7.4,
        "min_discharging_power": 0.0,
    },
}

RECOGNIZED_CONFIG_KEYS = {
    "selected_buildings",
    "schema_overrides",
    "building_overrides",
    "defaults",
    "validation",
}


def _parse_timestamp(ts: Any) -> datetime:
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    if isinstance(ts, str):
        normalized = ts.strip()
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        try:
            dt = datetime.fromisoformat(normalized)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    dt = datetime.strptime(normalized, fmt).replace(tzinfo=timezone.utc)
                    break
                except ValueError:
                    continue
            else:
                raise ValueError(f"Invalid timestamp string format: {ts}")

        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


def _deep_merge(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = deepcopy(base)
        for key, value in override.items():
            if key in merged:
                merged[key] = _deep_merge(merged[key], value)
            else:
                merged[key] = deepcopy(value)
        return merged
    return deepcopy(override)


def _slug(value: str, fallback: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in str(value))
    cleaned = cleaned.strip("_-")
    return cleaned or fallback


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        if pd.isna(value):
            return None
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped in {"", "nan", "None", "null"}:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    if isinstance(value, dict):
        if "value" in value:
            return _safe_float(value.get("value"))
        values = value.get("values")
        if isinstance(values, list) and len(values) > 0:
            return _safe_float(values[0])
    if isinstance(value, list) and len(value) > 0:
        return _safe_float(value[0])
    return None


def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    candidate = str(value).strip()
    if candidate.lower() == "nan":
        return ""
    return candidate


def _parse_time_step_value(value: Any, current_ts: pd.Timestamp, period_minutes: int) -> float | None:
    numeric = _safe_float(value)
    if numeric is not None:
        return numeric

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            dt = _parse_timestamp(stripped)
        except Exception:
            return None
        delta_minutes = (dt - current_ts.to_pydatetime()).total_seconds() / 60.0
        return max(0.0, delta_minutes / float(period_minutes))

    return None


def _normalize_soc_percent(value: Any) -> float | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if 0.0 <= numeric <= 1.0:
        numeric *= 100.0
    return float(np.clip(numeric, 0.0, 100.0))


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, float) and np.isnan(value):
        return True
    return False


def _normalize_citylearn_configs(raw: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    if not isinstance(raw, dict):
        raise HTTPException(status_code=400, detail="citylearn_configs must be a JSON object")

    legacy_mode = not any(key in raw for key in RECOGNIZED_CONFIG_KEYS)

    if legacy_mode:
        selected = raw.get("buildings") if isinstance(raw.get("buildings"), list) else None
        schema_overrides = deepcopy(raw)
        schema_overrides.pop("buildings", None)
        return {
            "selected_buildings": selected,
            "schema_overrides": schema_overrides,
            "building_overrides": {},
            "defaults": deepcopy(DEFAULTS_TEMPLATE),
            "validation": {"smoke_check": False},
        }, True

    selected_buildings = raw.get("selected_buildings")
    if selected_buildings is not None and not isinstance(selected_buildings, list):
        raise HTTPException(status_code=400, detail="citylearn_configs.selected_buildings must be a list")

    schema_overrides = raw.get("schema_overrides") or {}
    if not isinstance(schema_overrides, dict):
        raise HTTPException(status_code=400, detail="citylearn_configs.schema_overrides must be an object")

    building_overrides = raw.get("building_overrides") or {}
    if not isinstance(building_overrides, dict):
        raise HTTPException(status_code=400, detail="citylearn_configs.building_overrides must be an object")

    defaults = _deep_merge(DEFAULTS_TEMPLATE, raw.get("defaults") or {})
    validation = raw.get("validation") or {}
    smoke_check = bool(validation.get("smoke_check", False))

    return {
        "selected_buildings": selected_buildings,
        "schema_overrides": deepcopy(schema_overrides),
        "building_overrides": deepcopy(building_overrides),
        "defaults": defaults,
        "validation": {"smoke_check": smoke_check},
    }, False


def _list_energy_communities() -> list[str]:
    names = mongo_utils.list_databases()
    return sorted([name for name in names if name not in _SYSTEM_DATABASES])


def _get_site_schema(db, site_id: str) -> dict[str, Any]:
    if "schema" not in db.list_collection_names():
        raise HTTPException(status_code=404, detail=f"Missing 'schema' collection in site '{site_id}'")

    doc = db["schema"].find_one()
    schema = (doc or {}).get("schema")
    if not isinstance(schema, dict):
        raise HTTPException(status_code=404, detail=f"Missing schema document in site '{site_id}'")

    buildings = schema.get("buildings")
    if not isinstance(buildings, dict) or len(buildings) == 0:
        raise HTTPException(status_code=404, detail=f"Schema for site '{site_id}' does not contain buildings")

    return schema


def _select_building_collections(collection_names: list[str], building_ids: list[str]) -> tuple[dict[str, str], list[str]]:
    mapping: dict[str, str] = {}
    warnings: list[str] = []

    for building_id in building_ids:
        prefix = f"building_{building_id}"
        candidates = sorted([name for name in collection_names if name.startswith(prefix)])
        if not candidates:
            warnings.append(f"Building '{building_id}' skipped: no matching Mongo collection with prefix '{prefix}'.")
            continue
        exact_name = f"building_{building_id}"
        mapping[building_id] = exact_name if exact_name in candidates else candidates[0]

    return mapping, warnings


def _collection_time_bounds(collection) -> tuple[datetime | None, datetime | None]:
    oldest = collection.find_one({"timestamp": {"$exists": True}}, sort=[("timestamp", 1)])
    newest = collection.find_one({"timestamp": {"$exists": True}}, sort=[("timestamp", -1)])

    if oldest is None or newest is None:
        return None, None

    try:
        oldest_ts = _parse_timestamp(oldest.get("timestamp"))
        newest_ts = _parse_timestamp(newest.get("timestamp"))
    except Exception:
        return None, None

    return oldest_ts, newest_ts


def _resolve_time_window(
    db,
    collection_names: list[str],
    period_minutes: int,
    from_ts: str | None,
    until_ts: str | None,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.DatetimeIndex]:
    ranges: list[tuple[datetime, datetime]] = []

    for name in collection_names:
        oldest, newest = _collection_time_bounds(db[name])
        if oldest is None or newest is None:
            continue
        ranges.append((oldest, newest))

    if not ranges:
        raise HTTPException(status_code=404, detail="No timestamped data found in selected building collections.")

    latest_start = max(start for start, _ in ranges)
    earliest_end = min(end for _, end in ranges)

    requested_start = _parse_timestamp(from_ts) if from_ts else latest_start
    requested_end = _parse_timestamp(until_ts) if until_ts else earliest_end

    effective_start = max(requested_start, latest_start)
    effective_end = min(requested_end, earliest_end)

    start_floor = pd.Timestamp(effective_start).floor(f"{period_minutes}min")
    end_floor = pd.Timestamp(effective_end).floor(f"{period_minutes}min")

    if start_floor >= end_floor:
        raise HTTPException(
            status_code=404,
            detail="Invalid time range: no overlapping data for selected buildings and timestamps.",
        )

    target_index = pd.date_range(
        start=start_floor + pd.Timedelta(minutes=period_minutes),
        end=end_floor,
        freq=f"{period_minutes}min",
        tz="UTC",
    )

    if len(target_index) == 0:
        raise HTTPException(status_code=404, detail="No data points available for the requested period.")

    return start_floor, end_floor, target_index


def _flatten_document(doc: dict[str, Any]) -> dict[str, Any]:
    flat = {
        key: value
        for key, value in doc.items()
        if key not in {"_id", "observations", "decisions", "forecasts"}
    }

    observations = doc.get("observations")
    if isinstance(observations, dict):
        flat = {**flat, **observations}

    return flat


def _extract_pricing(payload: dict[str, Any]) -> dict[str, float | None]:
    current = _safe_float(payload.get("electricity_pricing"))
    pred_1 = _safe_float(payload.get("electricity_pricing_predicted_1"))
    pred_2 = _safe_float(payload.get("electricity_pricing_predicted_2"))
    pred_3 = _safe_float(payload.get("electricity_pricing_predicted_3"))

    energy_price = payload.get("energy_price")
    values: list[Any] = []

    if isinstance(energy_price, dict):
        raw_values = energy_price.get("values")
        if isinstance(raw_values, list):
            values = raw_values
        elif raw_values is not None:
            values = [raw_values]
    elif isinstance(energy_price, list):
        values = energy_price
    elif energy_price is not None:
        values = [energy_price]

    if current is None:
        if len(values) > 0:
            current = _safe_float(values[0])

    if pred_1 is None:
        pred_1 = _safe_float(values[1]) if len(values) > 1 else None
    if pred_2 is None:
        pred_2 = _safe_float(values[2]) if len(values) > 2 else None
    if pred_3 is None:
        pred_3 = _safe_float(values[3]) if len(values) > 3 else None

    return {
        "electricity_pricing": current,
        "electricity_pricing_predicted_1": pred_1,
        "electricity_pricing_predicted_2": pred_2,
        "electricity_pricing_predicted_3": pred_3,
    }


def _extract_numeric_series_rows(flat_docs: list[dict[str, Any]]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []

    for payload in flat_docs:
        timestamp = payload.get("timestamp")
        if timestamp is None:
            continue

        try:
            ts = _parse_timestamp(timestamp)
        except Exception:
            continue

        pricing_values = _extract_pricing(payload)
        row = {
            "timestamp": ts,
            "indoor_dry_bulb_temperature": _safe_float(payload.get("indoor_dry_bulb_temperature")),
            "average_unmet_cooling_setpoint_difference": _safe_float(
                payload.get("average_unmet_cooling_setpoint_difference")
            ),
            "indoor_relative_humidity": _safe_float(payload.get("indoor_relative_humidity")),
            "non_shiftable_load": _safe_float(payload.get("non_shiftable_load")),
            "dhw_demand": _safe_float(payload.get("dhw_demand")),
            "cooling_demand": _safe_float(payload.get("cooling_demand")),
            "heating_demand": _safe_float(payload.get("heating_demand")),
            "solar_generation": _safe_float(payload.get("solar_generation")),
            "outdoor_dry_bulb_temperature": _safe_float(payload.get("outdoor_dry_bulb_temperature")),
            "outdoor_relative_humidity": _safe_float(payload.get("outdoor_relative_humidity")),
            "diffuse_solar_irradiance": _safe_float(payload.get("diffuse_solar_irradiance")),
            "direct_solar_irradiance": _safe_float(payload.get("direct_solar_irradiance")),
            "outdoor_dry_bulb_temperature_predicted_1": _safe_float(
                payload.get("outdoor_dry_bulb_temperature_predicted_1")
            ),
            "outdoor_dry_bulb_temperature_predicted_2": _safe_float(
                payload.get("outdoor_dry_bulb_temperature_predicted_2")
            ),
            "outdoor_dry_bulb_temperature_predicted_3": _safe_float(
                payload.get("outdoor_dry_bulb_temperature_predicted_3")
            ),
            "outdoor_relative_humidity_predicted_1": _safe_float(
                payload.get("outdoor_relative_humidity_predicted_1")
            ),
            "outdoor_relative_humidity_predicted_2": _safe_float(
                payload.get("outdoor_relative_humidity_predicted_2")
            ),
            "outdoor_relative_humidity_predicted_3": _safe_float(
                payload.get("outdoor_relative_humidity_predicted_3")
            ),
            "diffuse_solar_irradiance_predicted_1": _safe_float(
                payload.get("diffuse_solar_irradiance_predicted_1")
            ),
            "diffuse_solar_irradiance_predicted_2": _safe_float(
                payload.get("diffuse_solar_irradiance_predicted_2")
            ),
            "diffuse_solar_irradiance_predicted_3": _safe_float(
                payload.get("diffuse_solar_irradiance_predicted_3")
            ),
            "direct_solar_irradiance_predicted_1": _safe_float(
                payload.get("direct_solar_irradiance_predicted_1")
            ),
            "direct_solar_irradiance_predicted_2": _safe_float(
                payload.get("direct_solar_irradiance_predicted_2")
            ),
            "direct_solar_irradiance_predicted_3": _safe_float(
                payload.get("direct_solar_irradiance_predicted_3")
            ),
            "carbon_intensity": _safe_float(payload.get("carbon_intensity")),
            **pricing_values,
        }

        rows.append(row)

    if len(rows) == 0:
        return pd.DataFrame(columns=["timestamp"])

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).sort_values("timestamp").set_index("timestamp")

    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _resample_numeric_frame(df: pd.DataFrame, target_index: pd.DatetimeIndex, period_minutes: int) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(index=target_index)

    agg_map = {
        "indoor_dry_bulb_temperature": "mean",
        "average_unmet_cooling_setpoint_difference": "mean",
        "indoor_relative_humidity": "mean",
        "non_shiftable_load": "sum",
        "dhw_demand": "sum",
        "cooling_demand": "sum",
        "heating_demand": "sum",
        "solar_generation": "sum",
        "outdoor_dry_bulb_temperature": "mean",
        "outdoor_relative_humidity": "mean",
        "diffuse_solar_irradiance": "mean",
        "direct_solar_irradiance": "mean",
        "outdoor_dry_bulb_temperature_predicted_1": "mean",
        "outdoor_dry_bulb_temperature_predicted_2": "mean",
        "outdoor_dry_bulb_temperature_predicted_3": "mean",
        "outdoor_relative_humidity_predicted_1": "mean",
        "outdoor_relative_humidity_predicted_2": "mean",
        "outdoor_relative_humidity_predicted_3": "mean",
        "diffuse_solar_irradiance_predicted_1": "mean",
        "diffuse_solar_irradiance_predicted_2": "mean",
        "diffuse_solar_irradiance_predicted_3": "mean",
        "direct_solar_irradiance_predicted_1": "mean",
        "direct_solar_irradiance_predicted_2": "mean",
        "direct_solar_irradiance_predicted_3": "mean",
        "carbon_intensity": "mean",
        "electricity_pricing": "mean",
        "electricity_pricing_predicted_1": "mean",
        "electricity_pricing_predicted_2": "mean",
        "electricity_pricing_predicted_3": "mean",
    }

    rule = f"{period_minutes}min"
    available_agg = {k: v for k, v in agg_map.items() if k in df.columns}
    resampled = df.resample(rule, label="right", closed="right").agg(available_agg)
    return resampled.reindex(target_index)


def _fill_series_with_defaults(
    frame: pd.DataFrame,
    columns: list[str],
    defaults: dict[str, Any],
    warnings: list[str],
    warning_prefix: str,
) -> pd.DataFrame:
    out = frame.copy()

    for col in columns:
        if col not in out.columns:
            out[col] = np.nan

        missing_count = int(out[col].isna().sum())
        if missing_count > 0:
            warnings.append(
                f"{warning_prefix}: filled {missing_count} missing values for '{col}' with deterministic defaults."
            )

        default_value = defaults.get(col)
        if default_value is not None:
            out[col] = out[col].fillna(default_value)

    return out


def _fill_predicted_from_base(frame: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = frame.copy()
    for pred_col, base_col in mapping.items():
        if pred_col not in out.columns or base_col not in out.columns:
            continue
        out[pred_col] = out[pred_col].where(~out[pred_col].isna(), out[base_col])
    return out


def _build_time_columns(index: pd.DatetimeIndex) -> pd.DataFrame:
    lisbon = index.tz_convert(ZoneInfo("Europe/Lisbon"))
    hours = lisbon.hour.astype(int)
    hours = np.where(hours == 0, 24, hours)

    return pd.DataFrame(
        {
            "month": lisbon.month.astype(int),
            "hour": hours,
            "day_type": lisbon.isocalendar().day.astype(int),
            "daylight_savings_status": [int(bool(ts.dst())) for ts in lisbon.to_pydatetime()],
        },
        index=index,
    )


def _extract_charger_map_from_building(building_schema: dict[str, Any]) -> dict[str, dict[str, Any]]:
    chargers = building_schema.get("chargers")
    if isinstance(chargers, dict) and len(chargers) > 0:
        return deepcopy(chargers)

    ev_chargers = building_schema.get("ev_chargers")
    if isinstance(ev_chargers, dict) and len(ev_chargers) > 0:
        return deepcopy(ev_chargers)

    return {}


def _default_building_schema(building_name: str, filename: str) -> dict[str, Any]:
    return {
        "include": True,
        "energy_simulation": filename,
        "weather": "weather.csv",
        "carbon_intensity": "carbon_intensity.csv",
        "pricing": "pricing.csv",
        "inactive_observations": [],
        "inactive_actions": [],
        "electrical_storage": {
            "type": "citylearn.energy_model.Battery",
            "autosize": False,
            "attributes": {
                "capacity": 6.4,
                "efficiency": 0.9,
                "capacity_loss_coefficient": 1e-5,
                "loss_coefficient": 0.0,
                "nominal_power": 5.0,
            },
        },
        "pv": {
            "type": "citylearn.energy_model.PV",
            "autosize": False,
            "attributes": {
                "nominal_power": 1.0,
            },
        },
    }


def _default_charger_schema(
    charger_id: str,
    filename: str,
    defaults: dict[str, Any],
) -> dict[str, Any]:
    return {
        "type": "citylearn.electric_vehicle_charger.Charger",
        "charger_simulation": filename,
        "autosize": False,
        "attributes": deepcopy(defaults.get("charger_attributes", DEFAULTS_TEMPLATE["charger_attributes"])),
    }


def _default_ev_definition(ev_id: str, defaults: dict[str, Any]) -> dict[str, Any]:
    ev_defaults = defaults.get("electric_vehicle", {})
    return {
        "include": True,
        "battery": {
            "type": "citylearn.energy_model.Battery",
            "autosize": False,
            "attributes": {
                "capacity": float(ev_defaults.get("battery_capacity", 50.0)),
                "nominal_power": float(ev_defaults.get("nominal_power", 50.0)),
                "initial_soc": float(ev_defaults.get("initial_soc", 0.5)),
                "depth_of_discharge": float(ev_defaults.get("depth_of_discharge", 0.9)),
            },
        },
    }


def _base_schema_template() -> dict[str, Any]:
    observations = {
        "month": {"active": True, "shared_in_central_agent": True},
        "day_type": {"active": True, "shared_in_central_agent": True},
        "hour": {"active": True, "shared_in_central_agent": True},
        "daylight_savings_status": {"active": False, "shared_in_central_agent": True},
        "outdoor_dry_bulb_temperature": {"active": True, "shared_in_central_agent": True},
        "outdoor_dry_bulb_temperature_predicted_1": {"active": True, "shared_in_central_agent": True},
        "outdoor_dry_bulb_temperature_predicted_2": {"active": True, "shared_in_central_agent": True},
        "outdoor_dry_bulb_temperature_predicted_3": {"active": True, "shared_in_central_agent": True},
        "outdoor_relative_humidity": {"active": True, "shared_in_central_agent": True},
        "outdoor_relative_humidity_predicted_1": {"active": True, "shared_in_central_agent": True},
        "outdoor_relative_humidity_predicted_2": {"active": True, "shared_in_central_agent": True},
        "outdoor_relative_humidity_predicted_3": {"active": True, "shared_in_central_agent": True},
        "diffuse_solar_irradiance": {"active": True, "shared_in_central_agent": True},
        "diffuse_solar_irradiance_predicted_1": {"active": True, "shared_in_central_agent": True},
        "diffuse_solar_irradiance_predicted_2": {"active": True, "shared_in_central_agent": True},
        "diffuse_solar_irradiance_predicted_3": {"active": True, "shared_in_central_agent": True},
        "direct_solar_irradiance": {"active": True, "shared_in_central_agent": True},
        "direct_solar_irradiance_predicted_1": {"active": True, "shared_in_central_agent": True},
        "direct_solar_irradiance_predicted_2": {"active": True, "shared_in_central_agent": True},
        "direct_solar_irradiance_predicted_3": {"active": True, "shared_in_central_agent": True},
        "carbon_intensity": {"active": True, "shared_in_central_agent": True},
        "indoor_dry_bulb_temperature": {"active": False, "shared_in_central_agent": False},
        "average_unmet_cooling_setpoint_difference": {"active": False, "shared_in_central_agent": False},
        "indoor_relative_humidity": {"active": False, "shared_in_central_agent": False},
        "non_shiftable_load": {"active": True, "shared_in_central_agent": False},
        "solar_generation": {"active": True, "shared_in_central_agent": False},
        "electrical_storage_soc": {"active": True, "shared_in_central_agent": False},
        "net_electricity_consumption": {"active": True, "shared_in_central_agent": False},
        "electricity_pricing": {"active": True, "shared_in_central_agent": True},
        "electricity_pricing_predicted_1": {"active": True, "shared_in_central_agent": True},
        "electricity_pricing_predicted_2": {"active": True, "shared_in_central_agent": True},
        "electricity_pricing_predicted_3": {"active": True, "shared_in_central_agent": True},
        "power_outage": {"active": False, "shared_in_central_agent": False},
        "comfort_band": {"active": False, "shared_in_central_agent": False},
        "electric_vehicle_charger_connected_state": {"active": True, "shared_in_central_agent": True},
        "connected_electric_vehicle_at_charger_battery_capacity": {"active": True, "shared_in_central_agent": True},
        "connected_electric_vehicle_at_charger_departure_time": {"active": True, "shared_in_central_agent": True},
        "connected_electric_vehicle_at_charger_required_soc_departure": {"active": True, "shared_in_central_agent": True},
        "connected_electric_vehicle_at_charger_soc": {"active": True, "shared_in_central_agent": True},
        "electric_vehicle_charger_incoming_state": {"active": True, "shared_in_central_agent": True},
        "incoming_electric_vehicle_at_charger_estimated_arrival_time": {
            "active": True,
            "shared_in_central_agent": True,
        },
        "incoming_electric_vehicle_at_charger_estimated_soc_arrival": {
            "active": True,
            "shared_in_central_agent": True,
        },
        "washing_machine_start_time_step": {"active": True, "shared_in_central_agent": True},
        "washing_machine_end_time_step": {"active": True, "shared_in_central_agent": True},
    }

    actions = {
        "cooling_storage": {"active": False},
        "heating_storage": {"active": False},
        "dhw_storage": {"active": False},
        "electrical_storage": {"active": True},
        "electric_vehicle_storage": {"active": True},
        "washing_machine": {"active": False},
    }

    return {
        "random_seed": 2022,
        "root_directory": None,
        "central_agent": False,
        "simulation_start_time_step": 0,
        "simulation_end_time_step": 0,
        "episode_time_steps": None,
        "rolling_episode_split": False,
        "random_episode_split": False,
        "seconds_per_time_step": 3600,
        "observations": observations,
        "actions": actions,
        "agent": {
            "type": "citylearn.agents.rbc.BasicElectricVehicleRBC_ReferenceController",
            "attributes": {},
        },
        "reward_function": {
            "type": "citylearn.reward_function.Electric_Vehicles_Reward_Function",
            "attributes": {},
        },
        "electric_vehicles_def": {},
        "buildings": {},
    }


def _infer_charger_ids_from_sessions(resampled_sessions: pd.DataFrame) -> list[str]:
    inferred: set[str] = set()

    if "charging_sessions" not in resampled_sessions.columns:
        return []

    for item in resampled_sessions["charging_sessions"].tolist():
        if isinstance(item, dict):
            for key in item.keys():
                if _safe_str(key):
                    inferred.add(_safe_str(key))

    return sorted(inferred)


def _pick_session_from_list(entries: list[Any], charger_id: str | None = None) -> dict[str, Any]:
    normalized = [entry for entry in entries if isinstance(entry, dict)]

    if len(normalized) == 0:
        return {}

    if charger_id:
        for entry in normalized:
            if _safe_str(entry.get("charger_id")) == charger_id:
                return entry

    def score(entry: dict[str, Any]) -> tuple[int, float]:
        ev = _safe_str(entry.get("electric_vehicle") or entry.get("electric_vehicle_id") or entry.get("user_id"))
        power = _safe_float(entry.get("power"))
        return (1 if ev else 0, abs(power or 0.0))

    return sorted(normalized, key=score, reverse=True)[0]


def _build_charger_rows(
    building_id: str,
    charger_ids: list[str],
    resampled_sessions: pd.DataFrame,
    period_minutes: int,
    defaults: dict[str, Any],
    warnings: list[str],
) -> tuple[dict[str, pd.DataFrame], set[str], bool]:
    charger_rows: dict[str, list[dict[str, Any]]] = {cid: [] for cid in charger_ids}
    observed_ev_ids: set[str] = set()

    if len(charger_ids) == 0:
        return {}, observed_ev_ids, False

    disable_ev = False

    for ts, row in resampled_sessions.iterrows():
        sessions = row.get("charging_sessions")
        electric_vehicles = row.get("electric_vehicles")
        electric_vehicles = electric_vehicles if isinstance(electric_vehicles, dict) else {}

        if isinstance(sessions, list) and len(sessions) > 0 and len(charger_ids) != 1:
            disable_ev = True
            break

        for charger_id in charger_ids:
            default_row = {
                "electric_vehicle_charger_state": defaults["charger"]["electric_vehicle_charger_state"],
                "electric_vehicle_id": "",
                "electric_vehicle_departure_time": defaults["charger"]["electric_vehicle_departure_time"],
                "electric_vehicle_required_soc_departure": defaults["charger"]["electric_vehicle_required_soc_departure"],
                "electric_vehicle_estimated_arrival_time": defaults["charger"]["electric_vehicle_estimated_arrival_time"],
                "electric_vehicle_estimated_soc_arrival": defaults["charger"]["electric_vehicle_estimated_soc_arrival"],
            }

            session: dict[str, Any] = {}
            if isinstance(sessions, dict):
                raw_session = sessions.get(charger_id)
                if isinstance(raw_session, dict):
                    session = raw_session
            elif isinstance(sessions, list) and len(charger_ids) == 1:
                session = _pick_session_from_list(sessions, charger_id)

            if session:
                flexibility = session.get("flexibility") if isinstance(session.get("flexibility"), dict) else {}
                ev_id = _safe_str(
                    session.get("electric_vehicle")
                    or session.get("electric_vehicle_id")
                    or session.get("user_id")
                )

                state = _safe_float(session.get("electric_vehicle_charger_state"))
                if state is None:
                    if ev_id:
                        state = 1
                    elif _parse_time_step_value(
                        session.get("electric_vehicle_estimated_arrival_time")
                        or flexibility.get("estimated_time_at_arrival")
                        or flexibility.get("arrival.time"),
                        ts,
                        period_minutes,
                    ) is not None:
                        state = 2
                    else:
                        state = defaults["charger"]["electric_vehicle_charger_state"]

                departure_time = _parse_time_step_value(
                    session.get("electric_vehicle_departure_time")
                    or session.get("departure_time")
                    or flexibility.get("estimated_time_at_departure")
                    or flexibility.get("departure.time"),
                    ts,
                    period_minutes,
                )
                arrival_time = _parse_time_step_value(
                    session.get("electric_vehicle_estimated_arrival_time")
                    or session.get("arrival_time")
                    or flexibility.get("estimated_time_at_arrival")
                    or flexibility.get("arrival.time"),
                    ts,
                    period_minutes,
                )

                required_soc = _normalize_soc_percent(
                    session.get("electric_vehicle_required_soc_departure")
                    or flexibility.get("estimated_soc_at_departure")
                    or flexibility.get("departure.soc")
                )
                estimated_soc = _normalize_soc_percent(
                    session.get("electric_vehicle_estimated_soc_arrival")
                    or flexibility.get("estimated_soc_at_arrival")
                    or session.get("soc")
                )

                if ev_id and ev_id in electric_vehicles and isinstance(electric_vehicles.get(ev_id), dict):
                    vehicle_payload = electric_vehicles.get(ev_id) or {}
                    if required_soc is None:
                        required_soc = _normalize_soc_percent(
                            (vehicle_payload.get("flexibility") or {}).get("estimated_soc_at_departure")
                        )
                    if estimated_soc is None:
                        estimated_soc = _normalize_soc_percent(
                            (vehicle_payload.get("flexibility") or {}).get("estimated_soc_at_arrival")
                        )

                if ev_id:
                    observed_ev_ids.add(ev_id)

                default_row.update(
                    {
                        "electric_vehicle_charger_state": int(state),
                        "electric_vehicle_id": ev_id,
                        "electric_vehicle_departure_time": departure_time,
                        "electric_vehicle_required_soc_departure": required_soc,
                        "electric_vehicle_estimated_arrival_time": arrival_time,
                        "electric_vehicle_estimated_soc_arrival": estimated_soc,
                    }
                )

            charger_rows[charger_id].append(default_row)

    if disable_ev:
        warnings.append(
            "EV disabled for building "
            f"'{building_id}': charging_sessions are list-based with multiple configured chargers (ambiguous mapping)."
        )
        return {}, observed_ev_ids, True

    frames: dict[str, pd.DataFrame] = {}

    for charger_id, rows in charger_rows.items():
        frame = pd.DataFrame(rows)
        for col in CHARGER_COLUMNS:
            if col not in frame.columns:
                frame[col] = np.nan

        frames[charger_id] = frame[CHARGER_COLUMNS]

    return frames, observed_ev_ids, False


def _build_static_validation(
    dataset_dir: str,
    schema: dict[str, Any],
    expected_length: int,
) -> dict[str, Any]:
    errors: list[str] = []

    required_top_level = [
        "observations",
        "actions",
        "buildings",
        "simulation_start_time_step",
        "simulation_end_time_step",
        "seconds_per_time_step",
    ]

    for key in required_top_level:
        if key not in schema:
            errors.append(f"schema.json missing required key '{key}'.")

    buildings = schema.get("buildings") if isinstance(schema.get("buildings"), dict) else {}
    if len(buildings) == 0:
        errors.append("schema.json has no buildings.")

    referenced_files: set[str] = set()

    for building_name, building in buildings.items():
        for field in ["energy_simulation", "weather", "pricing", "carbon_intensity"]:
            filename = building.get(field)
            if not isinstance(filename, str) or not filename:
                errors.append(f"buildings.{building_name}.{field} is missing or invalid.")
                continue
            referenced_files.add(filename)

        chargers = building.get("chargers") if isinstance(building.get("chargers"), dict) else {}
        for charger_name, charger in chargers.items():
            filename = charger.get("charger_simulation") if isinstance(charger, dict) else None
            if not isinstance(filename, str) or not filename:
                errors.append(
                    f"buildings.{building_name}.chargers.{charger_name}.charger_simulation is missing or invalid."
                )
            else:
                referenced_files.add(filename)

        washing_machines = (
            building.get("washing_machines")
            if isinstance(building.get("washing_machines"), dict)
            else {}
        )
        for wm_name, wm_cfg in washing_machines.items():
            filename = wm_cfg.get("washing_machine_energy_simulation") if isinstance(wm_cfg, dict) else None
            if not isinstance(filename, str) or not filename:
                errors.append(
                    "buildings."
                    f"{building_name}.washing_machines.{wm_name}.washing_machine_energy_simulation is missing or invalid."
                )
            else:
                referenced_files.add(filename)

    for filename in sorted(referenced_files):
        path = os.path.join(dataset_dir, filename)
        if not os.path.isfile(path):
            errors.append(f"Referenced file '{filename}' does not exist in dataset directory.")

    def _check_csv(path: str, expected_columns: list[str], label: str, expect_length: int = expected_length):
        if not os.path.isfile(path):
            return
        try:
            frame = pd.read_csv(path)
        except Exception as exc:
            errors.append(f"Failed to read {label} '{os.path.basename(path)}': {exc}")
            return

        if list(frame.columns) != expected_columns:
            errors.append(
                f"{label} '{os.path.basename(path)}' has incompatible headers. "
                f"Expected {expected_columns}, got {list(frame.columns)}."
            )

        if len(frame) != expect_length:
            errors.append(
                f"{label} '{os.path.basename(path)}' has {len(frame)} rows; expected {expect_length}."
            )

    for building in buildings.values():
        energy_file = building.get("energy_simulation")
        if isinstance(energy_file, str):
            _check_csv(os.path.join(dataset_dir, energy_file), BUILDING_COLUMNS, "energy_simulation")

    shared_files = {
        "pricing": (schema.get("buildings", {}), "pricing", PRICING_COLUMNS),
        "weather": (schema.get("buildings", {}), "weather", WEATHER_COLUMNS),
        "carbon_intensity": (schema.get("buildings", {}), "carbon_intensity", CARBON_COLUMNS),
    }

    for _, (buildings_map, field, columns) in shared_files.items():
        seen: set[str] = set()
        for building in buildings_map.values():
            filename = building.get(field)
            if isinstance(filename, str) and filename not in seen:
                seen.add(filename)
                _check_csv(os.path.join(dataset_dir, filename), columns, field)

    for building in buildings.values():
        chargers = building.get("chargers") if isinstance(building.get("chargers"), dict) else {}
        for charger in chargers.values():
            filename = charger.get("charger_simulation") if isinstance(charger, dict) else None
            if isinstance(filename, str):
                _check_csv(
                    os.path.join(dataset_dir, filename),
                    CHARGER_COLUMNS,
                    "charger_simulation",
                )

        washing = building.get("washing_machines") if isinstance(building.get("washing_machines"), dict) else {}
        for wm in washing.values():
            filename = wm.get("washing_machine_energy_simulation") if isinstance(wm, dict) else None
            if isinstance(filename, str):
                _check_csv(
                    os.path.join(dataset_dir, filename),
                    WASHING_MACHINE_COLUMNS,
                    "washing_machine_simulation",
                )

    return {
        "ok": len(errors) == 0,
        "errors": errors,
    }


def _run_smoke_check(dataset_dir: str, enabled: bool) -> dict[str, Any]:
    result = {
        "requested": bool(enabled),
        "executed": False,
        "ok": None,
        "error": None,
    }

    if not enabled:
        return result

    schema_path = os.path.join(dataset_dir, "schema.json")

    try:
        from citylearn.citylearn import CityLearnEnv  # type: ignore
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"citylearn is not available in runtime ({exc})."
        return result

    try:
        env = CityLearnEnv(schema=schema_path)
        env.reset()
        result["executed"] = True
        result["ok"] = True
    except Exception as exc:
        result["executed"] = True
        result["ok"] = False
        result["error"] = str(exc)

    return result


def list_citylearn_compatible_sites() -> list[dict[str, Any]]:
    sites: list[dict[str, Any]] = []

    for site_id in _list_energy_communities():
        db = mongo_utils.get_db(site_id)

        try:
            schema = _get_site_schema(db, site_id)
        except HTTPException:
            continue

        building_ids = sorted((schema.get("buildings") or {}).keys())
        collection_names = db.list_collection_names()
        mapping, _ = _select_building_collections(collection_names, building_ids)
        available = [building for building in building_ids if building in mapping]

        if len(available) == 0:
            continue

        sites.append({"site_id": site_id, "buildings": available})

    return sorted(sites, key=lambda item: item["site_id"])


def generate_citylearn_dataset(
    name: str,
    site_id: str,
    citylearn_configs: dict[str, Any],
    description: str = "",
    period: int = 60,
    from_ts: str | None = None,
    until_ts: str | None = None,
) -> dict[str, Any]:
    if period < 1:
        raise HTTPException(status_code=400, detail="period must be >= 1 minute")

    config, legacy_mode = _normalize_citylearn_configs(citylearn_configs)
    warnings: list[str] = []

    if legacy_mode:
        warnings.append("Legacy citylearn_configs detected: interpreted as schema_overrides with optional 'buildings'.")

    db = mongo_utils.get_db(site_id)
    site_schema = _get_site_schema(db, site_id)

    all_buildings = sorted((site_schema.get("buildings") or {}).keys())
    selected_buildings = config.get("selected_buildings") or all_buildings
    selected_buildings = [str(item) for item in selected_buildings]

    invalid_selected = [bid for bid in selected_buildings if bid not in all_buildings]
    if invalid_selected:
        warnings.append(
            f"Ignored unknown selected_buildings entries: {', '.join(sorted(set(invalid_selected)))}."
        )

    selected_buildings = [bid for bid in selected_buildings if bid in all_buildings]

    if len(selected_buildings) == 0:
        raise HTTPException(status_code=400, detail="No valid buildings selected for dataset generation.")

    collection_names = db.list_collection_names()
    building_collections, collection_warnings = _select_building_collections(collection_names, selected_buildings)
    warnings.extend(collection_warnings)

    if len(building_collections) == 0:
        raise HTTPException(
            status_code=404,
            detail="No data collections found for selected buildings.",
        )

    selected_buildings = [bid for bid in selected_buildings if bid in building_collections]
    start_floor, end_floor, target_index = _resolve_time_window(
        db,
        [building_collections[bid] for bid in selected_buildings],
        period,
        from_ts,
        until_ts,
    )

    dataset_path = os.path.join(settings.DATASETS_DIR, name)
    os.makedirs(settings.DATASETS_DIR, exist_ok=True)

    if os.path.exists(dataset_path):
        shutil.rmtree(dataset_path, ignore_errors=True)

    os.makedirs(dataset_path, exist_ok=True)

    try:
        defaults = config["defaults"]
        expected_length = len(target_index)

        time_frame = _build_time_columns(target_index)

        buildings_schema: dict[str, Any] = {}
        global_ev_ids: set[str] = set()

        reference_collection_name = building_collections[selected_buildings[0]]
        reference_docs = list(
            db[reference_collection_name].find(
                {
                    "timestamp": {
                        "$gte": (start_floor - pd.Timedelta(minutes=period)).to_pydatetime(),
                        "$lte": end_floor.to_pydatetime(),
                    }
                }
            )
        )

        if len(reference_docs) == 0:
            raise HTTPException(status_code=404, detail="No records available for pricing/weather/carbon reference data.")

        reference_flat_docs = [_flatten_document(doc) for doc in reference_docs]
        reference_df = _extract_numeric_series_rows(reference_flat_docs)
        reference_resampled = _resample_numeric_frame(reference_df, target_index, period)

        pricing_frame = _fill_series_with_defaults(
            _fill_predicted_from_base(
                reference_resampled[[col for col in PRICING_COLUMNS if col in reference_resampled.columns]]
                if len(reference_resampled.columns) > 0
                else pd.DataFrame(index=target_index),
                {
                    "electricity_pricing_predicted_1": "electricity_pricing",
                    "electricity_pricing_predicted_2": "electricity_pricing",
                    "electricity_pricing_predicted_3": "electricity_pricing",
                },
            ),
            PRICING_COLUMNS,
            defaults["pricing"],
            warnings,
            "pricing",
        )
        pricing_path = os.path.join(dataset_path, "pricing.csv")
        pricing_frame[PRICING_COLUMNS].to_csv(pricing_path, index=False)

        weather_frame = _fill_series_with_defaults(
            _fill_predicted_from_base(
                reference_resampled[[col for col in WEATHER_COLUMNS if col in reference_resampled.columns]]
                if len(reference_resampled.columns) > 0
                else pd.DataFrame(index=target_index),
                {
                    "outdoor_dry_bulb_temperature_predicted_1": "outdoor_dry_bulb_temperature",
                    "outdoor_dry_bulb_temperature_predicted_2": "outdoor_dry_bulb_temperature",
                    "outdoor_dry_bulb_temperature_predicted_3": "outdoor_dry_bulb_temperature",
                    "outdoor_relative_humidity_predicted_1": "outdoor_relative_humidity",
                    "outdoor_relative_humidity_predicted_2": "outdoor_relative_humidity",
                    "outdoor_relative_humidity_predicted_3": "outdoor_relative_humidity",
                    "diffuse_solar_irradiance_predicted_1": "diffuse_solar_irradiance",
                    "diffuse_solar_irradiance_predicted_2": "diffuse_solar_irradiance",
                    "diffuse_solar_irradiance_predicted_3": "diffuse_solar_irradiance",
                    "direct_solar_irradiance_predicted_1": "direct_solar_irradiance",
                    "direct_solar_irradiance_predicted_2": "direct_solar_irradiance",
                    "direct_solar_irradiance_predicted_3": "direct_solar_irradiance",
                },
            ),
            WEATHER_COLUMNS,
            defaults["weather"],
            warnings,
            "weather",
        )
        weather_path = os.path.join(dataset_path, "weather.csv")
        weather_frame[WEATHER_COLUMNS].to_csv(weather_path, index=False)

        carbon_frame = _fill_series_with_defaults(
            reference_resampled[["carbon_intensity"]]
            if "carbon_intensity" in reference_resampled.columns
            else pd.DataFrame(index=target_index),
            CARBON_COLUMNS,
            defaults["carbon_intensity"],
            warnings,
            "carbon_intensity",
        )
        carbon_path = os.path.join(dataset_path, "carbon_intensity.csv")
        carbon_frame[CARBON_COLUMNS].to_csv(carbon_path, index=False)

        for idx, building_id in enumerate(selected_buildings, start=1):
            collection_name = building_collections[building_id]
            docs = list(
                db[collection_name].find(
                    {
                        "timestamp": {
                            "$gte": (start_floor - pd.Timedelta(minutes=period)).to_pydatetime(),
                            "$lte": end_floor.to_pydatetime(),
                        }
                    }
                )
            )

            if len(docs) == 0:
                warnings.append(
                    f"Building '{building_id}' skipped: no records in requested time window after filtering."
                )
                continue

            flat_docs = [_flatten_document(doc) for doc in docs]
            numeric_df = _extract_numeric_series_rows(flat_docs)
            numeric_resampled = _resample_numeric_frame(numeric_df, target_index, period)

            building_numeric = _fill_series_with_defaults(
                numeric_resampled[[col for col in BUILDING_COLUMNS if col in numeric_resampled.columns]]
                if len(numeric_resampled.columns) > 0
                else pd.DataFrame(index=target_index),
                [
                    "indoor_dry_bulb_temperature",
                    "average_unmet_cooling_setpoint_difference",
                    "indoor_relative_humidity",
                    "non_shiftable_load",
                    "dhw_demand",
                    "cooling_demand",
                    "heating_demand",
                    "solar_generation",
                ],
                defaults["building"],
                warnings,
                f"building '{building_id}'",
            )

            building_frame = pd.concat([time_frame, building_numeric], axis=1)
            building_frame = building_frame[BUILDING_COLUMNS]

            building_filename = f"{_slug(building_id, f'building_{idx}')}.csv"
            building_frame.to_csv(os.path.join(dataset_path, building_filename), index=False)

            source_building_schema = deepcopy((site_schema.get("buildings") or {}).get(building_id, {}))
            merged_building_schema = _deep_merge(
                _default_building_schema(building_id, building_filename),
                source_building_schema,
            )

            merged_building_schema["energy_simulation"] = building_filename
            merged_building_schema["weather"] = "weather.csv"
            merged_building_schema["pricing"] = "pricing.csv"
            merged_building_schema["carbon_intensity"] = "carbon_intensity.csv"
            merged_building_schema["include"] = True
            merged_building_schema.setdefault("inactive_observations", [])
            merged_building_schema.setdefault("inactive_actions", [])

            session_rows = []
            for payload in flat_docs:
                timestamp = payload.get("timestamp")
                if timestamp is None:
                    continue
                try:
                    ts = _parse_timestamp(timestamp)
                except Exception:
                    continue
                session_rows.append(
                    {
                        "timestamp": ts,
                        "charging_sessions": payload.get("charging_sessions"),
                        "electric_vehicles": payload.get("electric_vehicles"),
                    }
                )

            session_df = pd.DataFrame(session_rows)
            if not session_df.empty:
                session_df["timestamp"] = pd.to_datetime(session_df["timestamp"], utc=True, errors="coerce")
                session_df = (
                    session_df.dropna(subset=["timestamp"]) \
                    .sort_values("timestamp") \
                    .set_index("timestamp") \
                    .resample(f"{period}min", label="right", closed="right") \
                    .last() \
                    .reindex(target_index)
                )
            else:
                session_df = pd.DataFrame(index=target_index, columns=["charging_sessions", "electric_vehicles"])

            charger_source = _extract_charger_map_from_building(source_building_schema)
            charger_ids = sorted(charger_source.keys())
            if len(charger_ids) == 0:
                inferred = _infer_charger_ids_from_sessions(session_df)
                if inferred:
                    charger_ids = inferred
                    warnings.append(
                        f"Building '{building_id}': inferred chargers from telemetry because schema had no charger definitions."
                    )

            charger_frames, observed_ev_ids, ev_disabled = _build_charger_rows(
                building_id,
                charger_ids,
                session_df,
                period,
                defaults,
                warnings,
            )

            if ev_disabled or len(charger_frames) == 0:
                merged_building_schema.pop("chargers", None)
            else:
                chargers_schema: dict[str, Any] = {}

                for charger_id, charger_frame in charger_frames.items():
                    charger_filename = (
                        f"charger_{_slug(building_id, 'b')}_{_slug(charger_id, 'charger')}.csv"
                    )
                    charger_frame.to_csv(
                        os.path.join(dataset_path, charger_filename),
                        index=False,
                    )

                    existing_schema = charger_source.get(charger_id, {})
                    charger_schema = _deep_merge(
                        _default_charger_schema(charger_id, charger_filename, defaults),
                        existing_schema if isinstance(existing_schema, dict) else {},
                    )
                    charger_schema["charger_simulation"] = charger_filename
                    chargers_schema[charger_id] = charger_schema

                merged_building_schema["chargers"] = chargers_schema
                global_ev_ids.update(observed_ev_ids)

            washing_machines = source_building_schema.get("washing_machines")
            if isinstance(washing_machines, dict) and len(washing_machines) > 0:
                normalized_washing: dict[str, Any] = {}
                for wm_name, wm_cfg in washing_machines.items():
                    wm_filename = (
                        f"washing_machine_{_slug(building_id, 'b')}_{_slug(wm_name, 'wm')}.csv"
                    )
                    wm_frame = pd.DataFrame(
                        {
                            "day_type": time_frame["day_type"].values,
                            "hour": time_frame["hour"].values,
                            "wm_start_time_step": [-1] * expected_length,
                            "wm_end_time_step": [-1] * expected_length,
                            "load_profile": ["-1"] * expected_length,
                        }
                    )
                    wm_frame.to_csv(os.path.join(dataset_path, wm_filename), index=False)

                    wm_schema = _deep_merge(
                        {
                            "type": "citylearn.energy_model.WashingMachine",
                            "autosize": False,
                            "washing_machine_energy_simulation": wm_filename,
                        },
                        wm_cfg if isinstance(wm_cfg, dict) else {},
                    )
                    wm_schema["washing_machine_energy_simulation"] = wm_filename
                    normalized_washing[wm_name] = wm_schema

                merged_building_schema["washing_machines"] = normalized_washing

            buildings_schema[building_id] = merged_building_schema

        if len(buildings_schema) == 0:
            raise HTTPException(
                status_code=404,
                detail="No buildings were exported. Check selected buildings and data availability.",
            )

        base_schema = _base_schema_template()
        base_schema["description"] = description or ""
        base_schema["seconds_per_time_step"] = int(period) * 60
        base_schema["simulation_start_time_step"] = 0
        base_schema["simulation_end_time_step"] = expected_length - 1
        base_schema["buildings"] = buildings_schema

        site_ev_schema = site_schema.get("electric_vehicles_def")
        if isinstance(site_ev_schema, dict):
            base_schema["electric_vehicles_def"] = _deep_merge(base_schema["electric_vehicles_def"], site_ev_schema)

        site_evs = site_schema.get("evs")
        if isinstance(site_evs, dict):
            for ev_id, ev_payload in site_evs.items():
                default_ev = _default_ev_definition(ev_id, defaults)
                if isinstance(ev_payload, dict):
                    battery_attrs = default_ev["battery"]["attributes"]
                    cap = _safe_float(ev_payload.get("battery_capacity"))
                    power = _safe_float(ev_payload.get("charging_power"))
                    if cap is not None:
                        battery_attrs["capacity"] = cap
                    if power is not None:
                        battery_attrs["nominal_power"] = power
                base_schema["electric_vehicles_def"][ev_id] = _deep_merge(
                    default_ev,
                    base_schema["electric_vehicles_def"].get(ev_id, {}),
                )

        for ev_id in sorted(global_ev_ids):
            if ev_id not in base_schema["electric_vehicles_def"]:
                base_schema["electric_vehicles_def"][ev_id] = _default_ev_definition(ev_id, defaults)

        final_schema = _deep_merge(base_schema, config["schema_overrides"])

        building_overrides = config.get("building_overrides") or {}
        for building_id, override in building_overrides.items():
            if not isinstance(override, dict):
                warnings.append(
                    f"Ignored building_overrides for '{building_id}': value must be an object."
                )
                continue

            if building_id not in final_schema.get("buildings", {}):
                warnings.append(
                    f"Ignored building_overrides for unknown building '{building_id}'."
                )
                continue

            final_schema["buildings"][building_id] = _deep_merge(
                final_schema["buildings"][building_id],
                override,
            )

        schema_path = os.path.join(dataset_path, "schema.json")
        with open(schema_path, "w", encoding="utf-8") as handle:
            json.dump(final_schema, handle, indent=2)

        static_validation = _build_static_validation(dataset_path, final_schema, expected_length)
        if not static_validation["ok"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Generated dataset failed CityLearn static validation.",
                    "errors": static_validation["errors"],
                },
            )

        smoke_validation = _run_smoke_check(dataset_path, config["validation"]["smoke_check"])

        validation = {
            "static": static_validation,
            "smoke_check": smoke_validation,
            "time_window": {
                "from_ts": target_index[0].isoformat(),
                "until_ts": target_index[-1].isoformat(),
                "rows": expected_length,
            },
        }

        if config["validation"]["smoke_check"] and smoke_validation.get("ok") is False:
            warnings.append(
                "CityLearn smoke_check failed. Dataset was still generated because smoke_check is optional."
            )

        return {
            "path": dataset_path,
            "warnings": sorted(set(warnings)),
            "validation": validation,
        }

    except Exception:
        shutil.rmtree(dataset_path, ignore_errors=True)
        raise
