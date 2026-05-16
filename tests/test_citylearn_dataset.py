import pandas as pd

from app.utils import citylearn_dataset as c


def test_normalize_citylearn_configs_legacy_supports_buildings_selection():
    normalized, legacy = c._normalize_citylearn_configs(
        {
            "buildings": ["B1", "B2"],
            "signals": ["load", "pv"],
            "weather": True,
        }
    )

    assert legacy is True
    assert normalized["selected_buildings"] == ["B1", "B2"]
    assert "buildings" not in normalized["schema_overrides"]
    assert normalized["schema_overrides"]["signals"] == ["load", "pv"]


def test_ev_policy_dict_by_charger_is_direct_mapping():
    index = pd.date_range(start="2026-01-01T01:00:00Z", periods=2, freq="60min")
    sessions = pd.DataFrame(
        {
            "charging_sessions": [
                {
                    "C1": {"electric_vehicle": "EV_A", "power": 7.2},
                    "C2": {"electric_vehicle": "", "power": 0.0},
                },
                {
                    "C1": {"electric_vehicle": "EV_B", "power": 3.0},
                    "C2": {"electric_vehicle": "", "power": 0.0},
                },
            ],
            "electric_vehicles": [{}, {}],
        },
        index=index,
    )

    warnings: list[str] = []
    frames, ev_ids, disabled = c._build_charger_rows(
        building_id="B1",
        charger_ids=["C1", "C2"],
        resampled_sessions=sessions,
        period_minutes=60,
        defaults=c.DEFAULTS_TEMPLATE,
        warnings=warnings,
    )

    assert disabled is False
    assert set(frames.keys()) == {"C1", "C2"}
    assert ev_ids == {"EV_A", "EV_B"}
    assert frames["C1"].iloc[0]["electric_vehicle_id"] == "EV_A"


def test_ev_policy_list_with_single_charger_maps_to_that_charger():
    index = pd.date_range(start="2026-01-01T01:00:00Z", periods=1, freq="60min")
    sessions = pd.DataFrame(
        {
            "charging_sessions": [
                [
                    {"charger_id": "X", "user_id": "EV_X", "power": 4.5},
                    {"charger_id": "Y", "user_id": "", "power": 0.0},
                ]
            ],
            "electric_vehicles": [{}],
        },
        index=index,
    )

    warnings: list[str] = []
    frames, ev_ids, disabled = c._build_charger_rows(
        building_id="B1",
        charger_ids=["C_ONLY"],
        resampled_sessions=sessions,
        period_minutes=60,
        defaults=c.DEFAULTS_TEMPLATE,
        warnings=warnings,
    )

    assert disabled is False
    assert set(frames.keys()) == {"C_ONLY"}
    assert frames["C_ONLY"].iloc[0]["electric_vehicle_id"] == "EV_X"
    assert "EV_X" in ev_ids


def test_ev_policy_list_with_multiple_chargers_disables_ev_for_building():
    index = pd.date_range(start="2026-01-01T01:00:00Z", periods=1, freq="60min")
    sessions = pd.DataFrame(
        {
            "charging_sessions": [[{"charger_id": "C1", "user_id": "EV_1", "power": 5.0}]],
            "electric_vehicles": [{}],
        },
        index=index,
    )

    warnings: list[str] = []
    frames, ev_ids, disabled = c._build_charger_rows(
        building_id="B_ambiguous",
        charger_ids=["C1", "C2"],
        resampled_sessions=sessions,
        period_minutes=60,
        defaults=c.DEFAULTS_TEMPLATE,
        warnings=warnings,
    )

    assert disabled is True
    assert frames == {}
    assert ev_ids == set()
    assert any("B_ambiguous" in warning for warning in warnings)


class _FakeCollection:
    def __init__(self, docs):
        self._docs = docs

    def find_one(self, *_args, **_kwargs):
        return self._docs[0] if self._docs else None


class _FakeDB(dict):
    def list_collection_names(self):
        return list(self.keys())


def test_list_citylearn_compatible_sites_filters_missing_schema(monkeypatch):
    db_with_schema = _FakeDB(
        {
            "schema": _FakeCollection([
                {"schema": {"buildings": {"B1": {}, "B2": {}}}}
            ]),
            "building_B1": _FakeCollection([]),
        }
    )
    db_without_schema = _FakeDB({"building_X": _FakeCollection([])})

    def fake_get_db(name: str):
        if name == "site_ok":
            return db_with_schema
        return db_without_schema

    monkeypatch.setattr(c.mongo_utils, "list_databases", lambda: ["admin", "site_ok", "site_bad"])
    monkeypatch.setattr(c.mongo_utils, "get_db", fake_get_db)

    payload = c.list_citylearn_compatible_sites()
    assert payload == [{"site_id": "site_ok", "buildings": ["B1"]}]
