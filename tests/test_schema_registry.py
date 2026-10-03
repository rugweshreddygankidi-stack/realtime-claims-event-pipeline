import json

import pytest

from claims import schema_registry as sr


class Recorder:
    def __init__(self, responses):
        self.calls, self.responses = [], list(responses)

    def __call__(self, method, url, body=None):
        self.calls.append((method, url, body))
        reply = self.responses.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def test_set_compatibility_calls_config_endpoint(monkeypatch):
    rec = Recorder([{"compatibility": "BACKWARD"}])
    monkeypatch.setattr(sr, "_request", rec)
    sr.set_compatibility("http://sr", "subj", "BACKWARD")
    assert rec.calls == [("PUT", "http://sr/config/subj", {"compatibility": "BACKWARD"})]


def test_register_returns_schema_id(monkeypatch):
    rec = Recorder([{"id": 7}])
    monkeypatch.setattr(sr, "_request", rec)
    assert sr.register("http://sr", "subj", '{"type":"string"}') == 7
    assert rec.calls[0][:2] == ("POST", "http://sr/subjects/subj/versions")
    assert rec.calls[0][2] == {"schema": '{"type":"string"}'}


def test_register_surfaces_incompatible_schema_error(monkeypatch):
    monkeypatch.setattr(sr, "_request", Recorder([sr.RegistryError(409, "incompatible")]))
    with pytest.raises(sr.RegistryError) as exc:
        sr.register("http://sr", "subj", "{}")
    assert exc.value.status == 409


@pytest.mark.parametrize("reply,expected", [({"is_compatible": True}, True), ({"is_compatible": False}, False), ({}, False)])
def test_is_compatible(monkeypatch, reply, expected):
    monkeypatch.setattr(sr, "_request", Recorder([reply]))
    assert sr.is_compatible("http://sr", "subj", "{}") is expected


def test_get_latest_returns_only_needed_fields(monkeypatch):
    monkeypatch.setattr(sr, "_request", Recorder([{"id": 3, "version": 2, "schema": "S", "subject": "x"}]))
    assert sr.get_latest("http://sr", "subj") == {"id": 3, "version": 2, "schema": "S"}


def test_registry_error_message_contains_status():
    assert "409" in str(sr.RegistryError(409, "nope"))


def test_shipped_schemas_are_valid_json_with_expected_fields():
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "schemas"
    base = json.loads((root / "claim_event.avsc").read_text())
    names = [f["name"] for f in base["fields"]]
    assert "claim_event_id" in names and "claim_amount" in names
    v2 = json.loads((root / "claim_event_v2_compatible.avsc").read_text())
    added = [f for f in v2["fields"] if f["name"] not in names]
    assert added and all("default" in f for f in added)  # backward-compatible: new fields have defaults
    breaking = json.loads((root / "claim_event_v2_breaking.avsc").read_text())
    amount = next(f for f in breaking["fields"] if f["name"] == "claim_amount")
    assert amount["type"] == "string"
