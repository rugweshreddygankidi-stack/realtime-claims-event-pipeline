"""Tiny Confluent Schema Registry client (standard library only) + CLI.

    python -m claims.schema_registry register schemas/claim_event.avsc
    python -m claims.schema_registry check    schemas/claim_event_v2_breaking.avsc
    python -m claims.schema_registry latest
"""
from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

SUBJECT = os.environ.get("SCHEMA_SUBJECT", "claims.events-value")
DEFAULT_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


class RegistryError(RuntimeError):
    def __init__(self, status: int, body: str):
        super().__init__(f"schema registry returned HTTP {status}: {body}")
        self.status = status
        self.body = body


def _request(method: str, url: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": CONTENT_TYPE})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as exc:
        raise RegistryError(exc.code, exc.read().decode()) from exc


def set_compatibility(base_url: str, subject: str, level: str = "BACKWARD") -> dict:
    return _request("PUT", f"{base_url}/config/{subject}", {"compatibility": level})


def register(base_url: str, subject: str, schema_str: str) -> int:
    """Register a schema; raises RegistryError (HTTP 409) if it breaks the compatibility rule."""
    return _request("POST", f"{base_url}/subjects/{subject}/versions", {"schema": schema_str})["id"]


def is_compatible(base_url: str, subject: str, schema_str: str) -> bool:
    result = _request("POST", f"{base_url}/compatibility/subjects/{subject}/versions/latest",
                      {"schema": schema_str})
    return bool(result.get("is_compatible"))


def get_latest(base_url: str, subject: str) -> dict:
    """Return {'id': int, 'version': int, 'schema': str}."""
    result = _request("GET", f"{base_url}/subjects/{subject}/versions/latest")
    return {"id": result["id"], "version": result["version"], "schema": result["schema"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["register", "check", "latest"])
    parser.add_argument("schema_file", nargs="?")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--subject", default=SUBJECT)
    parser.add_argument("--compatibility", default="BACKWARD")
    args = parser.parse_args()

    if args.command == "latest":
        info = get_latest(args.url, args.subject)
        print(json.dumps({"id": info["id"], "version": info["version"]}))
        return

    if not args.schema_file:
        parser.error("schema_file is required")
    schema_str = Path(args.schema_file).read_text()

    if args.command == "register":
        set_compatibility(args.url, args.subject, args.compatibility)
        try:
            print(f"registered: schema id {register(args.url, args.subject, schema_str)} "
                  f"(compatibility={args.compatibility})")
        except RegistryError as exc:
            print(f"REJECTED by schema registry: {exc.body}")
            raise SystemExit(1)
    else:
        print("compatible" if is_compatible(args.url, args.subject, schema_str) else "INCOMPATIBLE")


if __name__ == "__main__":
    main()
