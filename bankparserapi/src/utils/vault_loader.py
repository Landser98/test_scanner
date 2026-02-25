"""
Vault configuration loader.

Loads key/value configuration from HashiCorp Vault KV v2 once at startup
and injects values into process environment variables.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Dict

import requests

_LOCK = threading.Lock()
_LOADED = False


def _to_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_addr(addr: str) -> str:
    return addr.rstrip("/")


def _safe_set_env(data: Dict[str, Any], overwrite: bool) -> None:
    for key, value in data.items():
        if not isinstance(key, str) or not key:
            continue
        if value is None:
            continue
        if not overwrite and key in os.environ:
            continue
        os.environ[key] = str(value)


def _fetch_vault_payload() -> Dict[str, Any]:
    addr = os.getenv("VAULT_ADDR", "").strip()
    token = os.getenv("VAULT_TOKEN", "").strip()
    mount = os.getenv("VAULT_KV_MOUNT", "").strip() or "secret"
    path = os.getenv("VAULT_CONFIG_PATH", "").strip()
    config_key = os.getenv("VAULT_CONFIG_KEY", "").strip()
    timeout = float(os.getenv("VAULT_TIMEOUT_SECONDS", "10").strip())
    skip_verify = _to_bool(os.getenv("VAULT_SKIP_VERIFY"), default=False)

    if not addr or not token or not path:
        raise ValueError("VAULT_ADDR, VAULT_TOKEN, and VAULT_CONFIG_PATH must be set")

    url = f"{_normalize_addr(addr)}/v1/{mount}/data/{path.lstrip('/')}"
    response = requests.get(
        url,
        headers={"X-Vault-Token": token},
        timeout=timeout,
        verify=not skip_verify,
    )
    response.raise_for_status()
    body = response.json()

    raw_data = body.get("data", {}).get("data")
    if not isinstance(raw_data, dict):
        raise ValueError("Vault response does not contain KV v2 data payload")

    if config_key:
        if config_key not in raw_data:
            raise KeyError(f"VAULT_CONFIG_KEY '{config_key}' not found in secret")
        selected = raw_data[config_key]
    else:
        selected = raw_data

    if isinstance(selected, str):
        selected = json.loads(selected)

    if not isinstance(selected, dict):
        raise ValueError("Vault config payload must be a JSON object (dict)")

    return selected


def load_vault_config_once() -> None:
    """
    Load config from Vault once per process.

    Behavior:
    - Disabled unless VAULT_ENABLED=true OR required VAULT vars are present
    - Existing env vars are preserved unless VAULT_OVERWRITE=true
    - If VAULT_REQUIRED=true, startup fails on any load error
    """
    global _LOADED

    if _LOADED:
        return

    with _LOCK:
        if _LOADED:
            return

        required = _to_bool(os.getenv("VAULT_REQUIRED"), default=False)
        enabled = _to_bool(os.getenv("VAULT_ENABLED"), default=False)
        has_minimum = bool(os.getenv("VAULT_ADDR") and os.getenv("VAULT_TOKEN") and os.getenv("VAULT_CONFIG_PATH"))

        if not enabled and not has_minimum:
            _LOADED = True
            return

        overwrite = _to_bool(os.getenv("VAULT_OVERWRITE"), default=False)

        try:
            payload = _fetch_vault_payload()
            _safe_set_env(payload, overwrite=overwrite)
        except Exception:
            if required:
                raise
        finally:
            _LOADED = True
