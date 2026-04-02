"""Dynamic mock-data generator for tap-mailshake tests.

At import time (i.e. the moment the test session first touches mock mode) this
module reads every JSON Schema from ``tap_mailshake/schemas/``, synthesises
one representative record per stream, and caches the full API envelope
``{"nextToken": null, "results": [<record>]}`` in the module-level dict
``FIXTURES``.

``mock_base.py`` reads directly from ``FIXTURES``; no fixture files are
read from disk during the test run.

Value-generation rules
──────────────────────
* ``integer``         → ``1``
* ``number``          → ``1.0``
* ``boolean``         → ``False``
* ``string``

  * ``format: date-time`` *or* field name contains "date" → ISO-8601 datetime
  * ``format: uri``    *or* field name contains "url" / "link" → example URL
  * field name contains "email" → example e-mail address
  * anything else → ``"mock_<field_name>"``

* ``object``          → recurse over ``properties``; free-form → ``{}``
* ``array``           → single-element list built from ``items`` schema
* ``null``-only       → ``None``

The same schema always produces the same value, which keeps test assertions
deterministic.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


_SCHEMA_DIR = Path(__file__).parent.parent / "tap_mailshake" / "schemas"

# A representative datetime safely *after* the default start_date
# (2024-01-01T00:00:00Z) so incremental-replication bookmark tests pass.
_DEFAULT_DATETIME = "2024-02-01T10:00:00.000Z"

# ---------------------------------------------------------------------------
# API-path  →  schema-file mapping
# ---------------------------------------------------------------------------
# Paths that lack a dedicated schema reuse the closest structural equivalent.

_PATH_TO_SCHEMA: dict[str, str] = {
    "campaigns/list":               "campaigns",
    "recipients/list":              "recipients",
    "leads/list":                   "leads",
    "senders/list":                 "senders",
    "team/list-members":            "team_members",
    "activity/sent":                "sent_messages",
    "activity/opens":               "opens",
    "activity/clicks":              "clicks",
    "activity/replies":             "replies",
}


def _pick_concrete_type(types: list[str]) -> str:
    """Return the first non-null JSON Schema type from a mixed list."""
    for t in types:
        if t != "null":
            return t
    return "null"


def _generate_value(schema: dict, field_name: str = "") -> Any:
    """Recursively synthesise one value that satisfies *schema*.

    The result is fully deterministic: the same schema + field_name always
    produces the same value.
    """
    if not schema:
        return None

    raw_type = schema.get("type", "string")
    types: list[str] = raw_type if isinstance(raw_type, list) else [raw_type]
    concrete = _pick_concrete_type(types)
    fmt: str = schema.get("format", "")
    fname_lower = field_name.lower()

    if concrete == "null":
        return None

    if concrete == "integer":
        return 1

    if concrete == "number":
        return 1.0

    if concrete == "boolean":
        return False

    if concrete == "string":
        if fmt == "date-time" or "date" in fname_lower:
            return _DEFAULT_DATETIME
        if fmt == "uri" or any(k in fname_lower for k in ("url", "link")):
            return "https://example.com/mock"
        if fmt == "email" or "email" in fname_lower:
            return "mock@example.com"
        return f"mock_{field_name}" if field_name else "mock_value"

    if concrete == "object":
        props: dict = schema.get("properties", {})
        if not props:
            # Free-form object (e.g. "fields" with additionalProperties: true)
            return {}
        return {k: _generate_value(v, k) for k, v in props.items()}

    if concrete == "array":
        items_schema: dict = schema.get("items", {})
        return [_generate_value(items_schema, field_name.rstrip("s"))]

    # Unrecognised type — return None to stay safe
    return None


def _generate_record(schema: dict) -> dict:
    """Return one record whose shape matches a top-level ``object`` schema."""
    props: dict = schema.get("properties", {})
    return {k: _generate_value(v, k) for k, v in props.items()}


def _load_schema(stream_name: str) -> dict:
    """Load and return the JSON Schema for *stream_name*, or ``{}`` if absent."""
    path = _SCHEMA_DIR / f"{stream_name}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {}


def _build_fixtures() -> dict[str, dict]:
    """Build and return the complete in-memory fixture map.

    Returns
    -------
    dict
        ``{ api_path_key: {"nextToken": None, "results": [<record>]} }``
    """
    fixtures: dict[str, dict] = {}

    for path_key, stream_name in _PATH_TO_SCHEMA.items():
        schema = _load_schema(stream_name)
        record = _generate_record(schema) if schema else {}
        fixtures[path_key] = {"nextToken": None, "results": [record]}

    return fixtures


FIXTURES: dict[str, dict] = _build_fixtures()
