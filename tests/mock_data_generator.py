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

# A set of representative datetimes that span a wide range so that
# start-date and bookmark tests can distinguish sync results:
#   - 2021-06-01: safely before any realistic start_date_2
#   - 2024-01-15: between start_date_1 (2021-01-01) and start_date_2 (2024-01-20)
#   - 2024-02-01: after all test start dates
_MOCK_DATETIMES = [
    "2021-06-01T10:00:00.000000Z",
    "2024-01-15T10:00:00.000000Z",
    "2024-02-01T10:00:00.000000Z",
]
# Keep this alias so any code that imports _DEFAULT_DATETIME still works.
_DEFAULT_DATETIME = _MOCK_DATETIMES[0]

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


def _generate_value(schema: dict, field_name: str = "", record_index: int = 0) -> Any:
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
        return record_index + 1  # gives ids 1, 2, 3 per record

    if concrete == "number":
        return float(record_index + 1)

    if concrete == "boolean":
        return False

    if concrete == "string":
        if fmt == "date-time" or "date" in fname_lower:
            return _MOCK_DATETIMES[record_index % len(_MOCK_DATETIMES)]
        if fmt == "uri" or any(k in fname_lower for k in ("url", "link")):
            return "https://example.com/mock"
        if fmt == "email" or "email" in fname_lower:
            return "mock@example.com"
        return f"mock_{field_name}_{record_index}" if field_name else f"mock_value_{record_index}"

    if concrete == "object":
        props: dict = schema.get("properties", {})
        if not props:
            return {}
        return {k: _generate_value(v, k, record_index) for k, v in props.items()}

    if concrete == "array":
        items_schema: dict = schema.get("items", {})
        return [_generate_value(items_schema, field_name.rstrip("s"), record_index)]

    return None


def _generate_record(schema: dict, record_index: int = 0) -> dict:
    """Return one record whose shape matches a top-level ``object`` schema."""
    props: dict = schema.get("properties", {})
    return {k: _generate_value(v, k, record_index) for k, v in props.items()}


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
        records = [_generate_record(schema, i) for i in range(3)] if schema else [{}] * 3
        fixtures[path_key] = {"nextToken": None, "results": records}

    return fixtures


FIXTURES: dict[str, dict] = _build_fixtures()
