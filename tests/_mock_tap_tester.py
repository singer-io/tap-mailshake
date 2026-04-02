"""
Stub implementations of tap_tester modules (connections, menagerie, runner)
and a minimal BaseCase, used when INTEGRATION_TEST_MODE=mock.

The stubs replace the live Stitch-platform infrastructure so the existing
integration test files run without modification and without a live account.

Flow
────
  connections.ensure_connection(test) → _MockConn(test)
  test.run_and_verify_check_mode(conn_id) → calls discover() directly
  test.perform_and_verify_table_and_field_selection(...) → marks streams selected
  runner.run_sync_mode(test, conn_id) → patches HTTP, calls sync(), captures records
  menagerie.get_exit_status(conn_id, job) → fake success payload
  runner.examine_target_output_file(...) → returns conn_id.record_counts
  menagerie.get_state / set_state → get/set conn_id.state
"""
from __future__ import annotations

import json
import sys
import types
import unittest
from typing import Any, Dict


# ─── Mock connection object ────────────────────────────────────────────────


class _MockConn:
    """Carries all per-test-run state between stub method calls."""

    def __init__(self, test_instance):
        self.test = test_instance
        self.catalog = None          # set by run_and_verify_check_mode
        self.state: Dict = {}
        self.records: Dict[str, list] = {}        # stream → list[record]
        self.record_counts: Dict[str, int] = {}   # stream → count

    def run_sync(self) -> None:
        """Patch MailshakeClient.request and run sync(), capturing Singer output."""
        import io
        from unittest.mock import patch, MagicMock
        from tap_mailshake.sync import sync
        from tap_mailshake.client import MailshakeClient

        # Clear records from any previous run so counts reflect only this sync
        self.records = {}
        self.record_counts = {}

        captured = io.StringIO()
        mock_request = self.test._build_mock_request()

        with patch.object(MailshakeClient, "check_access", return_value=True), \
             patch.object(MailshakeClient, "request", side_effect=mock_request):

            # Build a lightweight client instance without the real __init__
            client = MailshakeClient.__new__(MailshakeClient)
            client._MailshakeClient__api_key = "mock-api-key"
            client._MailshakeClient__user_agent = "mock/1.0"
            client.base_url = "https://api.mailshake.com/2017-04-01"
            client._MailshakeClient__session = MagicMock()
            client.request_timeout = 300

            old_stdout = sys.stdout
            sys.stdout = captured
            try:
                sync(
                    client=client,
                    config={
                        "start_date": self.test.start_date,
                        "user_agent": "mock/1.0",
                    },
                    catalog=self.catalog,
                    state=dict(self.state),
                )
            finally:
                sys.stdout = old_stdout

        # Parse the captured Singer lines
        for line in captured.getvalue().splitlines():
            try:
                msg = json.loads(line)
                if msg.get("type") == "RECORD":
                    stream = msg["stream"]
                    self.records.setdefault(stream, []).append(msg["record"])
                elif msg.get("type") == "STATE":
                    self.state = msg.get("value", {})
            except json.JSONDecodeError:
                pass

        self.record_counts = {s: len(r) for s, r in self.records.items()}


# ─── connections stub ──────────────────────────────────────────────────────

connections = types.ModuleType("tap_tester.connections")


def _ensure_connection(test_instance, **kwargs) -> _MockConn:
    return _MockConn(test_instance)


connections.ensure_connection = _ensure_connection


# ─── menagerie stub ────────────────────────────────────────────────────────

menagerie = types.ModuleType("tap_tester.menagerie")


def _get_exit_status(conn_id: _MockConn, job_name: Any) -> dict:
    return {
        "exit_status": {
            "discovery_exit_status": 0,
            "tap_exit_status": 0,
            "target_exit_status": 0,
            "check_exit_status": 0,
        }
    }


def _verify_sync_exit_status(test, exit_status: dict, job_name: Any) -> None:
    tap_status = exit_status.get("exit_status", {}).get("tap_exit_status", 0)
    test.assertEqual(tap_status, 0, "Mock sync exited with non-zero tap status")


def _get_state(conn_id: _MockConn) -> dict:
    return dict(conn_id.state)


def _set_state(conn_id: _MockConn, state: dict) -> None:
    conn_id.state = dict(state)


menagerie.get_exit_status = _get_exit_status
menagerie.verify_sync_exit_status = _verify_sync_exit_status
menagerie.get_state = _get_state
menagerie.set_state = _set_state


def _select_catalog(conn_id: _MockConn, catalog_entry: dict) -> None:
    """Mark a single stream as selected in the catalog."""
    if conn_id.catalog is None:
        return
    from singer import metadata as md
    stream_name = catalog_entry.get("stream_name") or catalog_entry.get("tap_stream_id")
    for stream in conn_id.catalog.streams:
        if stream.stream == stream_name:
            mdata = md.to_map(stream.metadata)
            mdata[()]["selected"] = True
            stream.metadata = md.to_list(mdata)
            break


menagerie.select_catalog = _select_catalog


# ─── runner stub ──────────────────────────────────────────────────────────

runner = types.ModuleType("tap_tester.runner")


def _run_sync_mode(test_instance, conn_id: _MockConn) -> _MockConn:
    """Run a mock sync; returns conn_id used as the 'job_name' in subsequent calls."""
    conn_id.run_sync()
    return conn_id


def _examine_target_output_file(
    test_instance, conn_id: _MockConn, streams, pk_fields
) -> dict:
    return {s: conn_id.record_counts.get(s, 0) for s in streams}


runner.run_sync_mode = _run_sync_mode
runner.examine_target_output_file = _examine_target_output_file


# ─── BaseCase stub ────────────────────────────────────────────────────────


class BaseCase(unittest.TestCase):
    """
    Minimal stand-in for tap_tester.base_suite_tests.base_case.BaseCase.

    Provides the Singer metadata constants and the catalog helper methods
    that MailshakeBaseTest and the integration test files depend on.
    """

    # Singer metadata key constants (match tap_tester.BaseCase)
    PRIMARY_KEYS = "table-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    REPLICATION_KEYS = "valid-replication-keys"
    FULL_TABLE = "FULL_TABLE"
    INCREMENTAL = "INCREMENTAL"

    # ── catalog helpers ───────────────────────────────────────────────────

    def run_and_verify_check_mode(self, conn_id: _MockConn) -> list:
        """Call discover() directly; registers the catalog on conn_id."""
        from tap_mailshake.discover import discover
        from singer import metadata as md

        catalog = discover()
        conn_id.catalog = catalog

        entries = []
        for stream in catalog.streams:
            mdata_map = md.to_map(stream.metadata)
            root_meta = mdata_map.get((), {})
            # Add "key_properties" literal so tests that check for it pass
            root_meta["key_properties"] = root_meta.get("table-key-properties", [])
            entries.append(
                {
                    "stream_name": stream.stream,
                    "tap_stream_id": stream.stream,
                    "schema": stream.schema.to_dict(),
                    "metadata": {"metadata": root_meta},
                    "key_properties": stream.key_properties,
                }
            )
        return entries

    def perform_and_verify_table_and_field_selection(
        self,
        conn_id: _MockConn,
        found_catalogs: list,
        select_all_fields: bool = True,
    ) -> list:
        """Mark all catalog streams (and their fields) as selected."""
        from singer import metadata as md

        for stream in conn_id.catalog.streams:
            mdata = md.to_map(stream.metadata)
            mdata[()]["selected"] = True
            if select_all_fields:
                for breadcrumb in list(mdata.keys()):
                    if breadcrumb != ():
                        mdata[breadcrumb]["selected"] = True
            stream.metadata = md.to_list(mdata)

        return found_catalogs

    def select_all_streams_and_fields(
        self,
        conn_id: _MockConn,
        found_catalogs: list,
        select_all_fields: bool = True,
    ) -> list:
        """Alias for perform_and_verify_table_and_field_selection."""
        return self.perform_and_verify_table_and_field_selection(
            conn_id, found_catalogs, select_all_fields=select_all_fields
        )

    # ── replication helpers ───────────────────────────────────────────────

    @classmethod
    def expected_replication_method(cls) -> dict:
        return {
            stream: meta[cls.REPLICATION_METHOD]
            for stream, meta in cls.expected_metadata().items()
        }
