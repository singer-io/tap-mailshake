from base import MailshakeBaseTest
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest as TT_BookmarkTest


class BookmarkTest(TT_BookmarkTest, MailshakeBaseTest):
    """Verify incremental streams save bookmarks and respect them on subsequent syncs."""

    # Bookmark datetime format used by this tap for incremental streams.
    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    initial_bookmarks = {
        "bookmarks": {
            "leads": "2021-01-01T00:00:00.000000Z",
            "senders": "2021-01-01T00:00:00.000000Z",
            "sent_messages": "2021-01-01T00:00:00.000000Z",
            "opens": "2021-01-01T00:00:00.000000Z",
            "clicks": "2021-01-01T00:00:00.000000Z",
            "replies": "2021-01-01T00:00:00.000000Z",
        }
    }

    @staticmethod
    def name():
        return "tap_mailshake_bookmark_test"

    def streams_to_test(self):
        # Exclude campaigns and team_members (no replication keys) and recipients (per-campaign child stream, not synced without campaigns).
        streams_to_exclude = set({
            "campaigns",
            "team_members",
            "recipients"
        })
        return self.expected_stream_names().difference(streams_to_exclude)

    def calculate_new_bookmarks(self):
        """Reset bookmark to before all mock records so sync 2 returns all records."""
        new_bookmarks = {}
        for stream in self.streams_to_test():
            replication_keys = self.expected_replication_keys(stream)
            assert len(replication_keys) == 1
            rep_key = next(iter(replication_keys))
            new_bookmarks[self.get_stream_id(stream)] = {
                rep_key: "2024-01-05T00:00:00.000000Z"
            }
        return new_bookmarks

    @staticmethod
    def manipulate_state(state: dict, new_bookmarks: dict) -> dict:
        """Override for tap-mailshake's flat bookmark format.

        tap-mailshake stores state as::

            {"bookmarks": {"leads": "2024-02-01T10:00:00.000000Z"}}

        instead of the standard nested dict format other taps use.
        This override extracts the value from the ``{rep_key: value}`` dict
        that ``calculate_new_bookmarks`` returns and stores it as a flat string.
        """
        from copy import deepcopy
        new_state = deepcopy(state)
        if "bookmarks" not in new_state:
            new_state["bookmarks"] = {}
        for stream, bmark in new_bookmarks.items():
            if isinstance(bmark, dict):
                # extract the datetime value from {replication_key: value}
                bmark = next(iter(bmark.values()))
            new_state["bookmarks"][stream] = bmark
        return new_state
