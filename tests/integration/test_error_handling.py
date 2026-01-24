"""Integration tests for error handling and database diagnostics.

These tests validate that ParadeDB errors are properly propagated through
Django's ORM and that error messages are meaningful to developers.
"""

from __future__ import annotations

import pytest
from django.db import connection, transaction
from django.db.utils import DatabaseError
from tests.models import MockItem

from paradedb.search import MoreLikeThis, ParadeDB, Parse, Regex

pytestmark = [
    pytest.mark.integration,
    pytest.mark.django_db(transaction=True),
    pytest.mark.usefixtures("mock_items"),
]


class TestParseQueryErrors:
    """Test error handling for pdb.parse() query syntax errors."""

    def test_invalid_parse_syntax_raises_database_error(self) -> None:
        """Invalid parse syntax raises DatabaseError with helpful message."""
        with pytest.raises(DatabaseError) as exc_info:
            list(
                MockItem.objects.filter(description=ParadeDB(Parse("AND AND invalid")))
            )
        error_msg = str(exc_info.value).lower()
        assert "could not parse query string" in error_msg
        assert "and and invalid" in error_msg

    def test_parse_with_unclosed_quotes(self) -> None:
        """Unclosed quotes in parse query raise clear error."""
        with pytest.raises(DatabaseError) as exc_info:
            list(
                MockItem.objects.filter(description=ParadeDB(Parse('"unclosed quote')))
            )
        error_msg = str(exc_info.value).lower()
        assert "could not parse" in error_msg


class TestRegexQueryErrors:
    """Test error handling for pdb.regex() pattern errors."""

    def test_invalid_regex_pattern_raises_error(self) -> None:
        """Invalid regex pattern raises database error with pattern info."""
        with pytest.raises(DatabaseError) as exc_info:
            list(MockItem.objects.filter(description=ParadeDB(Regex("[invalid(regex"))))
        error_msg = str(exc_info.value).lower()
        assert "regex" in error_msg
        assert "unclosed character class" in error_msg or "invalid" in error_msg


class TestFieldErrors:
    """Test error handling for field-related issues."""

    def test_search_on_indexed_field_works(self) -> None:
        """Searching on an indexed field returns results."""
        queryset = MockItem.objects.filter(description=ParadeDB("shoes"))
        assert queryset.exists()

    def test_more_like_this_nonexistent_id_returns_empty(self) -> None:
        """MoreLikeThis with non-existent ID returns empty result, not error."""
        queryset = MockItem.objects.filter(
            MoreLikeThis(product_id=999999, fields=["description"])
        )
        assert queryset.count() == 0


class TestConnectionErrors:
    """Test database connection error handling."""

    def test_raw_sql_error_propagates_clearly(self) -> None:
        """Raw SQL errors from ParadeDB are propagated with context."""
        with (
            pytest.raises(DatabaseError) as exc_info,
            connection.cursor() as cursor,
        ):
            cursor.execute(
                "SELECT * FROM mock_items WHERE id @@@ pdb.parse('AND AND');"
            )
        error_msg = str(exc_info.value).lower()
        assert "could not parse" in error_msg


class TestNoticeHandling:
    """Test PostgreSQL NOTICE message handling.

    Django/psycopg collects notices. In psycopg3, use add_notice_handler.
    NOTICEs don't raise exceptions but can be inspected.
    """

    def test_notices_do_not_raise_exceptions(self) -> None:
        """NOTICE messages (like 'index exists') don't interrupt execution."""
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS mock_items_bm25_idx ON mock_items "
                "USING bm25 (id, description) WITH (key_field='id');"
            )
            cursor.execute("SELECT COUNT(*) FROM mock_items;")
            (count,) = cursor.fetchone()
        assert count > 0


class TestTransactionErrorRecovery:
    """Test error recovery within transactions."""

    def test_error_recovery_with_atomic_block(self) -> None:
        """Errors inside atomic block can be caught and recovered."""
        try:
            with transaction.atomic():
                list(
                    MockItem.objects.filter(
                        description=ParadeDB(Parse("AND AND invalid"))
                    )
                )
        except DatabaseError:
            pass

        assert MockItem.objects.filter(description=ParadeDB("shoes")).exists()

    def test_savepoint_rollback_on_error(self) -> None:
        """Savepoints allow partial rollback after search errors."""
        initial_count = MockItem.objects.count()

        try:
            with transaction.atomic():  # noqa: SIM117 - nested for savepoint test
                with transaction.atomic():
                    list(
                        MockItem.objects.filter(
                            description=ParadeDB(Parse("BAD QUERY SYNTAX"))
                        )
                    )
        except DatabaseError:
            pass

        assert MockItem.objects.count() == initial_count


class TestErrorMessageQuality:
    """Test that error messages are developer-friendly."""

    def test_parse_error_includes_query_string(self) -> None:
        """Parse errors include the problematic query string."""
        bad_query = "field:value AND AND broken"
        with pytest.raises(DatabaseError) as exc_info:
            list(MockItem.objects.filter(description=ParadeDB(Parse(bad_query))))
        error_msg = str(exc_info.value)
        assert bad_query in error_msg

    def test_error_message_includes_guidance(self) -> None:
        """ParadeDB errors include helpful guidance."""
        with pytest.raises(DatabaseError) as exc_info:
            list(MockItem.objects.filter(description=ParadeDB(Parse("OR OR"))))
        error_msg = str(exc_info.value).lower()
        assert "column:term" in error_msg or "capitalize" in error_msg

    def test_regex_error_shows_pattern_location(self) -> None:
        """Regex errors show where the pattern is invalid."""
        with pytest.raises(DatabaseError) as exc_info:
            list(MockItem.objects.filter(description=ParadeDB(Regex("(unclosed"))))
        error_msg = str(exc_info.value)
        assert "unclosed" in error_msg.lower()
