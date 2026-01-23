"""Tests for SQL generation.

This plugin tests SQL string generation only - no database required.
Uses syrupy for snapshot testing. Run `pytest --snapshot-update` to update.
"""

from django.db.models import F, Q, Window
from django.db.models.functions import RowNumber

from paradedb.functions import Score, Snippet
from paradedb.indexes import BM25Index
from paradedb.search import (
    PQ,
    Fuzzy,
    MoreLikeThis,
    ParadeDB,
    Parse,
    Phrase,
    Regex,
    Term,
)
from tests.models import Product


class DummySchemaEditor:
    """Minimal schema editor for SQL string generation."""

    @staticmethod
    def quote_name(name: str) -> str:
        return f'"{name}"'


class TestParadeDBLookup:
    """Test ParadeDB lookup SQL generation."""

    def test_single_term_search(self, snapshot) -> None:
        """Single term generates: WHERE description &&& 'shoes'."""
        queryset = Product.objects.filter(description=ParadeDB("shoes"))
        assert str(queryset.query) == snapshot

    def test_and_search_multiple_terms(self, snapshot) -> None:
        """Multiple terms generate: WHERE description &&& ARRAY[...]"""
        queryset = Product.objects.filter(description=ParadeDB("running", "shoes"))
        assert str(queryset.query) == snapshot

    def test_and_search_three_terms(self, snapshot) -> None:
        """Edge case: three terms for AND search."""
        queryset = Product.objects.filter(
            description=ParadeDB("running", "shoes", "lightweight")
        )
        assert str(queryset.query) == snapshot


class TestPQObject:
    """Test PQ SQL generation."""

    def test_or_search(self, snapshot) -> None:
        """PQ OR generates: WHERE description ||| ARRAY[...]"""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth"))
        )
        assert str(queryset.query) == snapshot

    def test_or_search_chained(self, snapshot) -> None:
        """Edge case: chaining multiple OR terms."""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth") | PQ("speaker"))
        )
        assert str(queryset.query) == snapshot

    def test_pq_and_search(self, snapshot) -> None:
        """PQ AND generates: WHERE description &&& ARRAY[...]"""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("shoes") & PQ("sandals"))
        )
        assert str(queryset.query) == snapshot


class TestPhraseSearch:
    """Test Phrase search SQL generation."""

    def test_phrase_search(self, snapshot) -> None:
        """Phrase generates: WHERE description ### 'wireless bluetooth'."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("wireless bluetooth"))
        )
        assert str(queryset.query) == snapshot

    def test_phrase_with_slop(self, snapshot) -> None:
        """Phrase with slop: WHERE description ### 'running shoes'::pdb.slop(1)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Phrase("running shoes", slop=1))
        )
        assert str(queryset.query) == snapshot


class TestFuzzySearch:
    """Test Fuzzy search SQL generation."""

    def test_fuzzy_search(self, snapshot) -> None:
        """Fuzzy generates: WHERE description ||| 'sheos'::pdb.fuzzy(1)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Fuzzy("sheos", distance=1))
        )
        assert str(queryset.query) == snapshot

    def test_multiple_fuzzy_terms(self, snapshot) -> None:
        """Multiple fuzzy terms generate OR array."""
        queryset = Product.objects.filter(
            description=ParadeDB(
                Fuzzy("runnning", distance=1), Fuzzy("shoez", distance=1)
            )
        )
        assert str(queryset.query) == snapshot


class TestParseQuery:
    """Test Parse query SQL generation."""

    def test_parse_query(self, snapshot) -> None:
        """Parse generates: WHERE description @@@ pdb.parse(..., lenient => true)."""
        queryset = Product.objects.filter(
            description=ParadeDB(Parse("running AND shoes", lenient=True))
        )
        assert str(queryset.query) == snapshot


class TestTermQuery:
    """Test Term query SQL generation."""

    def test_term_query(self, snapshot) -> None:
        """Term generates: WHERE description @@@ pdb.term('shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Term("shoes")))
        assert str(queryset.query) == snapshot


class TestRegexQuery:
    """Test Regex query SQL generation."""

    def test_regex_query(self, snapshot) -> None:
        """Regex generates: WHERE description @@@ pdb.regex('run.*shoes')."""
        queryset = Product.objects.filter(description=ParadeDB(Regex("run.*shoes")))
        assert str(queryset.query) == snapshot


class TestScoreAnnotation:
    """Test Score annotation SQL generation."""

    def test_score_annotation(self, snapshot) -> None:
        """Score generates: SELECT ..., pdb.score(id) AS search_score."""
        queryset = Product.objects.filter(
            description=ParadeDB("running", "shoes")
        ).annotate(search_score=Score())
        assert str(queryset.query) == snapshot

    def test_score_with_ordering(self, snapshot) -> None:
        """Score with ORDER BY search_score DESC."""
        queryset = (
            Product.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .order_by("-search_score")
        )
        assert str(queryset.query) == snapshot

    def test_score_filter(self, snapshot) -> None:
        """Filter by score: WHERE pdb.score(id) > 0."""
        queryset = (
            Product.objects.filter(description=ParadeDB("shoes"))
            .annotate(search_score=Score())
            .filter(search_score__gt=0)
        )
        assert str(queryset.query) == snapshot


class TestSnippetAnnotation:
    """Test Snippet annotation SQL generation."""

    def test_snippet_annotation(self, snapshot) -> None:
        """Snippet generates: pdb.snippet(description) AS snippet."""
        queryset = Product.objects.filter(
            description=ParadeDB(PQ("wireless") | PQ("bluetooth"))
        ).annotate(snippet=Snippet("description"))
        assert str(queryset.query) == snapshot

    def test_snippet_with_custom_formatting(self, snapshot) -> None:
        """Custom snippet: pdb.snippet(description, '<mark>', '</mark>', 100)."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            snippet=Snippet(
                "description",
                start_sel="<mark>",
                stop_sel="</mark>",
                max_num_chars=100,
            )
        )
        assert str(queryset.query) == snapshot


class TestBM25Index:
    """Test BM25 index SQL generation."""

    def test_basic_index_sql(self, snapshot) -> None:
        """Basic BM25 index DDL generation."""
        index = BM25Index(
            fields={"id": {}, "description": {}},
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert sql == snapshot

    def test_index_with_tokenizer(self, snapshot) -> None:
        """Index with tokenizer configuration."""
        index = BM25Index(
            fields={
                "id": {},
                "description": {
                    "tokenizer": "simple",
                    "filters": ["lowercase", "stemmer"],
                    "stemmer": "english",
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert sql == snapshot

    def test_json_field_index(self, snapshot) -> None:
        """JSON field with json_keys configuration."""
        index = BM25Index(
            fields={
                "id": {},
                "metadata": {
                    "json_keys": {
                        "title": {"tokenizer": "simple", "filters": ["lowercase"]},
                        "brand": {"tokenizer": "simple"},
                    }
                },
            },
            key_field="id",
            name="product_search_idx",
        )
        schema_editor = DummySchemaEditor()
        sql = str(index.create_sql(model=Product, schema_editor=schema_editor))
        assert sql == snapshot


class TestMoreLikeThis:
    """Test MoreLikeThis SQL generation."""

    def test_mlt_by_id(self, snapshot) -> None:
        """MLT by ID: WHERE id @@@ pdb.more_like_this(5)."""
        queryset = Product.objects.filter(MoreLikeThis(product_id=5))
        assert str(queryset.query) == snapshot

    def test_mlt_multiple_ids(self, snapshot) -> None:
        """MLT with multiple IDs generates OR conditions."""
        queryset = Product.objects.filter(MoreLikeThis(product_ids=[5, 12, 23]))
        assert str(queryset.query) == snapshot

    def test_mlt_with_parameters(self, snapshot) -> None:
        """MLT with tuning parameters."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                product_id=5, min_term_freq=2, max_query_terms=10, min_doc_freq=1
            )
        )
        assert str(queryset.query) == snapshot

    def test_mlt_by_text(self, snapshot) -> None:
        """MLT by arbitrary text."""
        queryset = Product.objects.filter(
            MoreLikeThis(
                text="comfortable running shoes",
                fields=["description", "category"],
            )
        )
        assert str(queryset.query) == snapshot


class TestDjangoIntegration:
    """Test Django ORM integration."""

    def test_paradedb_with_django_q(self, snapshot) -> None:
        """Combine ParadeDB with Django Q for complex logic."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB(Phrase("running shoes")), rating__gte=4)
            | Q(category=ParadeDB("Electronics"), description=ParadeDB("wireless"))
        )
        assert str(queryset.query) == snapshot

    def test_negation_with_q(self, snapshot) -> None:
        """Negation using ~Q with ParadeDB."""
        queryset = Product.objects.filter(
            Q(description=ParadeDB("running", "athletic")),
            ~Q(description=ParadeDB("cheap")),
        )
        assert str(queryset.query) == snapshot

    def test_with_standard_filters(self, snapshot) -> None:
        """ParadeDB search combined with standard ORM filters."""
        queryset = Product.objects.filter(
            description=ParadeDB("shoes"),
            price__lt=100,
            in_stock=True,
            rating__gte=4,
        )
        assert str(queryset.query) == snapshot

    def test_with_window_functions(self, snapshot) -> None:
        """ParadeDB search with Django window functions."""
        queryset = Product.objects.filter(description=ParadeDB("shoes")).annotate(
            rank_in_category=Window(
                expression=RowNumber(),
                partition_by=[F("category")],
                order_by=F("price").desc(),
            )
        )
        assert str(queryset.query) == snapshot
