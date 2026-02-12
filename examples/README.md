# ParadeDB for Django: Examples & Cookbook

Welcome to the **ParadeDB for Django** examples! This directory contains a collection of self-contained scripts designed to teach you how to integrate powerful search and analytics features into your Django application using ParadeDB.

Think of this as a **cookbook**: whether you need simple keyword search, an e-commerce filtering system, or a cutting-edge RAG (Retrieval-Augmented Generation) pipeline, you'll find a recipe here.

## 🚀 Getting Started

Before running any example, you need to set up your environment.

### 1. Install Dependencies

All examples share a common set of dependencies.

```bash
# Create and activate a virtual environment (optional but recommended)
python -m venv .venv
source .venv/bin/activate

# Install the package and example requirements
pip install -r examples/requirements.txt
```

### 2. Start ParadeDB

You need a running ParadeDB instance. We provide a helper script to start one via Docker and set the necessary environment variables.

```bash
# Sourcing this script starts ParadeDB and exports DATABASE_URL
source scripts/run_paradedb.sh
```

**Note:** If you already have a Postgres instance with ParadeDB installed, you can simply set the `DATABASE_URL` environment variable manually:
`export DATABASE_URL=postgresql://user:password@localhost:5432/dbname`

---

## 📚 The Examples

We've organized the examples into three categories:

1. **Essentials**: Core search features used in almost every app.
2. **Smart Features**: UX enhancements like autocomplete and recommendations.
3. **AI & Vectors**: Advanced semantic search and generative AI flows.

### 🔹 Essentials

#### 1. Quickstart (`quickstart/quickstart.py`)

_The "Hello World" of ParadeDB._

This script demonstrates the fundamental building blocks of search. You will learn how to:

- **Index data**: Define a `BM25Index` on your model.
- **Search**: Perform basic keyword queries.
- **Score**: Sort results by relevance (BM25 score).
- **Highlight**: Generate snippets (e.g., `<b>run</b>ning`) to show users why a result matched.

**Run it:**

```bash
python examples/quickstart/quickstart.py
```

#### 2. Faceted Search (`faceted_search/faceted_search.py`)

_Building an E-commerce Sidebar._

Facets are the "filters" you see on shopping sites (e.g., "Brand (5)", "Color (3)"). This example shows how to compute these counts efficiently in a single query.

**Key Concepts:**

- **Aggregations**: Counting documents by category, rating, etc.
- **Hybrid Results**: Getting search results _and_ facet counts together.
- **Indexing for facets**: Text facet fields should be indexed with
  `literal`/`literal_normalized` tokenizers (as done in `examples/common.py`).
  Numeric fields like `rating` are naturally suited for aggregations.

**About `fast` fields**

This faceted example does not add explicit `fast` field options because the
aggregated fields are already indexed in an aggregation-friendly way:

- `category` and `metadata_color` use `literal` tokenization.
- `rating` is numeric.

Use explicit `fast` options when you need columnar behavior for other fields,
especially when defining BM25 indexes through the Django `BM25Index` DSL.

**Run it:**

```bash
python examples/faceted_search/faceted_search.py
```

---

### 🔹 Smart Features

#### 3. Autocomplete (`autocomplete/`)

_Instant "As-You-Type" Suggestions._

Standard search requires hitting "Enter". Autocomplete gives immediate feedback. This example uses **N-gram tokenization** to match substrings (e.g., "wir" matches "wireless").

**How it works:**

1. We create a specialized index that breaks text into small chunks (n-grams).
2. Queries match these chunks, allowing for partial matches even in the middle of words.

**Run it:**

```bash
cd examples/autocomplete
python setup.py          # Step 1: Create table with ngram index
python autocomplete.py   # Step 2: Run the search demo
```

#### 4. More Like This (`more_like_this/more_like_this.py`)

_Recommendations & "Related Content"._

Want to show "Related Articles" or "Customers also bought"? This feature analyzes the text of a document to find others with similar keywords, using TF-IDF logic—no complex vector embeddings required.

**Run it:**

```bash
python examples/more_like_this/more_like_this.py
```

---

### 🔹 AI & Vectors

#### 5. Hybrid Search with RRF (`hybrid_rrf/`)

_The Best of Both Worlds: Keywords + Semantics._

Keyword search (BM25) is great for exact matches ("Part #123"). Vector search is great for meaning ("warm clothing" matches "coat"). **Hybrid Search** combines them using **Reciprocal Rank Fusion (RRF)** for superior results.

**Prerequisites:**

- `pgvector` must be installed (included in the ParadeDB Docker image).

**Run it:**

```bash
cd examples/hybrid_rrf
python setup.py         # Loads pre-computed embeddings into the DB
python hybrid_rrf.py    # Performs the hybrid search
```

#### 6. RAG: Retrieval-Augmented Generation (`rag/`)

_Chat with your Data._

This example builds a mini QA system. It searches your data for relevant context and feeds it to an LLM (Large Language Model) to answer questions based _only_ on your data.

**Prerequisites:**

- An API Key from [OpenRouter](https://openrouter.ai/) (provides access to GPT-4, Claude, etc.).
- Set `export OPENROUTER_API_KEY=sk-...` in your terminal.

**Run it:**

```bash
cd examples/rag
python rag.py
```

---

## 🛠 Under the Hood: `common.py`

You might notice that many examples import from `common`. This is a helper module located at `examples/common.py`. It handles the boring stuff so the examples remain focused:

- **`configure_django()`**: Sets up a minimal in-memory Django configuration.
- **`MockItem`**: A simple Django model used across examples to simulate products.
- **`setup_mock_items()`**: Populates the database with dummy data.

Feel free to read `common.py` if you're curious how to set up standalone Django scripts!
