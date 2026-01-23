# Installation

## Requirements

- Python 3.12+
- Django 5.0+
- PostgreSQL with ParadeDB pg_search extension v0.20.0+

## Install the package

```bash
pip install django-paradedb
```

## Add to Django settings

```python
INSTALLED_APPS = [
    # ...
    'paradedb',
]
```

## Database setup

Ensure your PostgreSQL database has the ParadeDB extension enabled:

```sql
CREATE EXTENSION IF NOT EXISTS pg_search;
```
