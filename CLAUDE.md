# Testing

Run the tests before every push:

```bash
scripts/run_tests.sh
```

To run a subset of tests, pass pytest selectors:

```bash
scripts/run_tests.sh tests/test_paradedb_queries.py::test_tokenizer_override_invalid_identifier
```
