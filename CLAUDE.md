# Integration Tests

Integration tests require a running ParadeDB/Postgres instance. Use the provided script to start the container and set up the correct DSN:

```bash
PARADEDB_PASSWORD=postgres scripts/run_integration_tests.sh
```

To run a subset of tests, pass pytest selectors:

```bash
PARADEDB_PASSWORD=postgres scripts/run_integration_tests.sh tests/integration/test_paradedb_queries.py::test_tokenizer_override_invalid_identifier
```
