<h1 align="center">
  <a href="https://paradedb.com">
    <picture align=center>
      <source media="(prefers-color-scheme: dark)" srcset="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-dark-large.svg">
      <source media="(prefers-color-scheme: light)" srcset="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-light-large.svg">
      <img alt="The ParadeDB logo." src="https://github.com/paradedb/paradedb/raw/main/docs/logo/paradedb-logo-light-large.svg">
    </picture>
  </a>
  <br>
</h1>

<p align="center">
  <b>Just use Postgres.</b><br/>
  One Postgres for your application data, full-text search, vector retrieval, and aggregations.
</p>

<h3 align="center">
  <a href="https://paradedb.com">Website</a> &bull;
  <a href="https://www.paradedb.com/docs/start/introduction">Docs</a> &bull;
  <a href="https://paradedb.com/slack/">Community</a> &bull;
  <a href="https://paradedb.com/blog/">Blog</a> &bull;
  <a href="https://www.paradedb.com/docs/project/changelog">Changelog</a>
</h3>

<p align="center">
  <a href="https://pypi.org/project/django-paradedb/"><img src="https://img.shields.io/pypi/v/django-paradedb" alt="PyPI"></a>&nbsp;
  <a href="https://pypi.org/project/django-paradedb/"><img src="https://img.shields.io/pypi/pyversions/django-paradedb" alt="Python Versions"></a>&nbsp;
  <a href="https://pypi.org/project/django-paradedb/"><img src="https://img.shields.io/pypi/dm/django-paradedb" alt="Downloads"></a>&nbsp;
  <a href="https://codecov.io/gh/paradedb/django-paradedb"><img src="https://codecov.io/gh/paradedb/django-paradedb/graph/badge.svg" alt="Codecov"></a>&nbsp;
  <a href="https://github.com/paradedb/django-paradedb?tab=MIT-1-ov-file#readme"><img src="https://img.shields.io/github/license/paradedb/django-paradedb?color=blue" alt="License"></a>&nbsp;
  <a href="https://paradedb.com/slack"><img src="https://img.shields.io/badge/Join%20Slack-purple?logo=slack" alt="Community"></a>&nbsp;
  <a href="https://x.com/paradedb"><img src="https://img.shields.io/twitter/url?url=https%3A%2F%2Ftwitter.com%2Fparadedb&label=Follow%20%40paradedb" alt="Follow @paradedb"></a>
</p>

---

## ParadeDB for Django

The official [Django](https://www.djangoproject.com/) integration for ParadeDB. Get started with the [setup guide](https://www.paradedb.com/docs/start/connect-your-app#django).

## Requirements & Compatibility

| Component            | Supported                                                                                                                                                            |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Python               | 3.10+                                                                                                                                                                |
| Django               | 4.2+                                                                                                                                                                 |
| PostgreSQL           | 15+                                                                                                                                                                  |
| pgvector             | 0.7.0+ (provides vector types for ParadeDB’s [native vector search](https://www.paradedb.com/docs/reference/vector/overview), included in the ParadeDB Docker image) |
| ParadeDB / pg_search | 0.26.0+                                                                                                                                                              |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, running tests, linting, and the PR workflow.

## Support

If you're missing a feature or have found a bug, please open a [GitHub Issue](https://github.com/paradedb/django-paradedb/issues/new/choose). For community support, join the [ParadeDB Slack Community](https://paradedb.com/slack).

## Acknowledgments

We would like to thank the following members of the Django community for their valuable feedback and reviews during the development of this package:

- [Timothy Allen](https://github.com/FlipperPA) - Principal Engineer at The Wharton School, PSF and DSF member
- [Frank Wiles](https://github.com/frankwiles) - President & Founder of REVSYS

## License

ParadeDB for Django is licensed under the [MIT License](LICENSE).
