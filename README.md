<div align="center">

<a href="https://github.com/ez-cdc/dbmazz" target="_blank">
  <img src="assets/ez-cdc-banner.png" alt="dbmazz — Real-time PostgreSQL CDC" width="100%">
</a>

<br>

# dbmazz

**Real-time PostgreSQL CDC. One binary. No Kafka.**

A single Rust daemon that streams PostgreSQL changes to StarRocks, Snowflake, or another PostgreSQL with sub-second replication lag in steady state.

Built and maintained by **[EZ-CDC](https://ez-cdc.com)**.

[![License: ELv2](https://img.shields.io/badge/license-ELv2-blue.svg)](LICENSE)
[![GHCR](https://img.shields.io/badge/ghcr.io-ez--cdc%2Fdbmazz-0969da?logo=docker)](https://github.com/ez-cdc/dbmazz/pkgs/container/dbmazz)
[![Discussions](https://img.shields.io/badge/GitHub-Discussions-181717?logo=github)](https://github.com/ez-cdc/dbmazz/discussions)
[![Built with Rust](https://img.shields.io/badge/built%20with-Rust-orange?logo=rust)](https://www.rust-lang.org)

</div>

---

## Try it

dbmazz is operated through the `ez-cdc` CLI. Install it with one command:

```bash
curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
```

Then point it at a PostgreSQL source and a sink:

```bash
ez-cdc datasource init                                    # write a starter config
ez-cdc datasource add                                     # interactive wizard for source + sink
ez-cdc quickstart --source my-pg --sink my-warehouse      # spin up dbmazz + live dashboard
```

Press `t` in the dashboard to generate live traffic, `q` to quit. The CLI runs on Linux and macOS (`amd64` and `arm64`).

Full CLI reference: [`e2e-cli/README.md`](e2e-cli/README.md).

---

## Production

For managed BYOC deployment with auto-healing workers, centralized monitoring, RBAC, audit logs, and a web portal — running dbmazz in your own AWS or GCP account via Terraform — see **[EZ-CDC Cloud](https://ez-cdc.com)**.

---

## Contributing

dbmazz welcomes contributions.

1. Read [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup, conventions, and the PR checklist.
2. To add a new source or sink connector, see [`docs/contributing-connectors.md`](docs/contributing-connectors.md).

---

## Community

- **GitHub Discussions** — questions, ideas, show & tell: [github.com/ez-cdc/dbmazz/discussions](https://github.com/ez-cdc/dbmazz/discussions)
- **Issues** — bug reports and feature requests: [github.com/ez-cdc/dbmazz/issues](https://github.com/ez-cdc/dbmazz/issues)
- **Changelog** — [`CHANGELOG.md`](CHANGELOG.md)

---

## License

[Elastic License v2.0](LICENSE).

dbmazz is free for commercial and non-commercial use, including running it in production, embedding it in your own product, or modifying it for internal use. The only restriction is that you cannot offer dbmazz to third parties as a managed service.

---

## About EZ-CDC

**dbmazz is the open-source CDC engine maintained by [EZ-CDC](https://ez-cdc.com).** We build modern data replication tools for teams that want streaming Postgres CDC without operating a streaming platform.

The same team also runs **[EZ-CDC Cloud](https://ez-cdc.com)**: a managed BYOC platform that deploys dbmazz into your own AWS or GCP account via Terraform.
