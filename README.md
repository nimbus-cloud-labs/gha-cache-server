# GHA Cache Server

The GHA Cache Server is a Rust implementation of the GitHub Actions cache
service. It exposes the same HTTP surface as the hosted Actions cache so that
self-managed runners can share build artifacts across jobs and workflows.

## Prerequisites

Before starting the server make sure the following requirements are satisfied:

- `DATABASE_URL` – connection string for the PostgreSQL, MySQL or SQLite
  database that stores cache metadata. The URL must use one of the supported
  drivers described in `src/config.rs` and be reachable from the running
  process.
- Blob storage backend – select the backend with the `BLOB_STORE` environment
  variable and provide the backend-specific configuration. The server ships with
  S3, Google Cloud Storage and filesystem implementations. See the
  [configuration guide](docs/configuration.md) for the full list of options and
  environment variables.

## Running locally

With the prerequisites in place you can start the server with Cargo:

```bash
cargo run --release
```

By default the binary listens on port `8080`. Additional runtime options are
available through environment variables; refer to the documentation linked
above for details.

## Additional documentation

Further configuration topics, including cleanup controls and advanced storage
settings, are documented in [`docs/configuration.md`](docs/configuration.md).
Administrative cache resets rotate an internal storage generation so new
artifacts are written below a fresh `vN/` prefix while the previous generation
is retired from metadata immediately.
