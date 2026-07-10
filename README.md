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

## Installing on a VM

The project ships native Linux packaging for direct VM installs. Tagged
releases publish Debian `.deb` and RPM `.rpm` artifacts for `amd64` and `arm64`,
alongside the container image and Helm chart.

Install the package for your distribution, edit `/etc/gha-cache-server/env`, and
start the systemd service:

```bash
sudo apt install ./gha-cache-server_*.deb
sudo systemctl enable --now gha-cache-server
```

```bash
sudo dnf install ./gha-cache-server-*.rpm
sudo systemctl enable --now gha-cache-server
```

The default package configuration stores SQLite metadata and filesystem blobs
under `/var/lib/gha-cache-server`. Production deployments can switch to
PostgreSQL, MySQL, S3, or Google Cloud Storage through the environment file.
See the [VM installation guide](docs/vm-installation.md) for package build
commands and operational notes.

## Additional documentation

Further configuration topics, including cleanup controls and advanced storage
settings, are documented in [`docs/configuration.md`](docs/configuration.md).
The local GitHub Actions artifact service is documented in
[`docs/artifact-protocol.md`](docs/artifact-protocol.md).
Administrative cache resets rotate an internal storage generation so new
artifacts are written below a fresh `vN/` prefix while the previous generation
is retired from metadata immediately.
