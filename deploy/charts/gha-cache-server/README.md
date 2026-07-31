# gha-cache-server Helm chart

This chart deploys the GitHub Actions compatible cache server. It exposes the
HTTP API, configures the storage backend through environment variables and
optionally provisions supporting resources such as ingress rules or filesystem
volumes.

## Configuration

### Container image

The chart uses the published image at `ghcr.io/n-cloud-labs/gha-cache-server`.
Override `.image.tag` to pin a specific release. Use `.imagePullSecrets` when
working with private registries.

### Environment configuration

Sensitive values are stored in a Secret while non-sensitive configuration lives
in a ConfigMap. Populate the maps through the following values:

- `.env.config` – map of environment variables rendered into the ConfigMap.
  Typical entries include `BLOB_STORE`, `DATABASE_DRIVER` and the cleanup
  tunables (`CLEANUP_INTERVAL_SECS`, `CACHE_ENTRY_MAX_AGE_SECS`,
  `CACHE_STORAGE_MAX_BYTES`, `CACHE_STORAGE_MIN_AVAILABLE_BYTES` and
  `CACHE_STORAGE_TARGET_AVAILABLE_BYTES`).
- `.env.secret` – map of secrets injected through `envFrom`. Set
  `DATABASE_URL` and storage credentials here.
- `.env.existingConfigMap` / `.env.existingSecret` – reference pre-existing
  resources instead of having the chart create them.
- `.env.extra` / `.env.extraEnvFrom` – raw additions to the Deployment for
  advanced scenarios.

### Storage backends

The server supports filesystem, S3-compatible and Google Cloud Storage
backends. Choose one by setting the `BLOB_STORE` environment variable via
`.env.config` and supply the backend-specific values listed below.

#### Amazon S3 and compatible providers

```yaml
env:
  config:
    BLOB_STORE: s3
    DATABASE_DRIVER: postgres
  secret:
    DATABASE_URL: postgres://user:pass@db/cache
    AWS_ACCESS_KEY_ID: your-access-key
    AWS_SECRET_ACCESS_KEY: your-secret
  extra:
    - name: AWS_REGION
      value: eu-central-1
```

Optional keys such as `AWS_ENDPOINT_URL`, `S3_FORCE_PATH_STYLE`,
`AWS_TLS_CA_BUNDLE`, or `AWS_TLS_INSECURE` can be added to `.env.config` or
`.env.extra` as needed.

#### Google Cloud Storage

```yaml
env:
  config:
    BLOB_STORE: gcs
    DATABASE_DRIVER: postgres
    GCS_BUCKET: your-bucket
  secret:
    DATABASE_URL: postgres://user:pass@db/cache
    GCS_SERVICE_ACCOUNT_JSON: |
      {"type":"service_account", ...}
```

When running inside GKE with Workload Identity, omit the inline credentials and
instead bind the pod ServiceAccount to the desired IAM service account.

#### Filesystem backend

Enable the `.fsBackend.enabled` flag to mount persistent storage. The chart can
provision a PersistentVolumeClaim or reuse an existing one:

```yaml
fsBackend:
  enabled: true
  mountPath: /var/lib/gha-cache
  uploadRoot: /var/lib/gha-cache-uploads
  size: 200Gi
  uploads:
    enabled: true
    mountPath: /var/lib/gha-cache-uploads
    emptyDir:
      medium: Memory
```

When `uploadRoot` is set (explicitly or via the `.fsBackend.uploads.mountPath`
default) the chart injects the `FS_UPLOAD_ROOT` environment variable so staged
multipart uploads land on the desired path. Enabling `.fsBackend.uploads` mounts
an ephemeral `emptyDir` volume at the configured `mountPath`, keeping temporary
uploads outside the persistent cache storage.

Additional POSIX permission settings (such as `FS_FILE_MODE`) should be added to
`.env.config`. Grant the pod access to the PersistentVolume by enabling `.rbac`
when the cluster enforces strict policies.

### Cleanup settings

The server periodically removes expired caches. Configure the frequency and
limits through `.env.config`:

```yaml
env:
  config:
    CLEANUP_INTERVAL_SECS: "300"
    CACHE_ENTRY_MAX_AGE_SECS: "86400"
    CACHE_STORAGE_MAX_BYTES: "107374182400"
    CACHE_STORAGE_MIN_AVAILABLE_BYTES: "10737418240"
    CACHE_STORAGE_TARGET_AVAILABLE_BYTES: "21474836480"
```

### Ingress and service exposure

Customize `.service` and `.ingress` to expose the HTTP endpoint. The Deployment
exposes port `8080` by default and readiness/liveness probes monitor the
`/healthz` path.

### Database migrations

The container automatically runs migrations on startup. For controlled rollout
scenarios you can enable the provided Job by setting `.migrationJob.enabled`
and adjusting `.migrationJob.command`/`.migrationJob.args`. The default command
invokes `gha-cache-server --migrate-only`, which is useful once the binary
supports a dedicated migration mode. Today you can perform migrations manually
before deploying:

```bash
sqlx migrate run --source migrations/postgres
```

Run the command with the driver that matches your database (`migrations/mysql`
or `migrations/sqlite`).

## Installation

Render the templates with your chosen configuration and install the release:

```bash
helm upgrade --install cache-server deploy/charts/gha-cache-server \
  --namespace gha-cache --create-namespace \
  -f my-values.yaml
```

## Cleanup

To uninstall the release and remove deployed resources:

```bash
helm uninstall cache-server --namespace gha-cache
```

Persistent volumes created by the chart are not deleted automatically. Remove
any PVCs and storage buckets manually when they are no longer required.
