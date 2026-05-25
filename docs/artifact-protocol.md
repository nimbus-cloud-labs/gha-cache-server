# Artifact service protocol

This server implements the local subset of the GitHub Actions results artifact
service used by `actions/upload-artifact` and `actions/download-artifact`.
Artifacts are stored in the configured blob backend and are not uploaded to
GitHub, so they do not appear in the GitHub Actions web UI or GitHub-hosted
artifact REST API.

## TWIRP API

The artifact endpoints are exposed under:

```text
/twirp/github.actions.results.api.v1.ArtifactService
```

### `CreateArtifact`

Request:

```json
{
  "workflowRunBackendId": "run",
  "workflowJobRunBackendId": "job",
  "name": "logs",
  "version": 4
}
```

Response:

```json
{
  "ok": true,
  "signedUploadUrl": "https://<host>/artifact-upload/<artifact-id>"
}
```

The returned URL accepts either a single `PUT` containing the ZIP payload or
the Azure Block Blob subset used by the official artifact client:

- `PUT <signedUploadUrl>?comp=block&blockid=<base64>` uploads one block.
- `PUT <signedUploadUrl>?comp=blocklist` commits the XML block list and
  completes the backing multipart upload.

### `FinalizeArtifact`

Request:

```json
{
  "workflowRunBackendId": "run",
  "workflowJobRunBackendId": "job",
  "name": "logs",
  "size": 1024,
  "hash": "sha256:..."
}
```

Response:

```json
{
  "ok": true,
  "artifactId": 123456789
}
```

Finalization completes the backend multipart upload and marks the artifact as
downloadable.

### `ListArtifacts`

Request:

```json
{
  "workflowRunBackendId": "run",
  "workflowJobRunBackendId": "job",
  "nameFilter": "logs"
}
```

Response:

```json
{
  "artifacts": [
    {
      "workflowRunBackendId": "run",
      "workflowJobRunBackendId": "job",
      "databaseId": 123456789,
      "name": "logs",
      "size": 1024,
      "digest": "sha256:...",
      "createdAt": "2026-05-24T10:00:00Z"
    }
  ]
}
```

Only finalized, non-deleted artifacts are listed. Lookups are scoped to the
workflow run backend identifier so artifacts uploaded by one job can be
downloaded by later jobs in the same workflow run.

### `GetSignedArtifactURL`

Request:

```json
{
  "workflowRunBackendId": "run",
  "workflowJobRunBackendId": "job",
  "name": "logs"
}
```

Response:

```json
{
  "signedUrl": "https://<host>/artifact-download/<artifact-id>/logs.zip"
}
```

When direct downloads are enabled and the backend can create presigned URLs,
the response contains a backend URL. Otherwise it contains a local proxy URL.

### `DeleteArtifact`

Request:

```json
{
  "workflowRunBackendId": "run",
  "workflowJobRunBackendId": "job",
  "name": "logs"
}
```

Response:

```json
{
  "ok": true,
  "artifactId": 123456789
}
```

Deleted artifacts are hidden from list/download responses and their blob is
removed from storage.

## Expiration

When `CreateArtifact` includes `expiresAt`, the cleanup loop removes the
artifact metadata and blob after that timestamp. Artifact objects are stored
outside the cache generation prefix, so administrative cache resets do not
delete artifact payloads.

## Limitations

The implementation intentionally supports only the Azure Block Blob operations
needed by the artifact upload path. It does not expose general Azure container,
blob metadata, lease, copy, snapshot, or listing APIs.
