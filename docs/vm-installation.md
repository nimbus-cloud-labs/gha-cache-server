# VM installation

`gha-cache-server` can run directly on Linux virtual machines without Docker or
Kubernetes. The repository includes packaging for Debian-based distributions,
RPM-based distributions, and Arch Linux.

## Release artifacts

Pushing a semantic version tag such as `v1.2.3` runs the release workflow and
publishes installable packages to the GitHub Release:

- Debian packages (`.deb`) for `amd64` and `arm64`.
- RPM packages (`.rpm`) for `amd64` and `arm64`.
- Arch Linux packages (`.pkg.tar.zst`) for `amd64`.

The packages install:

- `/usr/bin/gha-cache-server`.
- `/etc/gha-cache-server/env` as the service environment file.
- `/var/lib/gha-cache-server` as the default data directory.
- A `gha-cache-server.service` systemd unit.
- A dedicated `gha-cache-server` system user and group.

## Debian and Ubuntu

Download the `.deb` artifact that matches the VM architecture, then install and
start the service:

```bash
sudo apt install ./gha-cache-server_*.deb
sudo editor /etc/gha-cache-server/env
sudo systemctl enable --now gha-cache-server
sudo systemctl status gha-cache-server
```

To build the package locally from a checkout:

```bash
sudo apt-get update
sudo apt-get install -y build-essential debhelper devscripts pkg-config protobuf-compiler libprotobuf-dev
dpkg-buildpackage -us -uc -b
```

The generated package is written to the parent directory of the checkout.

## Fedora, RHEL, CentOS, and compatible distributions

Download the `.rpm` artifact that matches the VM architecture, then install and
start the service:

```bash
sudo dnf install ./gha-cache-server-*.rpm
sudo editor /etc/gha-cache-server/env
sudo systemctl enable --now gha-cache-server
sudo systemctl status gha-cache-server
```

To build the package locally from a checkout:

```bash
sudo dnf group install -y development-tools
sudo dnf install -y git rpm-build rpmdevtools systemd-rpm-macros cmake pkgconfig protobuf-compiler protobuf-devel rust cargo
rpmdev-setuptree
VERSION=$(awk '/^Version:/ { print $2; exit }' packaging/rpm/gha-cache-server.spec)
git archive --format=tar --prefix="gha-cache-server-${VERSION}/" HEAD \
  | gzip > "${HOME}/rpmbuild/SOURCES/gha-cache-server-${VERSION}.tar.gz"
cp packaging/rpm/gha-cache-server.spec "${HOME}/rpmbuild/SPECS/"
rpmbuild -ba "${HOME}/rpmbuild/SPECS/gha-cache-server.spec"
```

The generated packages are written below `${HOME}/rpmbuild/RPMS` and
`${HOME}/rpmbuild/SRPMS`.

On SELinux enforcing systems, label the default data directory if the policy
does not already allow the service to read and write it:

```bash
sudo semanage fcontext -a -t var_lib_t '/var/lib/gha-cache-server(/.*)?'
sudo restorecon -Rv /var/lib/gha-cache-server
```

## Runtime configuration

The packaged service reads environment variables from
`/etc/gha-cache-server/env`. The default configuration uses SQLite metadata and
filesystem blob storage under `/var/lib/gha-cache-server`:

```env
DATABASE_URL=sqlite:///var/lib/gha-cache-server/cache.db
BLOB_STORE=fs
```

For production installs, update the same file to point at PostgreSQL, MySQL, S3,
or Google Cloud Storage as needed. See the full
[configuration guide](configuration.md) for all supported environment
variables.

After changing configuration, restart the service:

```bash
sudo systemctl restart gha-cache-server
sudo journalctl -u gha-cache-server -f
```
