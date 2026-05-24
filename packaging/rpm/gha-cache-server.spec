Name:           gha-cache-server
Version:        1.0.0
Release:        1%{?dist}
Summary:        Self-hosted compatibility server for the GitHub Actions cache API

License:        MIT
URL:            https://github.com/n-cloud-labs/gha-cache-server
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  cmake
BuildRequires:  gcc
BuildRequires:  make
BuildRequires:  pkgconfig
BuildRequires:  protobuf-compiler
BuildRequires:  protobuf-devel
BuildRequires:  systemd-rpm-macros
Requires:       systemd
Requires(post): shadow-utils
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd

%description
gha-cache-server is a self-hosted implementation of the GitHub Actions cache API,
allowing workflows to interact with a privately managed cache backend.

%prep
%autosetup -n %{name}-%{version}

%build
export CARGO_HOME=%{_builddir}/.cargo
export CARGO_TARGET_DIR=%{_builddir}/target
cargo build --release --locked

%install
rm -rf %{buildroot}
install -Dm0755 %{_builddir}/target/release/gha-cache-server \
    %{buildroot}%{_bindir}/gha-cache-server
install -d -m0750 %{buildroot}%{_sysconfdir}/gha-cache-server
install -Dm0640 packaging/config/env \
    %{buildroot}%{_sysconfdir}/gha-cache-server/env
install -d -m0750 %{buildroot}%{_localstatedir}/lib/gha-cache-server
install -Dm0644 packaging/rpm/gha-cache-server.service \
    %{buildroot}%{_unitdir}/gha-cache-server.service
install -Dm0644 packaging/rpm/gha-cache-server.tmpfiles \
    %{buildroot}%{_tmpfilesdir}/gha-cache-server.conf

%post
if ! getent group gha-cache-server >/dev/null 2>&1; then
    groupadd --system gha-cache-server >/dev/null 2>&1 || :
fi
if ! getent passwd gha-cache-server >/dev/null 2>&1; then
    useradd --system --home-dir /var/lib/gha-cache-server --no-create-home \
        --gid gha-cache-server --shell %{_sbindir}/nologin gha-cache-server >/dev/null 2>&1 || :
fi
install -d -o gha-cache-server -g gha-cache-server -m 0750 /var/lib/gha-cache-server
install -d -o gha-cache-server -g gha-cache-server -m 0750 /etc/gha-cache-server
if [ -f /etc/gha-cache-server/env ]; then
    chown gha-cache-server:gha-cache-server /etc/gha-cache-server/env
    chmod 0640 /etc/gha-cache-server/env
fi
if command -v systemd-tmpfiles >/dev/null 2>&1; then
    systemd-tmpfiles --create %{_tmpfilesdir}/gha-cache-server.conf >/dev/null 2>&1 || :
fi
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || :
    systemctl enable gha-cache-server.service >/dev/null 2>&1 || :
    systemctl try-restart gha-cache-server.service >/dev/null 2>&1 || :
fi

%preun
if [ $1 -eq 0 ]; then
    if command -v systemctl >/dev/null 2>&1; then
        systemctl disable gha-cache-server.service >/dev/null 2>&1 || :
        systemctl stop gha-cache-server.service >/dev/null 2>&1 || :
        systemctl daemon-reload >/dev/null 2>&1 || :
    fi
fi

%postun
if command -v systemctl >/dev/null 2>&1; then
    systemctl daemon-reload >/dev/null 2>&1 || :
fi

%files
%license LICENSE
%doc README.md
%doc packaging/rpm/README
%{_bindir}/gha-cache-server
%dir %attr(0750,gha-cache-server,gha-cache-server) %{_sysconfdir}/gha-cache-server
%config(noreplace) %attr(0640,gha-cache-server,gha-cache-server) %{_sysconfdir}/gha-cache-server/env
%dir %attr(0750,gha-cache-server,gha-cache-server) %{_localstatedir}/lib/gha-cache-server
%{_unitdir}/gha-cache-server.service
%{_tmpfilesdir}/gha-cache-server.conf

%changelog
* Tue Feb 18 2025 Alessandro Chitolina <alekitto@gmail.com> - 1.0.0-1
- Initial RPM packaging for gha-cache-server
