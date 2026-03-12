FROM debian:bookworm-slim

ARG TARGETARCH
ARG ORACLE_IC_VERSION=23.26.1.0.0
ARG ORACLE_IC_BUILD=2326100

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl unzip libaio1 libnsl2 \
    && rm -rf /var/lib/apt/lists/*

RUN case "${TARGETARCH}" in \
        amd64) oracle_ic_arch="x64"; oracle_ic_url="https://download.oracle.com/otn_software/linux/instantclient/${ORACLE_IC_BUILD}/instantclient-basiclite-linux.x64-${ORACLE_IC_VERSION}.zip" ;; \
        arm64) oracle_ic_arch="arm64"; oracle_ic_url="https://download.oracle.com/otn_software/linux/instantclient/${ORACLE_IC_BUILD}/instantclient-basiclite-linux.arm64-${ORACLE_IC_VERSION}.zip" ;; \
        *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac \
    && curl -fL "${oracle_ic_url}" -o /tmp/instantclient-basiclite.zip \
    && mkdir -p /opt/oracle \
    && unzip -q /tmp/instantclient-basiclite.zip -d /opt/oracle \
    && rm /tmp/instantclient-basiclite.zip \
    && oracle_ic_dir="$(find /opt/oracle -maxdepth 1 -type d -name 'instantclient_*' | head -n 1)" \
    && test -n "${oracle_ic_dir}" \
    && ln -s "${oracle_ic_dir}" /opt/oracle/instantclient \
    && printf '%s\n' /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf \
    && ldconfig \
    && echo "installed Oracle Instant Client for ${oracle_ic_arch}"

ENV LD_LIBRARY_PATH=/opt/oracle/instantclient
