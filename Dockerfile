ARG OA_RUNTIME_BASE_IMAGE=open_archive-oracle-runtime:23.26.1

FROM rust:1.86-slim AS builder

WORKDIR /app

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY src src
COPY sql sql

RUN cargo build --release --bin open_archive

FROM ${OA_RUNTIME_BASE_IMAGE}

WORKDIR /app

COPY --from=builder /app/target/release/open_archive /usr/local/bin/open_archive
COPY --from=builder /app/sql /app/sql
COPY docker/entrypoint.sh /usr/local/bin/open_archive-entrypoint

RUN chmod +x /usr/local/bin/open_archive-entrypoint

ENV OA_HTTP_BIND=0.0.0.0:3000
ENV OA_RELATIONAL_STORE=postgres
ENV OA_OBJECT_STORE=local_fs
ENV OA_OBJECT_STORE_ROOT=/var/lib/openarchive/objects
ENV RUST_LOG=info

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/open_archive-entrypoint"]
CMD ["serve"]
