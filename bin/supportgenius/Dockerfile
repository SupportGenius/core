# supportgenius — the "same modules as the Worker" binary, composed via
# crates/composition and linked against runtime-native + adapter-sqlite into
# one fully static musl executable.
#
# The build context must be the repository root, not bin/supportgenius (the
# workspace Cargo.toml lives at the root):
#
#   docker build -t supportgenius -f bin/supportgenius/Dockerfile .

# Keep this image tag in step with the toolchain channel pinned in
# rust-toolchain.toml at the repository root (both 1.98.1): cargo in the
# container honours that file, so a tag that drifts from it makes every
# build pay a toolchain download at best and a hard mismatch at worst.
# Bump the two together.
FROM rust:1.98.1-bookworm AS builder

# musl-tools provides the musl-gcc wrapper the x86_64-unknown-linux-musl
# target needs; no extra RUSTFLAGS or linker env — the build is fully static
# out of the box.
RUN apt-get update \
 && apt-get install -y --no-install-recommends musl-tools \
 && rm -rf /var/lib/apt/lists/* \
 && rustup target add x86_64-unknown-linux-musl

WORKDIR /src
# Whole-tree copy: the workspace spans crates/, ventures/ and bin/. (If a
# .dockerignore is ever added, target/ and .git/ would trim this context.)
COPY . .
# rust-toolchain.toml governs the exact toolchain.
RUN cargo build --locked --release --target x86_64-unknown-linux-musl -p supportgenius-bin

# An empty directory to transplant into the runtime image as a nonroot-owned
# /data — distroless has no shell and no mkdir, so the writable SQLite home
# has to be created by COPY --chown.
RUN mkdir /data-empty

FROM gcr.io/distroless/static-debian12:nonroot

# The musl binary is fully static, so distroless/static needs nothing else.
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/supportgenius /supportgenius
COPY --from=builder --chown=nonroot:nonroot /data-empty /data

# HARNESS_SECRET is deliberately not baked in: supply it at run time
# (`docker run -e HARNESS_SECRET=...`). Without it the harness cannot wire
# the Signer port and the binary fails at boot.
ENV LISTEN_ADDR=0.0.0.0:8080 \
    DATABASE_URL=sqlite:///data/supportgenius.db

EXPOSE 8080

USER nonroot

# Distroless ships no curl or wget, so the health probe is the binary's own
# --check-ready, which GETs /__ready on LISTEN_ADDR.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/supportgenius", "--check-ready"]

ENTRYPOINT ["/supportgenius"]
