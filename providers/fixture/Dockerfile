# syntax=docker/dockerfile:1

FROM rust:1.97.1-alpine AS build

WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY src src

RUN --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
  --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
  cargo build --locked --release --package trakkin-provider-fixture

FROM scratch AS artifact

COPY --from=build /workspace/target/release/trakkin-provider-fixture /provider
