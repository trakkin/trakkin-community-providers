---
name: plex-api
description: 'Guides Plex Media Server API and Trakkin provider work. Use when implementing, debugging, or reviewing Plex authentication, libraries, metadata, pagination, playback, watch history, or wire mapping.'
---

# Plex API

## Overview

Ground Plex changes in the official contract and the supported server behavior. Use community schemas for discovery, not as final authority.

## Sources

- API reference: `https://developer.plex.tv/pms/`
- Community documentation index: `https://plexapi.dev/llms.txt`
- Community OpenAPI schema: `https://plexapi.dev/plex-media-server.openapi.json`

The official site exposes no stable OpenAPI JSON URL. Do not persist or cite its session-bound `blob:` download.

## Workflow

1. Inspect the nearest client, adapter, catalog, observation code, and focused test under `crates/trakkin-provider-plex/`.
2. Find the official operation and required `X-Plex-*` headers. Use the community index or schema only to accelerate path and wire-shape discovery.
3. Confirm whether the operation requires explicit JSON negotiation, discovered keys, or server-specific feature paths.
4. Preserve optional and type-specific metadata until the Plex-to-Trakkin mapping decision is made.
5. Follow returned offsets and totals for pagination; do not assume the requested page size was returned.
6. Implement the smallest provider-aligned change, then run the focused test that exercises it.

## Stop and Verify

- Stop when community documentation conflicts with the official operation or observed supported-server behavior; resolve the discrepancy before coding.
- Stop before constructing an undocumented metadata key or feature path; verify how Plex exposes it.
- Never place Plex tokens in URLs, logs, persistent artwork links, errors, or committed fixtures.

## Validation

- Discovery or authentication: `cargo test -p trakkin-provider-plex --test discovery`
- Catalog mapping: `cargo test -p trakkin-provider-plex --test catalog`
- Watch history: `cargo test -p trakkin-provider-plex --test observations`
- Shared provider behavior: `cargo test -p trakkin-provider-plex --all-targets`
- Provider lint: `cargo clippy -p trakkin-provider-plex --all-targets --all-features -- -D warnings`
