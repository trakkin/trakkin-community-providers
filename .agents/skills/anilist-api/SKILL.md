---
name: anilist-api
description: 'Guides AniList GraphQL integration work. Use when implementing, debugging, or reviewing AniList queries, mutations, OAuth, pagination, rate limits, schema types, or media mapping.'
---

# AniList API

## Overview

Ground AniList changes in the current GraphQL schema and preserve the distinctions that matter at the Trakkin provider boundary.

## Sources

- Documentation: `https://docs.anilist.co/guide/introduction`
- GraphQL guide: `https://docs.anilist.co/guide/graphql/`
- API reference: `https://docs.anilist.co/reference/`
- GraphQL endpoint: `https://graphql.anilist.co`

## Workflow

1. Inspect the nearest query, transport type, mapper, and focused test under `crates/trakkin-provider-anilist/`.
2. Find the relevant operation in the official reference. Introspect the live schema when field, argument, nullability, union, or interface details are uncertain.
3. Send `POST` JSON with `query` and variables. Request only consumed fields; use fragments only when they improve reuse or readability.
4. Handle HTTP failure and GraphQL `errors` separately. Preserve meaningful nullability and type discrimination until normalization.
5. For operations using `Page`, follow `pageInfo.hasNextPage`; do not apply page semantics to `MediaListCollection`.
6. Implement the smallest change that fits the provider contract, then run the focused test that exercises it.

## Stop and Verify

- Stop if a field or argument comes only from memory; verify it against the schema.
- Stop if a successful HTTP response is treated as a successful GraphQL operation without checking `errors`.
- Never place OAuth tokens in query text, variables, logs, errors, or committed fixtures.

## Validation

- Client or transport: `cargo test -p trakkin-provider-anilist --test client`
- Discovery or authentication: `cargo test -p trakkin-provider-anilist --test discovery`
- Catalog or state mapping: `cargo test -p trakkin-provider-anilist --test catalog_state`
- Shared provider behavior: `cargo test -p trakkin-provider-anilist --all-targets`
