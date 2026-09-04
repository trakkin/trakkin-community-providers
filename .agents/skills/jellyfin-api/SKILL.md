---
name: jellyfin-api
description: 'Guides Jellyfin API integration work. Use when implementing, debugging, or reviewing Jellyfin authentication, libraries, items, playback, sessions, plugins, pagination, events, or wire models.'
---

# Jellyfin API

## Overview

Use Jellyfin's stable OpenAPI contract by default and preserve version-sensitive wire behavior at the integration boundary.

## Sources

- API reference: `https://api.jellyfin.org/`
- OpenAPI schema: `https://api.jellyfin.org/openapi/jellyfin-openapi-stable.json`

## Workflow

1. Identify the supported Jellyfin server version and the local integration boundary, if one exists.
2. Locate the exact operation and component schemas in the stable contract unless the task explicitly targets an unstable server.
3. Derive method, path, parameters, authentication, content type, statuses, and wire shape from that operation.
4. Preserve meaningful nullable fields, optional arrays, discriminators, enums, and unknown future values until normalization.
5. Investigate schema/runtime mismatches as version-sensitive behavior; do not fill gaps from Emby.
6. Add a representative request, response, or mapping test and run the narrowest available check.

## Stop and Verify

- Stop if a Jellyfin behavior was inferred from Emby or historical MediaBrowser similarity.
- Stop if runtime behavior differs from the stable schema; record the supported server version and test the observed contract.
- Never place access tokens or authorization headers in logs, errors, source control, or committed fixtures.

## Validation

- Prove the changed request shape or mapping with a representative fixture.
- Cover the relevant authorization, pagination, malformed-response, or version-sensitive boundary.
- Run the owning package's focused test, then its broader checks if shared transport changes.
- Trakkin currently has no Jellyfin provider package; do not claim repository-level provider validation until one exists.
