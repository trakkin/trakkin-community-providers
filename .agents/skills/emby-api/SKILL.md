---
name: emby-api
description: 'Guides Emby Server API integration work. Use when implementing, debugging, or reviewing Emby authentication, users, libraries, items, playback, sessions, webhooks, pagination, or wire models.'
---

# Emby API

## Overview

Use Emby's OpenAPI contract for wire behavior and verify version-sensitive differences against the server versions the integration supports.

## Sources

- API reference: `https://swagger.emby.media/?staticview=true#/`
- OpenAPI schema: `https://swagger.emby.media/openapi.json`

## Workflow

1. Identify the supported Emby server version and the local integration boundary, if one exists.
2. Locate the exact operation and referenced schemas in the official contract.
3. Derive method, path, parameters, authentication, content type, statuses, and wire shape from that operation.
4. Preserve meaningful optional, nullable, enum, and polymorphic states until normalization.
5. Investigate schema/runtime mismatches as version-sensitive behavior; do not fill gaps from Jellyfin.
6. Add a representative request, response, or mapping test and run the narrowest available check.

## Stop and Verify

- Stop if an Emby behavior was inferred from Jellyfin or historical MediaBrowser similarity.
- Stop if runtime behavior differs from OpenAPI; record the supported server version and test the observed contract.
- Never place API keys, authorization headers, or credential-bearing URLs in logs, errors, source control, or committed fixtures.

## Validation

- Prove the changed request shape or mapping with a representative fixture.
- Cover the relevant authentication, pagination, malformed-data, or error-status boundary.
- Run the owning package's focused test, then its broader checks if shared transport changes.
- Trakkin currently has no Emby provider package; do not claim repository-level provider validation until one exists.
