---
name: trakt-api
description: 'Guides Trakt Public API integration work. Use when implementing, debugging, or reviewing OAuth, movies, shows, lists, history, ratings, scrobbling, sync, pagination, extended data, or rate limits.'
---

# Trakt API

## Overview

Use Trakt's operation-level OpenAPI fragments for HTTP contracts and its guides for cross-operation workflow semantics.

## Sources

- Documentation index: `https://docs.trakt.tv/llms.txt`
- API reference index: `https://docs.trakt.tv/reference/llms.txt`
- OpenAPI source: `https://github.com/trakt/trakt-api/tree/master/projects/openapi`

No stable public URL for the complete generated `openapi.json` is verified. Do not depend on ReadMe's internal `api-next` endpoints.

## Workflow

1. Find the operation through the reference index and open its `.md` representation.
2. Record method, path, parameters, security, statuses, schemas, and `operationId` from its OpenAPI fragment.
3. Consult OAuth, pagination, extended-data, filter, rate-limit, or sync guides only when the operation needs them.
4. Preserve operation-specific API-version headers, client keys, OAuth, and access restrictions. Treat VIP-only or limited access separately from missing data.
5. For recurring sync, use documented last-activity timestamps to avoid broad requests when nothing changed.
6. Use the official OpenAPI generator repository only when a cross-operation view is genuinely necessary.
7. Add a focused request, sync, or mapping test, then run the narrowest available check.

## Stop and Verify

- Stop before generalizing one operation's headers, access level, pagination, or rate-limit behavior to another.
- Stop before broad sync when last-activity timestamps can prove no relevant change.
- Never place client keys, OAuth tokens, or authorization headers in logs, errors, source control, or committed fixtures.

## Validation

- Prove the changed operation's headers, request shape, statuses, and mapping.
- Cover unauthorized versus limited access, rate limits, pagination, and activity-based sync as applicable.
- Run the owning package's focused test, then broader checks if shared headers, transport, or mapping changes.
- Trakkin currently has no Trakt provider package; do not claim repository-level provider validation until one exists.
