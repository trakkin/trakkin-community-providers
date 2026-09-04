---
name: simkl-api
description: 'Guides Simkl API integration work. Use when implementing, debugging, or reviewing OAuth or PIN auth, catalog search, watchlists, history, ratings, playback, scrobbling, sync, pagination, or media mapping.'
---

# Simkl API

## Overview

Use Simkl's operation documentation for workflow semantics and its OpenAPI schema for the HTTP contract.

## Sources

- Documentation index: `https://api.simkl.org/llms.txt`
- OpenAPI schema: `https://api.simkl.org/openapi.json`

## Workflow

1. Follow the relevant operation link from `llms.txt`; consult `api-rules.md` and authentication, rate-limit, or workflow pages only when the task reaches those concerns.
2. Confirm method, path, parameters, security, statuses, and wire shape in OpenAPI.
3. Choose OAuth, PKCE, or PIN authentication from the client context; do not apply one flow universally. For PIN, poll at the returned `interval` and stop when `expires_in` elapses.
4. For recurring sync, check documented activity timestamps before fetching full lists and honor endpoint or cached-file cost guidance.
5. Preserve TV, movie, and anime differences in fields, external IDs, and watchlist rules until normalization.
6. Add a focused request, sync, or mapping test, then run the narrowest available check.

## Stop and Verify

- Stop if product-help documentation is being used as the API contract.
- Stop PIN polling when the issued code expires; require a new flow rather than polling indefinitely.
- Stop before a full recurring sync when activity timestamps can prove no relevant change.
- Never place client secrets or access tokens in URLs, logs, errors, source control, or committed fixtures.

## Validation

- Prove the chosen authentication flow and changed request or mapping shape.
- Cover rate limits, pagination, activity-based sync decisions, and media-specific behavior as applicable.
- Run the owning package's focused test, then broader checks if shared transport or sync changes.
- Trakkin currently has no Simkl provider package; do not claim repository-level provider validation until one exists.
