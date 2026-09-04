---
name: myanimelist-api
description: 'Guides MyAnimeList API v2 integration work. Use when implementing, debugging, or reviewing anime or manga discovery, OAuth, list status, user data, pagination, field selection, or wire models.'
---

# MyAnimeList API

## Overview

Ground MyAnimeList changes in the exact v2 operation contract, especially its operation-specific authentication, sparse fields, paging links, and form-encoded writes.

## Sources

- API reference: `https://myanimelist.net/apiconfig/references/api/v2`
- API base URL: `https://api.myanimelist.net/v2`

The reference embeds OpenAPI in rendered page state but exposes no stable official JSON or YAML URL. Do not invent one.

## Workflow

1. Locate the exact operation in the official v2 reference.
2. Record its method, authentication choices, parameters, encoding, statuses, and response schema before editing code.
3. Choose OAuth for user-scoped operations; use `X-MAL-CLIENT-ID` only where the operation allows it.
4. Request only the fields the feature consumes and preserve omitted or optional values at the wire boundary.
5. Follow returned `paging.next` or `paging.previous` links and respect the operation's `limit` and `offset` bounds.
6. For list-status writes, verify the exact method and allowed fields and send only supplied values as `application/x-www-form-urlencoded`.
7. Add a focused request or mapping test, then run the narrowest available check.

## Stop and Verify

- Stop before reusing one operation's authentication, page limit, or update fields for another operation.
- Treat error bodies and statuses together; verify absent-delete behavior before making retries idempotent.
- Never place client IDs, OAuth tokens, authorization headers, or credential-bearing URLs in logs, errors, source control, or committed fixtures.

## Validation

- Prove authentication selection, encoding, selected fields, pagination, and changed mapping behavior as applicable.
- Cover anime and manga separately when their list-state contracts differ.
- Run the owning package's focused test, then broader checks if shared transport or sync changes.
- Trakkin currently has no MyAnimeList provider package; do not claim repository-level provider validation until one exists.
