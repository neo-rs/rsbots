# Instorebotforwarder Commands Reference

## Overview
Instorebotforwarder scans configured source channels for Amazon links (including links inside embeds and link buttons), expands/normalizes them, optionally enriches via Amazon PA-API, routes into output buckets, and renders RS-style embeds using JSON templates from `config.json`.

## Slash Commands

#### `/testallmessage`
- **Description**: Diagnose a message: show scan surfaces (content/embeds/components), URLs found, Amazon detection (ASIN + which URL), expansion result, enrichment result, routing decision (destination channel), and which embed template key would be used. Includes an embed preview (no send to routing channels).
- **Parameters**:
  - `channel` (optional): Channel to inspect (defaults to the current channel)
  - `message_id` (optional): Message ID to inspect (defaults to the most recent message in `channel`)
- **Admin Only**: No (but requires permission to view the target channel/message)
- **Returns**: Ephemeral diagnostic output + embed preview

#### `/embedbuild list`
- **Description**: List which template routes currently have templates set and which config key they map to.
- **Admin Only**: Yes (administrator in a server)
- **Returns**: Ephemeral list output

#### `/embedbuild edit`
- **Description**: Edit a template via a Discord modal by pasting JSON. Saves to `Instorebotforwarder/config.json` only.
- **Parameters**:
  - `route`: `personal`, `grocery`, `deals`, `default`, `enrich_failed`
- **Admin Only**: Yes (administrator in a server)
- **Returns**: Ephemeral success/failure message

#### `/embedbuild preview`
- **Description**: Render a template preview (ephemeral) using placeholder/enriched context.
- **Parameters**:
  - `route`: `personal`, `grocery`, `deals`, `default`, `enrich_failed`
  - `asin`: Optional ASIN (defaults to `startup_test_asin`)
- **Admin Only**: Yes (administrator in a server)
- **Returns**: Ephemeral embed preview + debug header

## Command Summary
- **Total Commands**: 4
- **Admin Commands**: 3 (`/embedbuild list`, `/embedbuild edit`, `/embedbuild preview`)
- **Public Commands**: 1

