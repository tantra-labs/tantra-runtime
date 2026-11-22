# 0014 — `/ws/chat` structured trace streaming and per-caller thread persistence

**Status**: open

## What's stubbed or hardcoded
Consumer: `tantra-ui-web` ([`AgentPane.tsx`](../../../tantra-ui-web/src/components/organisms/agent/AgentPane.tsx), [`chat-threads-store.ts`](../../../tantra-ui-web/src/lib/chat-threads-store.ts), [`src/app/api/chat/route.ts`](../../../tantra-ui-web/src/app/api/chat/route.ts)) under root plan `0020`:
1. **No structured reasoning / telemetry / raw-payload events on `/ws/chat`**:
   - Currently `/ws/chat` streams text deltas, `tool_call`, `tool_call_update` (`scenes`), and terminal messages. It does not emit:
     - `thought_delta` (`{ type: "thought_delta", text }`) for live step reasoning;
     - `token_usage` (`{ type: "token_usage", totalTokens, promptTokens?, completionTokens? }`) from the underlying LLM response;
     - `raw_input` / `raw_output` JSON payloads on `tool_call` / `tool_call_update` frames.
   - `AgentPane.tsx` falls back cleanly using character-ratio token heuristics (`estimateTokens` in `src/lib/agent-trace.ts`) and tool-argument summaries, and forward-compatibly consumes those optional fields once emitted.
2. **No server-side multi-thread chat persistence (`/api/v2/chat/threads`)**:
   - Runtime persists a single session keyed by `(caller_id, workspace)`, with no HTTP CRUD API for listing, creating, switching, archiving, or deleting multiple named conversation threads per workspace.
   - `tantra-ui-web` currently stores up to 25 threads per workspace in browser `localStorage` via `src/lib/chat-threads-store.ts`.

## Why
Root plan `0020` was constrained strictly to `tantra-ui-web/`. Adding native `thought_delta` / `token_usage` / `raw_input` / `raw_output` frames requires wiring upstream agent events from `tantra-agent/docs/gaps/0010`, and multi-thread storage requires `tantra-runtime-persistence` + OpenAPI v2 routes scoped by `Caller`.

## How to close it
1. Forward `thought_delta`, `token_usage`, `raw_input`, and `raw_output` (from `tantra-agent` gap `0010`) through `tantra-runtime-api-rest::chat` `/ws/chat` wire frames.
2. Add `(caller_id, workspace_id, thread_id)` thread persistence in `tantra-runtime-persistence` and expose caller-scoped `/api/v2/chat/threads` (`GET`, `POST`, `PATCH /{id}`, `DELETE /{id}`) in `tantra-runtime-api-rest`.
3. Update `schema/openapi.v2.json` and switch `tantra-ui-web/src/lib/chat-threads-store.ts` from `localStorage` fallback to server sync.
