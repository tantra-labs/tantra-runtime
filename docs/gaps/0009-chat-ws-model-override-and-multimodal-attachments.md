# 0009 — `/ws/chat` WebSocket lacks native `model` override and multimodal image attachments

**Status**: open

## What's stubbed or hardcoded

`crates/tantra-runtime-api-rest/src/chat.rs`'s `ChatClientMessage` (line 221) accepts only `{ "text": "..." }`, and `ChatRuntime::from_env` resolves a single fixed `LlmProvider` at server startup from environment variables (`TANTRA_GGUF_PATH` or `TANTRA_LLM_BASE_URL` / `TANTRA_LLM_MODEL`).

As a result:
1. Per-request model selection from `tantra-ui-web` (`Auto`, `Claude 3.5 Sonnet`, `GPT-4o`, `SmolLM2-360M`, `Llama-3.2-3B`) cannot switch the active provider/model natively at turn time; `/api/chat` serializes the user's preferred model tag into the text prompt instead.
2. Chart crop snapshots (`Capture area for Quant`) and uploaded screenshots produce base64 PNG/image data URLs in the web client, but `/ws/chat` has no image/vision attachment schema (`attachments?: { mimeType, data }[]`), so `/api/chat` strips raw `dataUrl` bytes and forwards metadata + symbol context as text.

## Why

The v1 chat WebSocket bridge was built around `tantra-agent-chat`'s text-only turn API and a single configured `LlmProvider` instance. Supporting per-turn model routing and multimodal vision parts requires extending `ChatClientMessage` and `tantra-agent-llm` message parts.

## How to close it

1. Extend `ChatClientMessage` in `crates/tantra-runtime-api-rest/src/chat.rs` with optional `model: Option<String>` and `attachments: Option<Vec<ChatAttachmentInput>>` fields (supporting `image/png` / `image/jpeg` base64 payloads).
2. Route per-turn model overrides cleanly when `model` differs from server default (or return an explicit capability notice if the requested model is unavailable locally).
3. Pass image attachment parts through `tantra-agent-chat` / `tantra-agent-llm` when invoking vision-capable providers.
