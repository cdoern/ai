# Praxis → OGX: working notes

Personal reference. Companion to `ogx.yaml` in this directory, which is a
validated config that puts Praxis in front of a local OGX.

---

## 1. What Praxis is

A **programmable reverse proxy for AI traffic**. It sits between clients and
model backends, and everything it does is configured in YAML rather than
compiled per deployment.

The unit of behavior is a **filter**. A filter inspects or rewrites a request,
and filters run in order as a chain. Routing is itself just a filter near the
end of the chain.

Praxis AI is the AI-specific layer on top of [Praxis
core](https://github.com/praxis-proxy/praxis), which owns listeners, TLS, load
balancing, and health checks.

The consequence worth internalizing: **adding a capability means adding a filter
and naming it in a config**, not changing how the proxy works.

---

## 2. How a request moves

The mental model everything else hangs off:

```
client                POST /v1/responses hits the listener on :8080
  |
  v
classify              a filter reads the request and promotes facts to
                      internal headers: x-praxis-ai-format, x-praxis-ai-model
  |
  v
transform             optional filters rewrite the body: resolve file refs,
                      rehydrate history, inject credentials
  |
  v
router                matches path + those internal headers, picks a cluster
  |
  v
load_balancer         resolves the cluster to a real endpoint, forwards
```

This pattern has a name in the codebase: **classify → route → branch**.
Classifier filters promote facts to `x-praxis-ai-*` headers; the router matches
those headers.

That is why those headers must be proxy-owned and unspoofable — a client that
could set them could steer its own routing.

### The four hooks a filter can implement

| Hook | When |
|---|---|
| `on_request` | request head only — method, path, headers. No body. |
| `on_request_body` | request body bytes, streamed or buffered |
| `on_response` | response head from the backend |
| `on_response_body` | response body, including SSE chunks |

Each filter declares a `BodyMode`: `Stream` passes chunks through
incrementally, `StreamBuffer` collects up to a byte limit. Streaming filters
must never buffer a whole response — that rule is why SSE and WebSocket traffic
survive the proxy.

---

## 3. The crates

Dependencies flow strictly downward.

```
praxis-ai-proxy      the binary: loads config, registers filters, starts server
   |
praxis-ai-filters    A2A, MCP, guardrails, inference routing, token usage
   |
praxis-ai-apis       provider API types (OpenAI Responses/Conversations,
   |                 Anthropic Messages), classification, response storage,
   |                 SSE parsing   <-- the operation registry lives here
   |
praxis-filter        HttpFilter trait, pipeline, registry (upstream core)
```

---

## 4. Running it

Binary is `praxis-ai`, package is `praxis-ai-proxy`.

| Flag | Effect |
|---|---|
| `-c` / `--config` | path to YAML config (or set `PRAXIS_CONFIG`) |
| `-t` / `--validate` | validate and exit — no ports opened |
| `-T` / `--dump` | print effective config after defaults, then exit |

```console
# validate before binding anything
cargo run -p praxis-ai-proxy -- -c ogx.yaml -t

# serve
cargo run -p praxis-ai-proxy -- -c ogx.yaml

# watch every filter decide
RUST_LOG=debug cargo run -p praxis-ai-proxy -- -c ogx.yaml
```

**Start with `-t`.** Config errors are the most common failure, and validation
catches them without opening a socket or needing a backend alive.

---

## 5. Pointing at OGX

OGX listens on **:8321**. Target topology for RHOAI 3.6: Praxis is the only
public entrypoint, OGX is demoted to an internal backend reachable only by
Praxis.

`ogx.yaml` in this directory does that. Validated clean. Shape:

```yaml
listeners:
  - name: ai-gateway
    address: "127.0.0.1:8080"
    filter_chains: [ogx]

filter_chains:
  - name: ogx
    filters:
      # classify
      - filter: openai_responses_format
        on_invalid: continue
        headers:
          format: x-praxis-ai-format
          model: x-praxis-ai-model

      # route: pick a cluster by path
      - filter: router
        routes:
          - path_prefix: "/v1/vector_stores"
            cluster: ogx-vector-stores
          - path_prefix: "/v1/files"
            cluster: ogx-files
          - path_prefix: "/"
            cluster: ogx-inference

      # resolve clusters to endpoints
      - filter: load_balancer
        clusters:
          - name: ogx-vector-stores
            endpoints: ["127.0.0.1:8321"]
          - name: ogx-files
            endpoints: ["127.0.0.1:8321"]
          - name: ogx-inference
            endpoints: ["127.0.0.1:8321"]
```

All three clusters point at the same address today. Naming them separately now
means moving one to a different backend later is a cluster edit, not a routing
rewrite.

`path_prefix` matches on segment boundaries: `/v1/files` and
`/v1/files/file_1/content` match, `/v1/filesystem` does not.

### Verify

```console
curl http://127.0.0.1:8080/v1/models
curl http://127.0.0.1:8080/v1/vector_stores

# no OGX handy? fake one and watch the raw bytes arrive
while true; do printf 'HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}' | nc -l 8321; done
```

---

## 6. Forwarding vs delegating

The distinction that matters most for the migration, and easy to miss.

**Forwarding** — Praxis passes the request through to OGX and relays the answer.
One request in, one request out. Everything in section 5 is this.

**Delegating** — Praxis *pauses mid-request*, makes its own separate HTTP call
to OGX, uses the result to rewrite the body, then continues upstream. This is a
**callout**.

A callout is a second, distinct request with its own headers. That is why
callout filters carry their own URL, timeout, and header allowlist — and why
identity propagation across that hop is its own work item (RHAIENG-6607).

```yaml
- filter: openai_file_resolve
  files_api_url: "http://127.0.0.1:8321"
  # SSRF guard rejects loopback/private targets unless you opt in
  allow_private_files_api_url: true
  allow_pre_security_callout: true
  forward_headers:
    - authorization
  on_missing: reject
  timeout_ms: 10000
```

### Ownership per resource type

One authoritative backend each — this is what prevents dual-write.

| Resource | Owner in 3.6 |
|---|---|
| Files | OGX |
| Vector Stores | OGX |
| RAG retrieval | OGX |
| Responses runtime | Praxis |
| Conversations | Praxis |
| Auth, routing, policy | Praxis |

---

## 7. Where the operation registry fits

Back to section 2: the router picks a cluster from path and headers.
Historically that decision was **path-prefix matching** — fine when Praxis owned
one API, too blunt as a boundary for four.

Prefix matching cannot express three things the cutover needs:

- `PUT /v1/responses` is unsupported and should fall through to OGX — but a
  `/v1/responses` prefix rule captures it into the Praxis chain.
- `/v1/responses/input_tokens` is a real endpoint, not a response whose ID
  happens to be `input_tokens`.
- A multipart upload has a body Praxis must not buffer — which it needs to know
  *before* reading anything.

The operation registry replaces that guesswork with a declared table: every
endpoint states its method, path, family, transport, and body shape. The
classifier reads the request head, matches the table, and publishes the result
as proxy-owned headers the router can trust.

In one line: **it makes "is this request mine?" a precise question** — which is
exactly what the catch-all forward to OGX depends on.

### Issue map

| Issue | What |
|---|---|
| #746 | shared registry + matcher (merged, PR #772) |
| #743 | Responses operations registered (PR #788) |
| #744 | the `openai_operation` classifier filter |
| #742 | Conversations consumes the shared match |
| #745 | Files + Vector Stores registered for routing |
| #741 | consolidate Responses create request processing |

Dependency order: #746 → #744 → then #743 / #742 / #745 in parallel → #741 last.
