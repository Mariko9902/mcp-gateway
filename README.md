# MCP Gateway

MCP Gateway exposes local MCP servers and remote gateways through HTTP. It supports direct JSON-RPC requests, server-sent events (SSE), and explicit sessions. Local servers communicate over standard input and output; remote routes use the gateway's HTTP session endpoints.

## Requirements

- Go 1.24 or later.
- An MCP server executable for each local route, or an accessible remote gateway for each HTTP route.

## Build and run

```bash
go build -o bin/mcp-gateway .
cp .env.example .env
./bin/mcp-gateway -routes ./routes.yaml -root /path/to/mcp
```

The server listens on `127.0.0.1:15410` by default. Set `MCP_GATEWAY_TOKEN` in `.env` or the process environment to require authentication. The example routes in `routes.yaml` refer to local installations and should be adjusted before use. The routes file is optional; pass `-routes` explicitly to load it.

Go's `flag` package accepts the following options:

| Flag | Default | Description |
| --- | --- | --- |
| `-listen` | `127.0.0.1:15410` | HTTP listen address. |
| `-root` | `~/Coding/mcp` | Root for local commands and server discovery. |
| `-routes` | Empty | Path to a YAML routes file. |
| `-state` | `./mcp-gateway.state.json` | Session state file. |
| `-idle` | `0` | Session idle timeout; zero uses `MCP_GATEWAY_IDLE` or its default. |

The `.env` file is loaded from the working directory without overriding environment variables that are already set. `MCP_GATEWAY_LISTEN`, `MCP_GATEWAY_ROOT`, `MCP_GATEWAY_ROUTES`, and `MCP_GATEWAY_STATE` in `.env.example` are examples for a start script; the executable reads the corresponding command-line flags directly.

## Routes

Define named local and HTTP routes in YAML:

```yaml
routes:
  local-example:
    type: local
    command: local-example/bin/local-example
    args: []
    working_dir: /path/to/local-example
    env_inherit: true
    env:
      LOG_LEVEL: info
    timeout: 30s
  remote-example:
    type: http
    url: http://127.0.0.1:15411/mcps/remote-example
    headers:
      X-API-Key: example-token
    timeout: 30s
```

For a local route, `command` is required; relative paths are resolved under `-root`. `args`, `working_dir`, and `env` are optional. The process inherits the gateway's environment unless `env_inherit` is `false`. For an HTTP route, `url` is required and points to the remote gateway's `/mcps/<name>` endpoint; `headers` are sent to that backend. A route's `timeout` is a Go duration such as `30s` or `2m`.

When a name is not configured, the gateway looks for `<root>/<name>/bin/<name>`. If that file is absent, it accepts the sole executable in `<root>/<name>/bin/`. `GET /mcps` lists configured and discovered servers. The configured list includes route environment values, so restrict access to this endpoint when they contain credentials.

Send `SIGHUP` to reload the configured routes file without restarting the gateway. Existing sessions continue to use their established backend.

## Authentication

When `MCP_GATEWAY_TOKEN` is set, provide it in either `X-API-Key` or `Authorization: Bearer <token>`. If it is empty, requests are not authenticated. `/healthz` is available without authentication.

```bash
export MCP_GATEWAY_TOKEN='example-token'
curl -s http://127.0.0.1:15410/healthz
curl -s -H "X-API-Key: $MCP_GATEWAY_TOKEN" http://127.0.0.1:15410/mcps
```

## HTTP API

All endpoints other than `/healthz` require authentication when a gateway token is configured. The name `session` is an alias for `session-mcp`.

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Health check. |
| `GET` | `/mcps` | List configured and discovered servers. |
| `POST` | `/mcps/<name>` | Send a JSON-RPC request. |
| `GET` | `/mcps/<name>` | Open the compatibility SSE stream. |
| `POST` | `/mcps/<name>/session` | Create a session and return `session_id`. |
| `GET` | `/mcps/<name>/session/<id>/events` | Receive session events over SSE. |
| `POST` | `/mcps/<name>/session/<id>/rpc` | Send JSON-RPC to a session; responses arrive on the event stream. |
| `DELETE` | `/mcps/<name>/session/<id>` | Close a session. |
| `GET`, `HEAD` | `/mcp-files/swarm-mcp/download?path=<path>` | Download a file from an allowed swarm directory; `/files/swarm-mcp/download` is also accepted. |

For example, list tools on a configured server:

```bash
curl -s -X POST 'http://127.0.0.1:15410/mcps/local-example' \
  -H "X-API-Key: $MCP_GATEWAY_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tools/list","params":{},"id":1}'
```

An explicit session can be created with `POST /mcps/<name>/session`. Open its `/events` stream, then send requests to `/rpc`; the RPC endpoint acknowledges receipt with `{"ok":true}` and the response is delivered on the stream. Close the session with `DELETE` when finished. The compatibility endpoint also accepts a session ID through `X-MCP-Session-ID`, the `session_id` query parameter, or its session cookie.

For direct `tools/list` requests, `Disabled-Tools` or `X-Disabled-Tools` can supply a comma-separated list. The gateway adds it as `params.disabledTools` if that parameter is absent; the backend determines how to use it.

The file download endpoint accepts paths beneath `issues`, `docs`, `trace`, `locks`, or `workers` in the swarm data root. The root is selected from `MCP_GATEWAY_SWARM_ROOT`, then `SWARM_MCP_ROOT`, then `~/.swarm-mcp`.

## Sessions and timeouts

`MCP_GATEWAY_IDLE` controls the idle timeout when `-idle` is not positive. `MCP_GATEWAY_DEFAULT_TIMEOUT` controls the default backend timeout. Both accept Go durations or a number of seconds and default to 24 hours when unset. Route-level `timeout` overrides the backend default. The supplied `.env.example` sets the backend timeout to one hour.

The gateway saves session metadata to the state file. HTTP sessions with remote session IDs are restored at startup; local processes are not restored. The state file can include backend headers and should be protected accordingly.

HTTP retries can be configured with `MCP_GATEWAY_HTTP_RETRY_MAX_ATTEMPTS`, `MCP_GATEWAY_HTTP_RETRY_BASE_DELAY`, and `MCP_GATEWAY_HTTP_RETRY_MAX_DELAY`. The same suffixes are available with the `MCP_GATEWAY_HTTP_EVENTS` and `MCP_GATEWAY_HTTP_FORWARD` prefixes for event streams and direct forwarding. Delay values are Go durations.

## Tests

```bash
go test ./...
```
