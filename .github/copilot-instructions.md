# mgxhub-rs: Age of Empires II Record API Server

## Architecture Overview

Single-file Rust web server (`src/main.rs`, ~560 lines) that parses Age of Empires II game records and stores metadata in dual databases. Built with Axum for HTTP, using the `mgx` crate for record parsing.

**Data Flow:**
1. Client uploads `.mgx/.mgx2/.mgz/.mgl` file via POST multipart form
2. `mgx::Parser` extracts game metadata (players, civs, matchup, duration)
3. Three async tasks fire-and-forget to: save ZIP archive, index to Elasticsearch, store in SQLite
4. Returns parsed JSON (GZIP compressed by default)

**Key Dependencies:**
- `mgx = "0.1"` - Game record parsing (external crate, not in this repo)
- `axum = "0.8"` - Web framework with multipart upload support
- `elasticsearch = "8.17"` - Search indexing for game metadata
- `sqlx = "0.8"` - SQLite for persistent storage

## Configuration

All settings via CLI flags or environment variables (clap parser):
- `--max-size=50` / `MAX_SIZE` - Upload limit in MB
- `--port=3000` / `PORT`, `--host=0.0.0.0` / `HOST` - Server binding
- `--record-dir=data/records` / `RECORD_DIR` - ZIP archive storage
- `--map-dir=data/maps` / `MAP_DIR` - Generated minimap PNGs
- `--es-url`, `--es-index`, `--es-user`, `--es-pass` - Elasticsearch config
- `--no-gzip` - Disable response compression

## Critical Implementation Details

### File Upload Handler (`handle_upload`)
- Accepts only `.mgx/.mgx2/.mgz/.mgl` extensions (HD/DE versions rejected per README)
- Requires two form fields: `recfile` (binary) and `lastmod` (Unix timestamp in ms)
- Validates lastmod range: 1999-01-01 (AoE2 release) to now
- IP extraction respects `X-Forwarded-For` header for proxy deployments

### Async Task Pattern
Three tokio::spawn tasks run concurrently after response:
1. **Storage:** Create ZIP with DOS timestamp, include metadata in archive comment
2. **Elasticsearch:** Index with `op_type=Create` (idempotent, ignores 409 conflicts)
3. **SQLite:** Insert with duplicate key handling (error code 2067 = constraint violation)

All tasks use `Arc<serde_json::Value>` to share parsed data efficiently.

### Data Transformations
- `record.convert_encoding()` + `record.translate("zh")` - Localize to Chinese
- Matchup array → sorted string format (e.g., `[3,2,1]` → `"1v2v3"`)
- Map generation via `mgx::draw_map()` only if GUID exists and map not cached

### Elasticsearch Setup
- Auto-creates index on startup if missing using `es_mapping_ik.json`
- Uses IK analyzer for Chinese text search (player names, chat)
- Document ID = record MD5 (ensures uniqueness)

### Graceful Shutdown
Ctrl+C signal handled via oneshot channel to allow axum's `with_graceful_shutdown` to drain in-flight requests.

## Build & Development

**Prerequisites (Linux):**
```bash
apt install libssl-dev pkg-config
```

**Build:**
```bash
cargo build --release
```

**Run with custom config:**
```bash
cargo run -- --port 8080 --max-size 100 --es-url http://localhost:9200
```

**Docker:**
Multi-stage build with minimal debian:bookworm-slim runtime. Binary is `./mgxhub` in `/mgxhub` workdir.

## Code Style

Project uses custom rustfmt config (`rustfmt.toml`):
- `max_width = 120` - Wider lines than default
- `fn_single_line = true` - Short functions on one line
- `trailing_comma = "Never"` - No trailing commas
- `use_small_heuristics = "Max"` - Aggressive formatting compression

## Common Patterns

**Error handling:** Mostly `.unwrap()` - service expects properly configured environment or fails fast at startup.

**Logging:** Manual `println!` with timestamps via `get_current_time()`. Format: `[Info][timestamp] message` or `[ Err][timestamp] message`.

**Response headers:** Manually construct `HeaderMap` for GZIP and CORS (tower-http/cors middleware used for preflight).

## Testing Notes

No test suite currently. Manual testing requires:
- Running Elasticsearch instance
- Sample `.mgx` files (not included, HD/DE versions unsupported)
- Client that sends multipart/form-data with `recfile` and `lastmod` fields
