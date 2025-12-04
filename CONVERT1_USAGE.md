# Convert1 Binary Usage Guide

`convert1` is a migration tool for updating records created with earlier versions of mgxhub-rs.

## Features

- **SQLite Database Update**: Re-parses all record files from ZIP archives and updates the database with fresh metadata
- **Elasticsearch Re-indexing**: Creates a new Elasticsearch index and populates it with updated data
- **Migration Tracking**: Stores detailed results in a separate database for auditing
- **Flexible Execution**: Can run SQLite update, ES update, or both

## Building

```bash
cargo build --release --bin convert1
```

The binary will be located at `target/release/convert1.exe` (Windows) or `target/release/convert1` (Linux).

## Usage

### Update Both SQLite and Elasticsearch

```bash
convert1 --mode both
```

### Update Only SQLite

```bash
convert1 --mode sqlite
```

### Update Only Elasticsearch

```bash
convert1 --mode es
```

## Command-Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--record-dir` | `data/records` | Directory containing ZIP archives |
| `--db-path` | `data/mgxhub.sqlite` | Path to main SQLite database |
| `--results-db-path` | `data/convert1_results.sqlite` | Path to migration results database |
| `--es-url` | `http://127.0.0.1:9200` | Elasticsearch URL |
| `--es-index` | `records_demo` | Elasticsearch index name |
| `--es-user` | - | Elasticsearch username (optional) |
| `--es-pass` | - | Elasticsearch password (optional) |
| `--mode` | `both` | Execution mode: `sqlite`, `es`, or `both` |

All options can also be set via environment variables (uppercase, e.g., `RECORD_DIR`).

## Output Format

### SQLite Update

```
[<id>] updated. guid: <status>  |  duration: <status>  |  haswinner: <status>  |  hasai: <status>
```

- `<id>`: Record ID in database
- `<status>`: `-` (no change) or `✓` (changed)

Example:
```
[     1] updated. guid: -  |  duration: ✓  |  haswinner: -  |  hasai: -
[     2] FAILED: ZIP file not found: data/records/abc123.zip
[     3] updated. guid: ✓  |  duration: ✓  |  haswinner: ✓  |  hasai: -
```

### Elasticsearch Update

```
[<id>] indexed. md5: <hash>
```

Example:
```
[     1] indexed. md5: abc123def456
[     2] indexed. md5: 789xyz012abc
```

## Migration Process

### SQLite Update Steps

For each record in the database:

1. **Locate ZIP file**: Find `<md5>.zip` in records directory
2. **Extract record file**: Get first file from ZIP archive
3. **Parse with mgx**: Use current version of mgx parser
4. **Merge metadata**: Preserve `lastmod`, `filename`, and `created_at` from old data
5. **Detect changes**: Compare `guid`, `duration`, `haswinner`, `hasai` fields
6. **Update database**: Write new parsed data and updated fields
7. **Log results**: Record success/failure and change flags

### Elasticsearch Update Steps

1. **Create new index**: Use mapping from `es_mapping_ik.json` with version control disabled
2. **Index all records**: Insert every record from SQLite into new index
3. **Manual cleanup**: User creates aliases and deletes old index

## Migration Results Database

The tool creates a separate SQLite database to track migration results:

### Schema

```sql
CREATE TABLE conversion_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_id INTEGER NOT NULL,
    md5 TEXT NOT NULL,
    converted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    success BOOLEAN NOT NULL,
    error_message TEXT,
    guid_changed BOOLEAN NOT NULL,
    duration_changed BOOLEAN NOT NULL,
    haswinner_changed BOOLEAN NOT NULL,
    hasai_changed BOOLEAN NOT NULL
)
```

You can query this database to analyze the migration:

```sql
-- Count successful vs failed conversions
SELECT success, COUNT(*) FROM conversion_results GROUP BY success;

-- Find records where guid changed
SELECT record_id, md5 FROM conversion_results WHERE guid_changed = 1;

-- View all errors
SELECT record_id, error_message FROM conversion_results WHERE success = 0;
```

## Troubleshooting

### ZIP file not found

Ensure `--record-dir` points to the correct directory containing `.zip` archives.

### Failed to parse record

The record file may be corrupted or from an unsupported version. Check the error message in the results database.

### Elasticsearch index already exists

If running ES mode multiple times, either:
- Delete the old index manually: `curl -X DELETE http://localhost:9200/<index_name>`
- Use a different `--es-index` name

## Differences from Main Server

- **No version control**: Elasticsearch index is created without version control for better performance during bulk indexing
- **No background tasks**: All operations are synchronous and blocking
- **No map generation**: Only updates metadata, doesn't regenerate minimap PNGs
- **Preserves upload metadata**: Keeps original `lastmod`, `filename`, and `created_at` values

## Example Full Migration

```bash
# Step 1: Backup existing data
cp data/mgxhub.sqlite data/mgxhub.sqlite.backup

# Step 2: Run conversion (SQLite only first)
convert1 --mode sqlite

# Step 3: Check results
sqlite3 data/convert1_results.sqlite "SELECT COUNT(*) as total, SUM(success) as successful FROM conversion_results;"

# Step 4: If SQLite looks good, update Elasticsearch
convert1 --mode es --es-index records_v2

# Step 5: Test new index, then create alias and delete old index
curl -X POST "http://localhost:9200/_aliases" -H 'Content-Type: application/json' -d'
{
  "actions": [
    { "add": { "index": "records_v2", "alias": "records" } },
    { "remove_index": { "index": "records_demo" } }
  ]
}'
```
