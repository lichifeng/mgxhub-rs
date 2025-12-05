use axum::body::Bytes;
use chrono::{DateTime, Utc};
use clap::Parser;
use elasticsearch::{
    auth::Credentials,
    http::transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
    Elasticsearch, IndexParts,
};
use serde_json::Value;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use url::Url;

#[derive(Parser)]
#[command(author, version, about = "Convert and update records from earlier version", long_about = None)]
struct Config {
    /// Records storage directory
    #[arg(long, default_value = "data/records", env)]
    record_dir: PathBuf,

    /// SQLite database path (source)
    #[arg(long, default_value = "data/mgxhub.sqlite", env)]
    db_path: PathBuf,

    /// New SQLite database path (destination)
    #[arg(long, default_value = "data/mgxhub_new.sqlite", env)]
    new_db_path: PathBuf,

    /// Migration results database path
    #[arg(long, default_value = "data/convert1_results.sqlite", env)]
    results_db_path: PathBuf,

    /// Skip already processed records
    #[arg(long, default_value_t = false, env)]
    skip: bool,

    /// Elasticsearch URL
    #[arg(long, default_value = "http://127.0.0.1:9200", env)]
    es_url: String,

    /// Elasticsearch index name
    #[arg(long, default_value = "records_demo", env)]
    es_index: String,

    /// Elasticsearch username
    #[arg(long, env)]
    es_user: Option<String>,

    /// Elasticsearch password
    #[arg(long, env)]
    es_pass: Option<String>,

    /// Mode: sqlite, es, or both
    #[arg(long, default_value = "sqlite", env)]
    mode: String,

    /// Number of concurrent threads for Elasticsearch indexing
    #[arg(long, default_value_t = 4, env)]
    es_threads: usize,
}

#[derive(Debug, Clone)]
struct RecordRow {
    id: i64,
    md5: String,
    guid: Option<String>,
    duration: Option<i64>,
    haswinner: Option<String>,
    hasai: Option<String>,
    uploader: Option<String>,
    created_at: Option<String>,
    data: String,
}

#[derive(Debug)]
struct UpdateResult {
    record_id: i64,
    md5: String,
    success: bool,
    error_message: Option<String>,
    guid_changed: bool,
    duration_changed: bool,
    haswinner_changed: bool,
    hasai_changed: bool,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    // Validate mode
    if !["sqlite", "es", "both"].contains(&config.mode.as_str()) {
        eprintln!("[ Err] Invalid mode: {}. Must be 'sqlite', 'es', or 'both'", config.mode);
        std::process::exit(1);
    }

    println!("[Info][{}] Convert1 starting...", get_current_time());
    println!("├ Mode: {}", config.mode);
    println!("├ Skip: {}", config.skip);
    println!("├ Records directory: {}", config.record_dir.display());
    println!("├ Source database: {}", config.db_path.display());
    println!("├ New database: {}", config.new_db_path.display());
    println!("└ Results database: {}", config.results_db_path.display());

    // Initialize source SQLite
    let db_pool = SqlitePool::connect(&format!("sqlite:{}", config.db_path.display())).await.unwrap();

    // Initialize new database
    if !config.new_db_path.exists() {
        if let Some(parent) = config.new_db_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        File::create(&config.new_db_path).unwrap();
    }

    let new_db_pool = SqlitePool::connect(&format!("sqlite:{}", config.new_db_path.display())).await.unwrap();

    // Create new database table with parser and deleted fields
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            uploader TEXT,
            md5 TEXT UNIQUE NOT NULL,
            guid TEXT,
            parser TEXT,
            duration INTEGER,
            haswinner BOOLEAN,
            hasai BOOLEAN,
            deleted INTEGER DEFAULT 0,
            data TEXT NOT NULL
        )",
    )
    .execute(&new_db_pool)
    .await
    .unwrap();

    // Initialize results database
    if !config.results_db_path.exists() {
        if let Some(parent) = config.results_db_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        File::create(&config.results_db_path).unwrap();
    }

    let results_db_pool =
        SqlitePool::connect(&format!("sqlite:{}", config.results_db_path.display())).await.unwrap();

    // Create results table
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS conversion_results (
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
        )",
    )
    .execute(&results_db_pool)
    .await
    .unwrap();

    // Update SQLite if mode is sqlite or both
    if config.mode == "sqlite" || config.mode == "both" {
        println!("\n[Info][{}] Starting SQLite conversion...", get_current_time());
        update_sqlite(&config, &db_pool, &new_db_pool, &results_db_pool).await;
    }

    // Update Elasticsearch if mode is es or both
    if config.mode == "es" || config.mode == "both" {
        println!("\n[Info][{}] Starting Elasticsearch update...", get_current_time());
        update_elasticsearch(&config, &new_db_pool).await;
    }

    println!("\n\n[Info][{}] Convert1 completed!", get_current_time());
}

async fn update_sqlite(config: &Config, db_pool: &SqlitePool, new_db_pool: &SqlitePool, results_db_pool: &SqlitePool) {
    // Get all records from source database
    let rows = sqlx::query("SELECT id, md5, guid, duration, haswinner, hasai, uploader, created_at, data FROM records ORDER BY id")
        .fetch_all(db_pool)
        .await
        .unwrap();

    println!("[Info][{}] Found {} records to process", get_current_time(), rows.len());

    // Build a set of already processed md5s if skip is enabled
    let mut processed_md5s = std::collections::HashSet::new();
    if config.skip {
        if let Ok(existing_rows) = sqlx::query("SELECT md5 FROM records")
            .fetch_all(new_db_pool)
            .await
        {
            for row in existing_rows {
                if let Ok(md5) = row.try_get::<String, _>("md5") {
                    processed_md5s.insert(md5);
                }
            }
            println!("[Info][{}] Found {} already processed records, will skip them", get_current_time(), processed_md5s.len());
        }
    }

    for row in rows {
        let id: i64 = row.get("id");
        let md5: String = row.get("md5");
        
        // Skip if already processed
        if config.skip && processed_md5s.contains(&md5.trim_matches('"').to_string()) {
            print!("\r[{:>6}] skipped (already processed)", id);
            std::io::Write::flush(&mut std::io::stdout()).ok();
            continue;
        }
        let guid: Option<String> = row.get("guid");
        let duration: Option<i64> = row.get("duration");
        
        // Handle haswinner and hasai which could be TEXT or INTEGER
        let haswinner: Option<String> = row.try_get::<Option<String>, _>("haswinner")
            .or_else(|_| row.try_get::<Option<i64>, _>("haswinner").map(|v| v.map(|i| i.to_string())))
            .ok()
            .flatten();
        let hasai: Option<String> = row.try_get::<Option<String>, _>("hasai")
            .or_else(|_| row.try_get::<Option<i64>, _>("hasai").map(|v| v.map(|i| i.to_string())))
            .ok()
            .flatten();
        
        let uploader: Option<String> = row.try_get("uploader").ok();
        let created_at: Option<String> = row.try_get("created_at").ok();
        let data: String = row.get("data");
        
        let old_record = RecordRow { id, md5: md5.clone(), guid, duration, haswinner, hasai, uploader, created_at, data };

        let (result, new_data) = process_record(config, &old_record).await;

        // Print result
        let guid_mark = if result.guid_changed { "✓" } else { "-" };
        let duration_mark = if result.duration_changed { "✓" } else { "-" };
        let haswinner_mark = if result.haswinner_changed { "✓" } else { "-" };
        let hasai_mark = if result.hasai_changed { "✓" } else { "-" };

        if result.success {
            print!(
                "\r[{:>6}] updated. guid: {}  |  duration: {}  |  haswinner: {}  |  hasai: {}",
                result.record_id, guid_mark, duration_mark, haswinner_mark, hasai_mark
            );
            std::io::Write::flush(&mut std::io::stdout()).ok();

            if let Some(new_data_str) = new_data {
                // Parse the new data to extract updated fields
                if let Ok(new_json) = serde_json::from_str::<Value>(&new_data_str) {
                    let new_md5 = new_json.get("md5").and_then(|v| v.as_str());
                    let new_guid = new_json.get("guid").and_then(|v| v.as_str());
                    let new_parser = new_json.get("parser").and_then(|v| v.as_str());
                    let new_duration = new_json.get("duration").and_then(|v| v.as_i64());
                    let new_haswinner = new_json.get("haswinner").and_then(|v| v.as_bool());
                    let new_hasai = new_json.get("include_ai").and_then(|v| v.as_bool());

                    // Insert into new database
                    sqlx::query(
                        "INSERT INTO records (uploader, md5, guid, parser, duration, haswinner, hasai, created_at, data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    )
                    .bind(&old_record.uploader)
                    .bind(new_md5)
                    .bind(new_guid)
                    .bind(new_parser)
                    .bind(new_duration)
                    .bind(new_haswinner)
                    .bind(new_hasai)
                    .bind(&old_record.created_at)
                    .bind(&new_data_str)
                    .execute(new_db_pool)
                    .await
                    .ok();
                }
            }
        } else {
            println!(
                "\n[{:>6}] FAILED: {}",
                result.record_id,
                result.error_message.as_deref().unwrap_or("Unknown error")
            );
        }

        // Save result to results database
        sqlx::query(
            "INSERT INTO conversion_results (record_id, md5, success, error_message, guid_changed, duration_changed, haswinner_changed, hasai_changed) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(result.record_id)
        .bind(&result.md5)
        .bind(result.success)
        .bind(&result.error_message)
        .bind(result.guid_changed)
        .bind(result.duration_changed)
        .bind(result.haswinner_changed)
        .bind(result.hasai_changed)
        .execute(results_db_pool)
        .await
        .ok();
    }
}

async fn process_record(config: &Config, old_record: &RecordRow) -> (UpdateResult, Option<String>) {
    let mut result = UpdateResult {
        record_id: old_record.id,
        md5: old_record.md5.clone(),
        success: false,
        error_message: None,
        guid_changed: false,
        duration_changed: false,
        haswinner_changed: false,
        hasai_changed: false,
    };

    // Step 1: Locate and read ZIP file
    // Strip quotes from md5 if present (in case it's stored with quotes)
    let clean_md5 = old_record.md5.trim_matches('"');
    let zip_path = config.record_dir.join(format!("{}.zip", clean_md5));
    if !zip_path.exists() {
        result.error_message = Some(format!("ZIP file not found: {}", zip_path.display()));
        return (result, None);
    }

    // Step 2: Open ZIP and get first file
    let zip_file = match File::open(&zip_path) {
        Ok(f) => f,
        Err(e) => {
            result.error_message = Some(format!("Failed to open ZIP: {}", e));
            return (result, None);
        }
    };

    let mut archive = match zip::ZipArchive::new(zip_file) {
        Ok(a) => a,
        Err(e) => {
            result.error_message = Some(format!("Failed to read ZIP: {}", e));
            return (result, None);
        }
    };

    if archive.len() == 0 {
        result.error_message = Some("ZIP file is empty".to_string());
        return (result, None);
    }

    let mut first_file = match archive.by_index(0) {
        Ok(f) => f,
        Err(e) => {
            result.error_message = Some(format!("Failed to get first file from ZIP: {}", e));
            return (result, None);
        }
    };

    let filename = first_file.name().to_string();
    let file_size = first_file.size() as usize;

    let mut file_data = Vec::new();
    if let Err(e) = first_file.read_to_end(&mut file_data) {
        result.error_message = Some(format!("Failed to read file from ZIP: {}", e));
        return (result, None);
    }

    // Step 3: Parse the record file
    let mut record = mgx::Record::new(filename, file_size, 0);
    let bytes_data = Bytes::from(file_data);
    let mut parser = match mgx::Parser::new(bytes_data) {
        Ok(p) => p,
        Err(e) => {
            result.error_message = Some(format!("Failed to create parser: {}", e));
            return (result, None);
        }
    };

    if let Err(e) = parser.parse_to(&mut record) {
        result.error_message = Some(format!("Failed to parse record: {}", e));
        return (result, None);
    }

    record.convert_encoding();
    record.translate("zh");

    let mut new_json = match serde_json::to_value(&record) {
        Ok(v) => v,
        Err(e) => {
            result.error_message = Some(format!("Failed to serialize new record: {}", e));
            return (result, None);
        }
    };

    // Convert matchup array to string
    if let Value::Object(ref mut map) = new_json {
        if let Some(ref raw_matchup) = record.matchup {
            if let Some(matchup) = map.get_mut("matchup") {
                let mut sorted_matchup = raw_matchup.clone();
                sorted_matchup.sort();
                *matchup =
                    Value::String(sorted_matchup.iter().map(|n| n.to_string()).collect::<Vec<String>>().join("v"));
            }
        }
    }

    // Step 4: Parse old data JSON and extract lastmod and filename
    let old_json: Value = match serde_json::from_str(&old_record.data) {
        Ok(v) => v,
        Err(e) => {
            result.error_message = Some(format!("Failed to parse old data JSON: {}", e));
            return (result, None);
        }
    };

    // Step 5: Replace lastmod and filename in new JSON
    if let Some(lastmod) = old_json.get("lastmod") {
        new_json["lastmod"] = lastmod.clone();
    }
    if let Some(filename) = old_json.get("filename") {
        new_json["filename"] = filename.clone();
    }

    // Also preserve created_at if it exists
    if let Some(created_at) = old_json.get("created_at") {
        new_json["created_at"] = created_at.clone();
    }

    // Step 6: Compare and detect changes
    let new_guid = new_json.get("guid").and_then(|v| v.as_str()).map(|s| s.to_string());
    let new_duration = new_json.get("duration").and_then(|v| v.as_i64());
    let new_haswinner = new_json.get("haswinner").and_then(|v| v.as_bool());
    let new_hasai = new_json.get("include_ai").and_then(|v| v.as_bool());

    // Parse old string values to bool (handles "true"/"false", "0"/"1", or integer strings)
    let old_haswinner_bool = old_record.haswinner.as_ref().and_then(|s| parse_bool_from_string(s));
    let old_hasai_bool = old_record.hasai.as_ref().and_then(|s| parse_bool_from_string(s));

    // Strip quotes from old guid for comparison
    let old_guid_clean = old_record.guid.as_ref().map(|s| s.trim_matches('"').to_string());

    result.guid_changed = old_guid_clean != new_guid;
    result.duration_changed = old_record.duration != new_duration;
    result.haswinner_changed = old_haswinner_bool != new_haswinner;
    result.hasai_changed = old_hasai_bool != new_hasai;

    // Step 7: Update the record data (store as string in result)
    let new_data_str = match serde_json::to_string(&new_json) {
        Ok(s) => s,
        Err(e) => {
            result.error_message = Some(format!("Failed to serialize updated JSON: {}", e));
            return (result, None);
        }
    };

    result.success = true;
    (result, Some(new_data_str))
}

async fn update_elasticsearch(config: &Config, db_pool: &SqlitePool) {
    // Initialize Elasticsearch client
    let transport = if let (Some(user), Some(pass)) = (&config.es_user, &config.es_pass) {
        transport_with_cred(&config.es_url, Credentials::Basic(user.clone(), pass.clone())).unwrap()
    } else {
        Transport::single_node(&config.es_url).unwrap()
    };
    let es_client = Elasticsearch::new(transport);

    // Read mapping from file
    let mapping = include_str!("../../es_mapping_ik.json");
    let mapping: serde_json::Value = serde_json::from_str(&mapping).expect("Failed to parse es_mapping_ik.json");

    // Create new index with version control disabled
    let index_body = serde_json::json!({
        "mappings": mapping["mappings"].clone(),
        "settings": {
            "index": {
                "refresh_interval": "5s"
            }
        }
    });

    println!("[Info][{}] Creating Elasticsearch index: {}", get_current_time(), config.es_index);

    match es_client
        .indices()
        .create(elasticsearch::indices::IndicesCreateParts::Index(&config.es_index))
        .body(index_body)
        .send()
        .await
    {
        Ok(rep) => {
            if rep.status_code().is_success() {
                println!("[Info][{}] Created ES index: {}", get_current_time(), config.es_index);
            } else {
                eprintln!("[ Err][{}] Failed to create ES index: {:?}", get_current_time(), rep.status_code());
                if rep.status_code().as_u16() == 400 {
                    println!("[Info][{}] Index may already exist, continuing...", get_current_time());
                } else {
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("[ Err][{}] Failed to create ES index: {}", get_current_time(), e);
            std::process::exit(1);
        }
    }

    // Get all records from database
    let records = sqlx::query_as::<_, (i64, String, String)>("SELECT id, md5, data FROM records ORDER BY id")
        .fetch_all(db_pool)
        .await
        .unwrap();

    println!("[Info][{}] Found {} records to index with {} threads", get_current_time(), records.len(), config.es_threads);

    // Use Arc to share ES client across threads
    let es_client = std::sync::Arc::new(es_client);
    let es_index = std::sync::Arc::new(config.es_index.clone());
    
    // Counters for success and error
    let success_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let error_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let processed_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let total_records = records.len();

    // Split records into chunks for parallel processing
    let chunk_size = (records.len() + config.es_threads - 1) / config.es_threads;
    let mut tasks = vec![];

    for chunk in records.chunks(chunk_size) {
        let chunk_records = chunk.to_vec();
        let es_client = es_client.clone();
        let es_index = es_index.clone();
        let success_count = success_count.clone();
        let error_count = error_count.clone();
        let processed_count = processed_count.clone();

        let task = tokio::spawn(async move {
            for (id, md5, data) in chunk_records {
                let json_value: Value = match serde_json::from_str(&data) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("\n[{:>6}] FAILED to parse JSON: {}", id, e);
                        error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        continue;
                    }
                };

                match es_client.index(IndexParts::IndexId(&es_index, &md5)).body(&json_value).send().await {
                    Ok(retval) => {
                        if retval.status_code().is_success() {
                            success_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        } else {
                            eprintln!("\n[{:>6}] FAILED with status: {:?}", id, retval.status_code());
                            error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("\n[{:>6}] FAILED: {}", id, e);
                        error_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                
                let current = processed_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                print!("\r[{:>6}/{:>6}] indexing... md5: {}", current, total_records, md5);
                std::io::Write::flush(&mut std::io::stdout()).ok();
            }
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    for task in tasks {
        task.await.ok();
    }

    let final_success = success_count.load(std::sync::atomic::Ordering::Relaxed);
    let final_error = error_count.load(std::sync::atomic::Ordering::Relaxed);

    println!(
        "\n[Info][{}] Elasticsearch indexing completed. Success: {}, Errors: {}",
        get_current_time(),
        final_success,
        final_error
    );
}

fn get_current_time() -> String {
    let now: DateTime<Utc> = Utc::now();
    now.format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn parse_bool_from_string(s: &str) -> Option<bool> {
    let trimmed = s.trim();
    // Try parsing as integer first (0/1)
    if let Ok(num) = trimmed.parse::<i64>() {
        return Some(num != 0);
    }
    // Try parsing as boolean string
    match trimmed.to_lowercase().as_str() {
        "true" | "yes" => Some(true),
        "false" | "no" => Some(false),
        _ => None,
    }
}

pub fn transport_with_cred(url: &str, cred: Credentials) -> Option<Transport> {
    let u = Url::parse(url).unwrap();
    let conn_pool = SingleNodeConnectionPool::new(u);
    let transport_builder = TransportBuilder::new(conn_pool);
    transport_builder.auth(cred).build().ok()
}
