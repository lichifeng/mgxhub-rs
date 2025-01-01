use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Multipart, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Utc};
use clap::Parser;
use elasticsearch::{http::transport::Transport, Elasticsearch, IndexParts};
use flate2::{write::GzEncoder, Compression};
use serde_json::Value;
use sqlx::sqlite::SqlitePool;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use zip::{write::FileOptions, ZipWriter};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    /// Maximum file size in megabytes
    #[arg(long, default_value_t = 50)]
    max_size: usize,

    /// Port to listen on
    #[arg(long, default_value_t = 3000)]
    port: u16,

    /// Host address to bind to
    #[arg(long, default_value = "0.0.0.0")]
    host: String,

    /// Records storage directory
    #[arg(long, default_value = "records")]
    storage_dir: PathBuf,

    // Maps storage directory
    #[arg(long, default_value = "maps")]
    maps_dir: PathBuf,

    /// SQLite database path
    #[arg(long, default_value = "mgxhub.sqlite")]
    db_path: PathBuf,

    /// Elasticsearch URL
    #[arg(long, default_value = "http://192.168.200.11:9200")]
    es_url: String,

    /// Enable GZIP compression for JSON responses
    #[arg(long, default_value_t = false)]
    no_gzip: bool,
}

#[derive(Clone)]
struct AppState {
    max_size: usize,
    port: u16,
    host: String,
    storage_dir: PathBuf,
    maps_dir: PathBuf,
    db_pool: SqlitePool,
    es_client: Elasticsearch,
    gzip: bool,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    if config.max_size == 0 {
        eprintln!("Error: max-size must be greater than 0");
        std::process::exit(1);
    }

    // Create storage directory if it doesn't exist
    std::fs::create_dir_all(&config.storage_dir).unwrap();
    std::fs::create_dir_all(&config.maps_dir).unwrap();

    // Ensure database file exists
    if !config.db_path.exists() {
        // Create parent directories if they don't exist
        if let Some(parent) = config.db_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        // Create an empty database file
        File::create(&config.db_path).unwrap();
    }

    // Initialize SQLite
    let db_pool = SqlitePool::connect(&format!("sqlite:{}", config.db_path.display())).await.unwrap();

    // Create table if not exists
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            md5 TEXT UNIQUE NOT NULL,
            guid TEXT,
            duration INTEGER,
            haswinner BOOLEAN,
            hasai BOOLEAN,
            data TEXT NOT NULL
        )",
    )
    .execute(&db_pool)
    .await
    .unwrap();

    // Initialize Elasticsearch client
    let transport = Transport::single_node(&config.es_url).unwrap();
    let es_client = Elasticsearch::new(transport);

    let state = AppState {
        max_size: config.max_size,
        port: config.port,
        host: config.host.clone(),
        storage_dir: config.storage_dir,
        maps_dir: config.maps_dir,
        db_pool,
        es_client,
        gzip: !config.no_gzip,
    };

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/", post(handle_upload).layer(DefaultBodyLimit::max(state.max_size * 1024 * 1024)))
        .with_state(state.clone());

    // 创建关闭信号通道
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // 设置 ctrl-c 处理
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("\nReceived Ctrl+C, shutting down mgxhub...");
        let _ = tx.send(());
    });

    let addr = format!("{}:{}", state.host, state.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!("Mgxhub running on http://{}:{}", state.host, state.port);
    println!("├ Maximum upload size: {}MB", state.max_size);
    println!("├   Storage directory: {}", state.storage_dir.canonicalize().unwrap().to_string_lossy());
    println!("├      Maps directory: {}", state.maps_dir.canonicalize().unwrap().to_string_lossy());
    println!("├     SQLite database: {}", config.db_path.canonicalize().unwrap().to_string_lossy());
    println!("├   Elasticsearch URL: {}", config.es_url);
    println!("└        GZIP enabled: {}", state.gzip);
    println!("* Press Ctrl+C to stop the server");

    // 使用 axum::serve 的 with_graceful_shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            rx.await.ok();
        })
        .await
        .unwrap();

    println!("Server shutdown completed");
}

async fn handle_upload(State(state): State<AppState>, mut multipart: Multipart) -> impl IntoResponse {
    let mut recfile: Option<Bytes> = None;
    let mut recname: Option<String> = None;
    let mut lastmod: Option<u64> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("recfile") => {
                let filename = field.file_name().unwrap().to_string();
                // Check file extension
                let extension = filename.split('.').last().unwrap_or("").to_lowercase();
                if !["mgx", "mgx2", "mgz", "mgl"].contains(&extension.as_str()) {
                    return (StatusCode::UNSUPPORTED_MEDIA_TYPE, "Not an Age of Empires II record file")
                        .into_response();
                }

                let data = field.bytes().await.unwrap();
                if data.len() > (state.max_size * 1024 * 1024) as usize {
                    return (
                        StatusCode::PAYLOAD_TOO_LARGE,
                        format!("File size should be less than {}MB", state.max_size),
                    )
                        .into_response();
                }

                recname = Some(filename);
                recfile = Some(data);
            }
            Some("lastmod") => match field.text().await.unwrap().parse::<u64>() {
                Ok(time) => {
                    lastmod = Some(time);
                }
                Err(_) => {
                    return (StatusCode::BAD_REQUEST, "Invalid last modified time").into_response();
                }
            },
            _ => {}
        }
    }

    match (recfile, recname, lastmod) {
        (Some(data), Some(filename), Some(modified_time)) => {
            let now: DateTime<Utc> = Utc::now();
            let formatted_time = now.format("%Y-%m-%dT%H:%M:%S").to_string();

            println!(
                "[Info][{}] Uploaded: {}, size: {}, lastmod: {}",
                formatted_time,
                &filename,
                data.len(),
                modified_time
            );

            let mut record = mgx::Record::new(filename.clone(), data.len(), modified_time);
            let mut parser = mgx::Parser::new(data.clone()).unwrap();
            match parser.parse_to(&mut record) {
                Ok(_) => {}
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error parsing record: {}", e)).into_response();
                }
            }

            let mut json_value = serde_json::to_value(&record).unwrap();
            // Do some redaction
            if let Value::Object(ref mut map) = json_value {
                if let Some(ref raw_matchup) = record.matchup {
                    if let Some(matchup) = map.get_mut("matchup") {
                        let mut sorted_matchup = raw_matchup.clone();
                        sorted_matchup.sort();
                        *matchup = Value::String(
                            sorted_matchup.iter().map(|n| n.to_string()).collect::<Vec<String>>().join("v"),
                        );
                    }
                }
            }
            let json = serde_json::to_string(&json_value).unwrap();

            let response = if state.gzip {
                // Compress JSON with GZIP
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(json.as_bytes()).unwrap();
                let compressed_json = encoder.finish().unwrap();

                // Create response headers with GZIP encoding
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

                (StatusCode::OK, headers, compressed_json).into_response()
            } else {
                // Return uncompressed JSON
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());

                (StatusCode::OK, headers, json.clone()).into_response()
            };

            // Clone necessary values for async tasks
            let db_pool = state.db_pool.clone();
            let es_client = state.es_client.clone();
            let filename_clone = filename.clone();

            // Spawn async tasks
            tokio::spawn(async move {
                // 1. Create ZIP archive and map file
                let now: DateTime<Utc> = Utc::now();
                let formatted_time = now.format("%Y-%m-%dT%H:%M:%S").to_string();

                match record.guid {
                    Some(ref guid) => {
                        if guid.is_empty() {
                            ()
                        }
                        let map_path = state.maps_dir.clone().join(format!("{}.png", guid));
                        if map_path.exists() {
                            ()
                        }
                        match mgx::draw_map(&record, &parser, map_path.to_str().unwrap()) {
                            Ok(_) => {
                                #[cfg(debug_assertions)]
                                println!("[Info][{}] Saved: {:?}", formatted_time, &map_path);
                            }
                            Err(e) => {
                                eprintln!("[ Err][{}] Map: {}", formatted_time, e);
                            }
                        }
                    }
                    None => {}
                }

                let zip_path;
                if let Some(ref md5) = record.md5 {
                    if md5.is_empty() {
                        return;
                    }

                    zip_path = state.storage_dir.clone().join(format!("{}.zip", md5));
                    if zip_path.exists() {
                        return;
                    }
                } else {
                    return;
                }

                let file = File::create(&zip_path).unwrap();
                let mut zip = ZipWriter::new(file);
                zip.start_file(filename_clone, FileOptions::<()>::default()).unwrap();
                zip.write_all(&data).unwrap();

                match zip.finish() {
                    Ok(_) => {
                        #[cfg(debug_assertions)]
                        println!("[Info][{}] Saved: {:?}", formatted_time, &zip_path);
                    }
                    Err(e) => {
                        eprintln!("[ Err][{}] ZIP: {}", formatted_time, e);
                    }
                }
            });

            let json_value_arc = Arc::new(json_value);
            let es_json = Arc::clone(&json_value_arc);
            tokio::spawn(async move {
                // 2. Save to Elasticsearch
                let now: DateTime<Utc> = Utc::now();
                let formatted_time = now.format("%Y-%m-%dT%H:%M:%S").to_string();
                match es_client.index(IndexParts::Index("records_demo")).body(&es_json).send().await {
                    Ok(retval) => {
                        #[cfg(debug_assertions)]
                        println!("[Info][{}] Elasticsearch: {:?}", formatted_time, retval.status_code());
                    }
                    Err(e) => {
                        eprintln!("[Warn][{}] Elasticsearch: {}", formatted_time, e);
                    }
                }
            });

            let sql_json = Arc::clone(&json_value_arc);
            tokio::spawn(async move {
                // 3. Save to SQLite
                match sqlx::query("INSERT INTO records (md5, guid, haswinner, hasai, data) VALUES (?, ?, ?, ?, ?)")
                    .bind(&sql_json["md5"])
                    .bind(&sql_json["guid"])
                    .bind(&sql_json["haswinner"])
                    .bind(&sql_json["hasai"])
                    .bind(&json)
                    .execute(&db_pool)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        let now: DateTime<Utc> = Utc::now();
                        let formatted_time = now.format("%Y-%m-%dT%H:%M:%S").to_string();
                        eprintln!("[Warn][{}] SQLite: {}", formatted_time, e);
                    }
                }
            });

            response
        }
        _ => (StatusCode::BAD_REQUEST, "Missing required fields").into_response(),
    }
}
