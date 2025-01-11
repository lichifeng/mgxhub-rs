use axum::{
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Multipart, State},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use chrono::{DateTime, Datelike, Timelike, Utc};
use clap::Parser;
use elasticsearch::{
    auth::Credentials,
    http::transport::{SingleNodeConnectionPool, Transport, TransportBuilder},
    Elasticsearch, IndexParts,
};
use flate2::{write::GzEncoder, Compression};
use serde_json::Value;
use sqlx::sqlite::SqlitePool;
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use url::Url;
use zip::{write::FileOptions, ZipWriter};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Config {
    /// Maximum file size in megabytes
    #[arg(long, default_value_t = 50, env)]
    max_size: usize,

    /// Port to listen on
    #[arg(long, default_value_t = 3000, env)]
    port: u16,

    /// Host address to bind to
    #[arg(long, default_value = "0.0.0.0", env)]
    host: String,

    /// Records storage directory
    #[arg(long, default_value = "records", env)]
    record_dir: PathBuf,

    /// Maps storage directory
    #[arg(long, default_value = "maps", env)]
    map_dir: PathBuf,

    /// SQLite database path
    #[arg(long, default_value = "mgxhub.sqlite", env)]
    db_path: PathBuf,

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

    /// Enable GZIP compression for JSON responses
    #[arg(long, default_value_t = false, env)]
    no_gzip: bool,
}

#[derive(Clone)]
struct AppState {
    max_size: usize,
    port: u16,
    host: String,
    record_dir: PathBuf,
    map_dir: PathBuf,
    db_pool: SqlitePool,
    es_client: Elasticsearch,
    es_index: String,
    gzip: bool,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();
    if config.max_size == 0 {
        eprintln!("[ Err] max-size must be greater than 0");
        std::process::exit(1);
    }

    // Create storage directory if it doesn't exist
    std::fs::create_dir_all(&config.record_dir).unwrap();
    std::fs::create_dir_all(&config.map_dir).unwrap();

    // Ensure database file exists
    if !config.db_path.exists() {
        // Create parent directories if they don't exist
        if let Some(parent) = config.db_path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        // Create an empty database file
        File::create(&config.db_path).unwrap();

        println!("[Info] DB file not exists, create: {}", config.db_path.display());
    }

    // Initialize SQLite
    let db_pool = SqlitePool::connect(&format!("sqlite:{}", config.db_path.display())).await.unwrap();

    // Create table if not exists
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            uploader TEXT,
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

    // Initialize Elasticsearch client with optional credentials
    let transport = if let (Some(user), Some(pass)) = (config.es_user, config.es_pass) {
        transport_with_cred(&config.es_url, Credentials::Basic(user, pass)).unwrap()
    } else {
        Transport::single_node(&config.es_url).unwrap()
    };
    let es_client = Elasticsearch::new(transport);

    // Check if index exists
    match es_client
        .indices()
        .exists(elasticsearch::indices::IndicesExistsParts::Index(&[&config.es_index]))
        .send()
        .await
    {
        Ok(index_exists) => {
            if !index_exists.status_code().is_success() {
                // Read mapping from file
                let mapping = include_str!("../es_mapping_ik.json");
                let mapping: serde_json::Value =
                    serde_json::from_str(&mapping).expect("Failed to parse es_mapping.json");

                match es_client
                    .indices()
                    .create(elasticsearch::indices::IndicesCreateParts::Index(&config.es_index))
                    .body(mapping) // Use the mapping from file
                    .send()
                    .await
                {
                    Ok(rep) => {
                        if rep.status_code().is_success() {
                            println!("[Info] Created ES index: {} with mapping", config.es_index);
                        } else {
                            eprintln!("[ Err] Failed to create ES index: {:?}", rep);
                            std::process::exit(1);
                        }
                    }
                    Err(e) => {
                        eprintln!("[ Err] Failed to create ES index: {}", e);
                        std::process::exit(1);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("[ Err] Failed to check ES index: {}", e);
            std::process::exit(1);
        }
    }

    let state = AppState {
        max_size: config.max_size,
        port: config.port,
        host: config.host,
        record_dir: config.record_dir,
        map_dir: config.map_dir,
        db_pool,
        es_client,
        es_index: config.es_index,
        gzip: !config.no_gzip,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any) // Allow any origin
        .allow_methods([axum::http::Method::GET, axum::http::Method::POST]) // Allow GET and POST methods
        .allow_headers(Any); // Allow any headers

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/", post(handle_upload).layer(DefaultBodyLimit::max(state.max_size * 1024 * 1024)))
        .with_state(state.clone())
        .layer(cors);

    // 处理 ctrl-c 信号
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        println!("\nReceived Ctrl+C, shutting down mgxhub...");
        let _ = tx.send(());
    });

    let addr = format!("{}:{}", state.host, state.port);
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    println!(" [{}] Mgxhub running on http://{}:{}", get_current_time(), state.host, state.port);
    println!("├ Maximum upload size: {}MB", state.max_size);
    println!("├   Storage directory: {}", state.record_dir.canonicalize().unwrap().to_string_lossy());
    println!("├      Maps directory: {}", state.map_dir.canonicalize().unwrap().to_string_lossy());
    println!("├     SQLite database: {}", config.db_path.canonicalize().unwrap().to_string_lossy());
    println!("├   Elasticsearch URL: {}", config.es_url);
    println!("└        GZIP enabled: {}", state.gzip);
    println!("* Press Ctrl+C to stop the server");

    // 使用 axum::serve 的 with_graceful_shutdown
    axum::serve(listener, app.into_make_service_with_connect_info::<SocketAddr>())
        .with_graceful_shutdown(async {
            rx.await.ok();
        })
        .await
        .unwrap();

    println!("Mgxhub server has stopped.");
}

#[axum::debug_handler]
async fn handle_upload(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut recfile: Option<Bytes> = None;
    let mut recname: Option<String> = None;
    let mut lastmod: Option<u128> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("recfile") => {
                let filename = field.file_name().unwrap().to_string();
                // Check file extension
                let extension = filename.split('.').last().unwrap_or("").to_lowercase();
                if !["mgx", "mgx2", "mgz", "mgl"].contains(&extension.as_str()) {
                    return (
                        StatusCode::UNSUPPORTED_MEDIA_TYPE,
                        format!("Not an Age of Empires II record file: {} / {}", filename, extension),
                    )
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
            Some("lastmod") => match field.text().await.unwrap().parse::<u128>() {
                Ok(time) => {
                    // Validate time range (1999-01-01 to now)
                    let current_time = chrono::Utc::now().timestamp_millis() as u128;
                    let min_time = 915148800000; // 1999-01-01 00:00:00 UTC, age of empires 2 release date
                    if time < min_time || time > current_time {
                        lastmod = Some(current_time);
                    } else {
                        lastmod = Some(time);
                    }
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
            println!(
                "[Info][{}] {} uploaded: {}, size: {}, lastmod: {}",
                get_current_time(),
                addr.ip(),
                &filename,
                data.len(),
                modified_time
            );

            let mut record = mgx::Record::new(filename.clone(), data.len(), modified_time);
            let mut parser = mgx::Parser::new(data.clone()).unwrap();
            match parser.parse_to(&mut record) {
                Ok(_) => {
                    record.convert_encoding();
                    record.translate("zh");
                }
                Err(e) => {
                    return (StatusCode::INTERNAL_SERVER_ERROR, format!("Error parsing record: {}", e)).into_response();
                }
            }

            let mut json_value = serde_json::to_value(&record).unwrap();

            // Convert matchup array to string and other modifications
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
            json_value["created_at"] = Value::String(get_current_time());

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
            let filename_clone = filename.clone();
            let json_value_arc = Arc::new(json_value);

            // Spawn async tasks
            let storage_json = json_value_arc.clone();
            tokio::spawn(async move {
                // 1. Create ZIP archive and map file
                match record.guid {
                    Some(ref guid) => {
                        if guid.is_empty() {
                            ()
                        } else {
                            let map_path = state.map_dir.join(format!("{}.png", guid));
                            if map_path.exists() {
                                ()
                            } else {
                                match mgx::draw_map(&record, &parser, map_path.to_str().unwrap()) {
                                    Ok(_) => {
                                        #[cfg(debug_assertions)]
                                        println!("[Info][{}] Map saved: {:?}", get_current_time(), &map_path);
                                    }
                                    Err(e) => {
                                        eprintln!("[ Err][{}] Map: {}", get_current_time(), e);
                                    }
                                }
                            }
                        }
                    }
                    None => (),
                }

                let zip_path;
                if let Some(ref md5) = record.md5 {
                    if md5.is_empty() {
                        return;
                    }

                    zip_path = state.record_dir.join(format!("{}.zip", md5));
                    if zip_path.exists() {
                        println!(
                            "[Info][{}] {} uploaded existing file {} with name: {} lastmod: {}",
                            get_current_time(),
                            addr.ip(),
                            &md5,
                            &filename_clone,
                            modified_time
                        );
                        return;
                    }
                } else {
                    return;
                }

                let file = File::create(&zip_path).unwrap();
                let mut zip = ZipWriter::new(file);

                // Convert Unix timestamp to DOS date/time
                let dt =
                    chrono::DateTime::from_timestamp(modified_time as i64, 0).unwrap_or_else(|| chrono::Utc::now());
                let dos_time = zip::DateTime::from_date_and_time(
                    dt.year() as u16,
                    dt.month() as u8,
                    dt.day() as u8,
                    dt.hour() as u8,
                    dt.minute() as u8,
                    dt.second() as u8,
                )
                .unwrap_or_default();

                let ver = storage_json["ver"].as_str().unwrap_or("unknown");
                let matchup = storage_json["matchup"].as_str().unwrap_or("unknown");
                let guid = storage_json["guid"].as_str().unwrap_or("00000");
                let md5 = storage_json["md5"].as_str().unwrap_or("00000");
                let recorder = storage_json["recorder"].as_i64().unwrap_or(0);

                // Add a comment file with record info
                let comment = format!(
                    "\nAn Age of Empires II record file\n\nVersion: {}\nMatchup: {}\n\nG U I D: {}\nM  D  5: {}\n\nCollected by aocrec.com",
                    ver, matchup, guid, md5
                );
                zip.set_comment(comment);

                // Add record file with modified time
                let options = FileOptions::<()>::default().last_modified_time(dos_time);
                let filename = format!(
                    "{}_{}_{}p_{}.{}",
                    ver,
                    matchup,
                    recorder,
                    &guid[..5],
                    filename_clone.split('.').last().unwrap_or_default()
                );
                zip.start_file(filename, options).unwrap();
                zip.write_all(&data).unwrap();

                match zip.finish() {
                    Ok(_) => {
                        println!("[Info][{}] Zip saved: {:?}", get_current_time(), &zip_path);
                    }
                    Err(e) => {
                        eprintln!("[ Err][{}] ZIP: {}", get_current_time(), e);
                    }
                }
            });

            let es_json = json_value_arc.clone();
            tokio::spawn(async move {
                // 2. Save to Elasticsearch
                match state
                    .es_client
                    .index(IndexParts::IndexId(&state.es_index, es_json["md5"].as_str().unwrap()))
                    .body(&es_json)
                    .send()
                    .await
                {
                    Ok(retval) => {
                        if retval.status_code().is_success() {
                            #[cfg(debug_assertions)]
                            println!(
                                "[Info][{}] ESDocMD5: {} status: {:?}",
                                get_current_time(),
                                es_json["md5"],
                                retval.status_code()
                            );
                        } else {
                            eprintln!(
                                "[Warn][{}] ESDocMD5: {} status: {:?} ",
                                get_current_time(),
                                es_json["md5"],
                                retval.status_code()
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("[Warn][{}] ESDocMD5: {} status: {}", get_current_time(), es_json["md5"], e);
                    }
                }
            });

            let sql_json = json_value_arc.clone();
            tokio::spawn(async move {
                // 3. Save to SQLite
                match sqlx::query(
                    "INSERT INTO records (uploader, md5, guid, haswinner, hasai, data) VALUES (?, ?, ?, ?, ?, ?)",
                )
                .bind(addr.ip().to_string())
                .bind(&sql_json["md5"])
                .bind(&sql_json["guid"])
                .bind(&sql_json["haswinner"])
                .bind(&sql_json["hasai"])
                .bind(&json)
                .execute(&state.db_pool)
                .await
                {
                    Ok(_) => {}
                    Err(e) => match e.as_database_error() {
                        Some(db_err) => {
                            if db_err.code().unwrap_or_else(|| std::borrow::Cow::Borrowed("")) == "2067" {} else {
                                eprintln!("[ Err][{}] SQLite: {}", get_current_time(), e);
                            }
                        }
                        _ => {
                            eprintln!("[ Err][{}] SQLite: {}", get_current_time(), e);
                        }
                    }
                }
            });

            response
        }
        _ => (StatusCode::BAD_REQUEST, "Missing required fields").into_response(),
    }
}

fn get_current_time() -> String {
    let now: DateTime<Utc> = Utc::now();
    now.format("%Y-%m-%dT%H:%M:%S").to_string()
}

pub fn transport_with_cred(url: &str, cred: Credentials) -> Option<Transport> {
    let u = Url::parse(url).unwrap();

    let conn_pool = SingleNodeConnectionPool::new(u);
    let transport_builder = TransportBuilder::new(conn_pool);

    transport_builder.auth(cred).build().ok()
}
