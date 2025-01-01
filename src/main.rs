use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Multipart},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Router,
};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        .route("/", post(handle_upload).layer(DefaultBodyLimit::max(50 * 1024 * 1000)));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    println!("Server running on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}

async fn handle_upload(mut multipart: Multipart) -> impl IntoResponse {
    let mut recfile: Option<Bytes> = None;
    let mut recname: Option<String> = None;
    let mut lastmod: Option<u64> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("recfile") => {
                recname = Some(field.file_name().unwrap().to_string());
                println!("Received file: {:?}", recname);
                recfile = Some(field.bytes().await.unwrap());
            }
            Some("lastmod") => {
                lastmod = Some(field.text().await.unwrap().parse::<u64>().unwrap());
            }
            _ => {}
        }
    }

    match (recfile, recname, lastmod) {
        (Some(data), Some(filename), Some(modified_time)) => {
            // 这里可以处理 data
            println!("Received file size: {} bytes", data.len());
            println!("Last modified: {:?}", modified_time);

            let mut record = mgx::Record::new(filename, data.len(), modified_time);
            let mut parser = mgx::Parser::new(data).unwrap();
            parser.parse_to(&mut record).unwrap();
            println!("    guid: {:?}", record.guid);
            println!("     ver: {:?}", record.ver);
            println!("duration: {:?}", record.duration);

            (StatusCode::OK, "File received successfully").into_response()
        }
        _ => (StatusCode::BAD_REQUEST, "Missing required fields").into_response(),
    }
}
