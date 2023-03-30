use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder};
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use lasagnedb::{Db, StorageIterator};
use serde::Deserialize;
use std::ops::Bound;
use std::sync::Arc;
use tempfile::TempDir;
use tracing::{instrument, trace};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

struct ServerState {
    temp_dir: Arc<TempDir>,
    db: Arc<lasagnedb::Db>,
}

#[derive(Deserialize, Debug)]
struct KeyRequest {
    key: String,
}

#[derive(Deserialize, Debug)]
struct ScanRequest {
    key: String,
    count: u32,
}

#[get("/info")]
async fn info(state: web::Data<ServerState>) -> actix_web::Result<impl Responder> {
    Ok(HttpResponse::Ok().body(format!("{:?}", state.db)))
}

#[instrument(skip(state))]
#[get("/get")]
async fn get(
    state: web::Data<ServerState>,
    query: web::Query<KeyRequest>,
) -> actix_web::Result<impl Responder> {
    let key = Bytes::from(query.into_inner().key);
    let data = state.db.get(&key).unwrap();
    Ok(match data {
        None => HttpResponse::Ok().body(vec![]),
        Some(_data) => HttpResponse::Ok().body(_data),
    })
}

#[instrument(skip(state))]
#[get("/scan")]
async fn scan(state: web::Data<ServerState>, query: web::Query<ScanRequest>) -> impl Responder {
    let mut limit = query.count;
    let key = Bytes::from(query.into_inner().key);

    let mut iter = state
        .db
        .scan(Bound::Included(key), Bound::Unbounded)
        .unwrap();
    let mut result = BytesMut::new();
    while iter.is_valid() && limit > 0 {
        let _val = iter.value();
        result.put_u64_le(_val.len() as u64);
        result.extend(_val);
        limit -= 1;
        iter.next().unwrap();
    }
    let _result = result.freeze();
    HttpResponse::Ok().body(_result)
}

#[instrument(skip(state, payload))]
#[post("/put")]
async fn put(
    state: web::Data<ServerState>,
    query: web::Query<KeyRequest>,
    mut payload: web::Payload,
) -> impl Responder {
    let key = Bytes::from(query.into_inner().key);
    let mut value = BytesMut::new();
    while let Some(item) = payload.next().await {
        value.extend_from_slice(item.unwrap().as_ref())
    }
    state.db.put(key, value.freeze()).unwrap();
    HttpResponse::Ok().finish()
}

#[instrument(skip(state))]
#[post("/del")]
async fn del(state: web::Data<ServerState>, query: web::Query<KeyRequest>) -> impl Responder {
    let key = Bytes::from(query.into_inner().key);
    state.db.delete(key).unwrap();
    HttpResponse::Ok().finish()
}

fn setup() {
    if let Some(jaeger_endpoint) = option_env!("JAEGER_ENDPOINT") {
        println!("JAEGER_ENDPOINT: {}", jaeger_endpoint);
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(jaeger_endpoint)
            .with_service_name("lasagnedb")
            .install_simple()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = Registry::default().with(opentelemetry);
        tracing::subscriber::set_global_default(subscriber).unwrap();
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    setup();

    let temp_dir = Arc::new(tempfile::tempdir().unwrap());
    let db = Arc::new(Db::open(temp_dir.clone().path()).unwrap());

    let _temp_dir = temp_dir.clone();
    let _db = db.clone();
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(ServerState {
                temp_dir: _temp_dir.clone(),
                db: _db.clone(),
            }))
            .service(get)
            .service(scan)
            .service(put)
            .service(del)
            .service(info)
    })
    .bind(("0.0.0.0", 8080))?
    .run()
    .await
}
