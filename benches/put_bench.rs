use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use lasagnedb::KB;
use rand::RngCore;
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Registry;

fn put_big_value(db: Arc<lasagnedb::Db>) {
    let mut rng = rand::thread_rng();
    let key = Bytes::from(format!("{:020}", rng.next_u64()));
    let value = BytesMut::zeroed(KB * 50).freeze();
    db.put(key, value).unwrap();
}

fn put_small_value(db: Arc<lasagnedb::Db>) {
    let mut rng = rand::thread_rng();
    let key = Bytes::from(format!("{:020}", rng.next_u64()));
    let value = Bytes::from(format!("{:020}", rng.next_u64()));
    db.put(key, value).unwrap();
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

fn criterion_benchmark(c: &mut Criterion) {
    setup();

    let tmp_dir = tempfile::tempdir().unwrap();
    let path = tmp_dir.path();
    println!("path: {}", path.to_str().unwrap());

    let db = Arc::new(lasagnedb::Db::open(path).unwrap());

    c.bench_function("put small value", |b| {
        b.iter(|| put_small_value(db.clone()))
    });
    c.bench_function("put big value", |b| b.iter(|| put_big_value(db.clone())));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
