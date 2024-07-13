use criterion::{self, black_box, criterion_group, criterion_main, BenchmarkId};

use sqrl::{KvStore, KvsEngine};
use tokio::runtime::Runtime;

fn write_direct(c: &mut criterion::Criterion) {
    let store = KvStore::open(std::env::temp_dir()).expect("Open temp dir for KvStore");
    let rt = Runtime::new().unwrap();

    c.bench_with_input(BenchmarkId::new("write", "store"), &store, |b, s| {
        b.to_async(&rt).iter(|| async {
            black_box(s.set("key".to_string(), "value".to_string()).await.unwrap());
        })
    });
}

fn read_direct(c: &mut criterion::Criterion) {
    let store = KvStore::open(std::env::temp_dir()).expect("Open temp dir for KvStore");
    let rt = Runtime::new().unwrap();

    c.bench_with_input(BenchmarkId::new("read", "store"), &store, |b, s| {
        b.to_async(&rt).iter(|| async {
            assert_eq!(black_box(s.get("value".to_string()).await.unwrap()), None);
        })
    });
}

criterion_group!(benches, write_direct, read_direct);
criterion_main!(benches);
