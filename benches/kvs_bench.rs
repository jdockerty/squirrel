use criterion::{self, criterion_group, criterion_main};

use kvs::{KvStore, KvsEngine};

fn write_direct(c: &mut criterion::Criterion) {
    let store = KvStore::open(std::env::temp_dir()).expect("Open temp dir for KvStore");
    c.bench_function("write", move |b| {
        let key = "key".to_string();
        let value = "value".to_string();
        b.iter(|| {
            store.set(key.clone(), value.clone()).unwrap();
            assert_eq!(store.get(key.clone()).unwrap(), Some(value.clone()));
        })
    });
}

fn read_direct(c: &mut criterion::Criterion) {
    let store = KvStore::open(std::env::temp_dir()).expect("Open temp dir for KvStore");

    c.bench_function("read", move |b| {
        let mut counter = 0;
        b.iter(|| {
            assert_eq!(store.get(format!("{counter}")).unwrap(), None);
            counter += 1;
        })
    });
}

criterion_group!(benches, write_direct, read_direct);
criterion_main!(benches);
