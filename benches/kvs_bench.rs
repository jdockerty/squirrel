use criterion::{self, criterion_group, criterion_main};

use kvs::{KvStore, KvsEngine};

fn write_direct(c: &mut criterion::Criterion) {
    let store = KvStore::open(std::env::temp_dir()).expect("Open temp dir for KvStore");
    c.bench_function("write", move |b| {
        let mut counter = 0;
        b.iter(|| {
            store
                .set("key".to_string(), format!("{}", counter))
                .unwrap();
            assert_eq!(
                store.get("key".to_string()).unwrap(),
                Some(format!("{}", counter))
            );
            counter += 1;
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
