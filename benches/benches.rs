use criterion::{criterion_group, criterion_main, Criterion};
use kvs::KvStore;
use kvs::KvsEngine;
use once_cell::sync::Lazy;
use rand::prelude::*;
use tempfile::TempDir;

static SEED_VALUES: Lazy<Vec<String>> = Lazy::new(|| {
    let mut seeds = Vec::new();
    for _ in 0..=100 {
        seeds.push(rand::thread_rng().gen_range(0, 100000).to_string());
    }
    seeds
});

// NOTE: These benchmarks are likely not very accurate, but simply serve as a method
// to get the feel of performing them with criterion.
pub fn kvs(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let mut kvs = KvStore::open(temp_dir.path()).unwrap();

    c.bench_function("write_kvs", |b| {
        b.iter(|| {
            for (i, seed) in SEED_VALUES.iter().enumerate() {
                let key = i.to_string();
                let value = seed.to_string();
                kvs.set(key, value).unwrap();
            }
        })
    });

    c.bench_function("read_kvs", |b| {
        b.iter(|| {
            for (i, value) in SEED_VALUES.iter().enumerate() {
                let v = kvs.get(i.to_string()).unwrap();
                assert_eq!(v.unwrap(), *value);
            }
        })
    });
}

pub fn sled(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();

    let sled_store = sled::open(temp_dir.path()).unwrap();

    c.bench_function("write_sled", |b| {
        b.iter(|| {
            for (i, seed) in SEED_VALUES.iter().enumerate() {
                let key = i.to_string();
                let value = seed.to_string();
                sled_store.insert(key, value.as_bytes()).unwrap();
            }
        })
    });

    c.bench_function("read_sled", |b| {
        b.iter(|| {
            for (i, value) in SEED_VALUES.iter().enumerate() {
                let v = sled_store.get(i.to_string()).unwrap();
                assert_eq!(v.unwrap(), value);
            }
        })
    });
}

criterion_group!(benches, kvs, sled);
criterion_main!(benches);
