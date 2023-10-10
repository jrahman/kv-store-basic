use std::path::PathBuf;

use criterion::{criterion_group, criterion_main, Criterion};
use kvs::engines::{KvStore, SledKvStore, KvsEngine};

use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::SeedableRng;

fn kv_store(c: &mut Criterion) {

    let _ = std::fs::remove_dir_all("./logs");
    let _ = std::fs::create_dir("./logs");

    let mut rng = StdRng::seed_from_u64(1234);

    println!("Opening sled");
    let mut kv_store = KvStore::open(None, PathBuf::from("./logs")).unwrap();

    println!("Creating keys");
    let mut keys: Vec<String> = Vec::new();
    let dist = Alphanumeric{};

    for _ in 0..4096 {
        keys.push(dist.sample_string(&mut rng, 16));
    }

    println!("Creating values");
    let mut values: Vec<String> = Vec::new();
    for _ in 0..4096 {
        values.push(dist.sample_string(&mut rng, 128));
    }

    let mut i = 0;

    println!("Benchmarking writes");
    c.bench_function("kv_write", |b| {
	    b.iter(|| {
            i %= keys.len();
		    kv_store.set(keys.get(i).unwrap().clone(), values.get(i).unwrap().clone()).unwrap();
            i += 1;
		});
	});

    println!("Benchmarking reads");
    c.bench_function("kv_read", |b| {
	    b.iter(|| {
            i %= keys.len();
		    kv_store.get(keys.get(i).unwrap().clone()).unwrap();
            i += 1;
		});
	});
}

fn sled_store(c: &mut Criterion) {

    let _ = std::fs::remove_dir_all("./logs");
    let _ = std::fs::create_dir("./logs");

    let mut rng = StdRng::seed_from_u64(1234);

    println!("Opening sled");
    let mut sled_store = SledKvStore::open("./logs").unwrap();

    println!("Creating keys");
    let mut keys: Vec<String> = Vec::new();
    let dist = Alphanumeric{};

    for _ in 0..4096 {
        keys.push(dist.sample_string(&mut rng, 16))
    }

    println!("Creating values");
    let mut values: Vec<String> = Vec::new();
    for _ in 0..4096 {
        values.push(dist.sample_string(&mut rng, 128))
    }

    let mut i = 0;

    println!("Benchmarking writes");
    c.bench_function("sled_write", |b| {
	    b.iter(|| {
            i %= keys.len();
		    sled_store.set(keys.get(i).unwrap().clone(), values.get(i).unwrap().clone()).unwrap();
            i += 1;
		});
	});

    println!("Benchmarking reads");
    c.bench_function("sled_read", |b| {
	    b.iter(|| {
            i %= keys.len();
		    sled_store.get(keys.get(i).unwrap().clone()).unwrap();
            i += 1;
		});
	});
}


criterion_group!(benches, sled_store, kv_store);
criterion_main!(benches);