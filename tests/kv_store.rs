use rand::Rng;
use sqrl::{KvStore, KvsEngine, Result};
use std::{collections::HashMap, sync::Arc};
use tempfile::TempDir;
use tokio::sync::Barrier;
use walkdir::WalkDir;

// Should get previously stored value
#[tokio::test]
async fn get_stored_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    store.set("key2".to_owned(), "value2".to_owned()).await?;

    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    assert_eq!(
        store.get("key2".to_owned()).await?,
        Some("value2".to_owned())
    );

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    assert_eq!(
        store.get("key2".to_owned()).await?,
        Some("value2".to_owned())
    );

    Ok(())
}

// Should overwrite existent value
#[tokio::test]
async fn overwrite_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value1".to_owned())
    );
    store.set("key1".to_owned(), "value2".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value2".to_owned())
    );

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value2".to_owned())
    );
    store.set("key1".to_owned(), "value3".to_owned()).await?;
    assert_eq!(
        store.get("key1".to_owned()).await?,
        Some("value3".to_owned())
    );

    Ok(())
}

// Should get `None` when getting a non-existent key
#[tokio::test]
async fn get_non_existent_value() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert_eq!(store.get("key2".to_owned()).await?, None);

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    assert_eq!(store.get("key2".to_owned()).await?, None);

    Ok(())
}

#[tokio::test]
async fn remove_non_existent_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    assert!(store.remove("key1".to_owned()).await.is_err());
    Ok(())
}

#[tokio::test]
async fn remove_key() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    store.set("key1".to_owned(), "value1".to_owned()).await?;
    assert!(store.remove("key1".to_owned()).await.is_ok());
    assert_eq!(store.get("key1".to_owned()).await?, None);
    Ok(())
}

// Insert data until total size of the directory decreases.
// Test data correctness after compaction.
#[tokio::test]
async fn compaction() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    let dir_size = || {
        let entries = WalkDir::new(temp_dir.path()).into_iter();
        let len: walkdir::Result<u64> = entries
            .map(|res| {
                res.and_then(|entry| entry.metadata())
                    .map(|metadata| metadata.len())
            })
            .sum();
        len.expect("fail to get directory size")
    };

    let mut current_size = dir_size();
    for iter in 0..1000 {
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            let value = format!("{}", iter);
            store.set(key, value).await?;
        }

        let new_size = dir_size();
        if new_size > current_size {
            current_size = new_size;
            continue;
        }
        // Compaction triggered

        drop(store);
        // reopen and check content
        let store = KvStore::open(temp_dir.path())?;
        for key_id in 0..1000 {
            let key = format!("key{}", key_id);
            assert_eq!(store.get(key).await?, Some(format!("{}", iter)));
        }
        return Ok(());
    }

    panic!("No compaction detected");
}

// Ensure that we can conduct random operations and retrieve the correct values.
// As opposed to always setting sequential keys and values.
#[tokio::test]
async fn randomised_retrieval() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;

    let mut value_tracker = HashMap::new();
    let mut rng = rand::thread_rng();
    for i in 0..1000 {
        let key = format!("key{}", i);
        let value = format!("value{}", i);

        // Always set some random keys on every iteration
        for _ in 0..100 {
            store
                .set(key.clone(), format!("value{}", rng.gen::<i32>()))
                .await?;
        }

        if rng.gen::<usize>() % 2 == 0 {
            store.set(key.clone(), value.clone()).await?;
            value_tracker.insert(key.clone(), value.clone());
        } else {
            match store.remove(key.clone()).await {
                Ok(_) => {
                    value_tracker.remove(&key);
                }
                Err(sqrl::KvStoreError::RemoveOperationWithNoKey) => continue,
                Err(e) => return Err(e),
            }
        }
    }
    drop(store);

    let store = KvStore::open(temp_dir.path())?;

    for (k, v) in value_tracker {
        assert_eq!(store.get(k).await?, Some(v));
    }

    Ok(())
}

#[tokio::test]
async fn concurrent_set() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    let barrier = Arc::new(Barrier::new(1001));
    for i in 0..1000 {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            store
                .set(format!("key{}", i), format!("value{}", i))
                .await
                .unwrap();
            barrier.wait().await;
        });
    }
    barrier.wait().await;

    for i in 0..1000 {
        assert_eq!(
            store.get(format!("key{}", i)).await.unwrap(),
            Some(format!("value{}", i))
        );
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    for i in 0..1000 {
        assert_eq!(
            store.get(format!("key{}", i)).await.unwrap(),
            Some(format!("value{}", i))
        );
    }

    Ok(())
}

#[tokio::test]
async fn concurrent_get() -> Result<()> {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let store = KvStore::open(temp_dir.path())?;
    for i in 0..100 {
        store
            .set(format!("key{}", i), format!("value{}", i))
            .await
            .unwrap();
    }

    let mut handles = Vec::new();
    for thread_id in 0..100 {
        let store = store.clone();
        let handle = tokio::task::spawn(async move {
            for i in 0..100 {
                let key_id = (i + thread_id) % 100;
                assert_eq!(
                    store.get(format!("key{}", key_id)).await.unwrap(),
                    Some(format!("value{}", key_id))
                );
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }

    // Open from disk again and check persistent data
    drop(store);
    let store = KvStore::open(temp_dir.path())?;
    let mut handles = Vec::new();
    for thread_id in 0..100 {
        let store = store.clone();
        let handle = tokio::task::spawn(async move {
            for i in 0..100 {
                let key_id = (i + thread_id) % 100;
                assert_eq!(
                    store.get(format!("key{}", key_id)).await.unwrap(),
                    Some(format!("value{}", key_id))
                );
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }

    Ok(())
}
