use rumdb::{DbOptions, RumDb, storage::Storage};

fn main() {
    env_logger::init();

    let opts = DbOptions::default().max_log_file_size(10);

    let mut db = RumDb::open("/tmp/display_stats.rumdb/", opts).unwrap();

    db.put(b"hello".to_vec(), b"world".to_vec()).unwrap();
    assert_eq!(db.get(b"hello").unwrap(), Some(b"world".to_vec()));

    db.remove(b"hello").unwrap();
    assert_eq!(db.get(b"hello").unwrap(), None);

    println!("{}", db.storage_stats());
}
