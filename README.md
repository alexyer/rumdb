[![TestStatus](https://github.com/alexyer/rumdb/actions/workflows/rust_test.yml/badge.svg?event=push)](https://github.com/alexyer/rumdb/actions)
[![Crate](https://img.shields.io/crates/v/rumdb.svg)](https://crates.io/crates/rumdb)
[![API](https://docs.rs/rumdb/badge.svg)](https://docs.rs/rumdb)

# RumDB
Blazing fast log-structured key-value storage based on Bitcask[1] whitepaper.

## Features
- Low latency for reads and writes
- High throughput
- Easy to backup / restore
- Simple and easy to understand
- Store data much larger than the RAM

## Roadmap
- [x] Disk storage with hash map keydir structure
- [x] GET/PUT/REMOVE operations
- [x] Log files rotation
- [ ] Compaction and garbage collection
- [ ] Hint files for the faster startup time.
- [ ] Internal cache.
- [ ] Alternative storage implementations (e.g. tree-based to support range scans)

## References
[1] [Bitcask: A Log-Structured Hash Table for Fast Key/Value Data](https://riak.com/assets/bitcask-intro.pdf)