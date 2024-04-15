- [x] Add a DbOptions struct with a flush interval
- [x] Move memtables into immutable memtables every N milliseconds (if not empty)
- [x] Update flusher thread to notify listeners that write was durably persisted
- [ ] Update flusher thread to upload SSTs to object storage
- [ ] Add read methods to SsTable
- [ ] Update db.rs to use SsTable for reads

- Need to build a manifest on startup to know which SSTs to read
- Need to download files from object storage if a get points to an SST that's not on local disk
- Need to add Result<> to APIs errors and switch over to the anyhow crate