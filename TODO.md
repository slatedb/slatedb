- [x] Add a DbOptions struct with a flush interval
- [x] Move memtables into immutable memtables every N milliseconds (if not empty)
- [x] Update flusher thread to notify listeners that write was durably persisted
- [ ] Make a flusher thread to write immutable memtables to disk as an SST
- [ ] Update flusher thread to upload SSTs to object storage

- Need to build a manifest on startup to know which SSTs to read
- Need to download files from object storage if a get points to an SST that's not on local disk