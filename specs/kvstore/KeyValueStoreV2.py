from enum import Enum
from typing import List, Tuple, Optional

class DurabilityLevel(Enum):
    MEMORY = 0
    REMOTE = 2

class SlateDb:
    def __init__(self):
        # Use list for unified storage
        self.wal_buffer: List[Tuple[str, str, int]] = []
        self.flushed_wal: List[Tuple[str, str, int]] = []
        self.memtable: List[Tuple[str, str, int]] = []
        self.immutable_memtable: List[Tuple[str, str, int]] = []
        self.l0: List[List[Tuple[str, str, int]]] = [] 

        # Key watermarks
        self.last_committed_seq = 0
        self.last_remote_persisted_seq = 0
        self.next_seq = 1
    
    def put(self, key: str, value: str, sync):
        """Write data"""
        seq = self.next_seq
        self.next_seq += 1
        
        # Add to WAL
        self.wal_buffer.append((key, value, seq))

        # Update memtable
        self.memtable.append((key, value, seq))

        # await commit
        if sync == DurabilityLevel.REMOTE:
            self.flush_wal()

        # update last_committed_seq after flush
        self.last_committed_seq = seq

    def get(self, key: str, dirty: bool, durability_filter: DurabilityLevel) -> Optional[str]:
        """Read data"""
        max_seq = self._calculate_max_seq(dirty, durability_filter)

        # Search from memtable first
        for entry_key, entry_value, entry_seq in reversed(self.memtable + self.immutable_memtable):
            if entry_key == key and (max_seq is None or entry_seq <= max_seq):
                return entry_value
 
        # Search L0
        for l0_content in reversed(self.l0):
            for entry_key, entry_value, entry_seq in reversed(l0_content):
                if entry_key == key and (max_seq is None or entry_seq <= max_seq):
                    return entry_value
        return None
    
    def _calculate_max_seq(self, dirty: bool, durability_filter: DurabilityLevel) -> Optional[int]:
        """Calculate maximum readable sequence number"""
        max_seq = None
 
        # durability_filter controls persistence level
        if durability_filter == DurabilityLevel.REMOTE:
            max_seq = self.last_remote_persisted_seq
 
        # dirty controls committed or not
        if not dirty:
            if max_seq is None:
                max_seq = self.last_committed_seq
            else:
                max_seq = min(max_seq, self.last_committed_seq)
 
        return max_seq
 
    def flush_wal(self):
        """Flush WAL and trigger commit"""
        if not self.wal_buffer:
            return

        # Freeze current WAL
        self.flushed_wal.extend(self.wal_buffer)
        self.wal_buffer.clear()

        # update last_remote_persisted_seq
        self.last_remote_persisted_seq = self.flushed_wal[-1][2]

    def freeze_memtable(self):
        """Flush memtable to L0"""
        if not self.memtable:
            return

        # Freeze current memtable
        self.immutable_memtable.extend(self.memtable)
        self.memtable.clear()

    def flush_memtable(self):
        if self.immutable_memtable:
            self.l0.append(list(self.immutable_memtable))
            self.immutable_memtable.clear()


# Sample usage and testing
if __name__ == "__main__":
    # Create a new SlateDB instance
    db = SlateDb()
    
    print("=== SlateDB Simulation Demo ===\n")
    
    # Test 1: Basic put/get with Memory durability
    print("Test 1: Basic put/get with Memory durability")
    print("-" * 50)
    
    db.put("key1", "value1", DurabilityLevel.MEMORY)
    print(f"Put key1=value1 with MEMORY durability")
    print(f"Last committed seq: {db.last_committed_seq}")
    print(f"Last remote persisted seq: {db.last_remote_persisted_seq}")
    
    # Read with different settings
    result1 = db.get("key1", dirty=False, durability_filter=DurabilityLevel.MEMORY)
    print(f"Get key1 (dirty=False, Memory): {result1}")
    
    result2 = db.get("key1", dirty=True, durability_filter=DurabilityLevel.MEMORY)
    print(f"Get key1 (dirty=True, Memory): {result2}")
    
    result3 = db.get("key1", dirty=False, durability_filter=DurabilityLevel.REMOTE)
    print(f"Get key1 (dirty=False, Remote): {result3}")
    print()
    
    # Test 2: Remote durability with sync
    print("Test 2: Remote durability with sync")
    print("-" * 50)
    
    db.put("key2", "value2", DurabilityLevel.REMOTE)
    print(f"Put key2=value2 with REMOTE durability (sync)")
    print(f"Last committed seq: {db.last_committed_seq}")
    print(f"Last remote persisted seq: {db.last_remote_persisted_seq}")
    
    # Read with different settings
    result1 = db.get("key2", dirty=False, durability_filter=DurabilityLevel.MEMORY)
    print(f"Get key2 (dirty=False, Memory): {result1}")
    
    result2 = db.get("key2", dirty=False, durability_filter=DurabilityLevel.REMOTE)
    print(f"Get key2 (dirty=False, Remote): {result2}")
    print()
    
    # Test 3: Multiple writes with different durability levels
    print("Test 3: Multiple writes with different durability levels")
    print("-" * 50)
    
    db.put("key3", "value3_memory", DurabilityLevel.MEMORY)
    db.put("key4", "value4_remote", DurabilityLevel.REMOTE)
    db.put("key5", "value5_memory", DurabilityLevel.MEMORY)
    
    print(f"Put key3=value3_memory (MEMORY)")
    print(f"Put key4=value4_remote (REMOTE)")
    print(f"Put key5=value5_memory (MEMORY)")
    print(f"Last committed seq: {db.last_committed_seq}")
    print(f"Last remote persisted seq: {db.last_remote_persisted_seq}")
    print()
    
    # Test reading all keys with different settings
    keys = ["key3", "key4", "key5"]
    for key in keys:
        result_memory = db.get(key, dirty=False, durability_filter=DurabilityLevel.MEMORY)
        result_remote = db.get(key, dirty=False, durability_filter=DurabilityLevel.REMOTE)
        print(f"Key: {key}")
        print(f"  Memory durability: {result_memory}")
        print(f"  Remote durability: {result_remote}")
    print()
    
    # Test 4: Demonstrate dirty reads
    print("Test 4: Demonstrate dirty reads")
    print("-" * 50)
    
    # Add a write that hasn't been flushed yet
    db.put("key6", "value6_uncommitted", DurabilityLevel.MEMORY)
    print(f"Put key6=value6_uncommitted (MEMORY, not flushed)")
    print(f"Last committed seq: {db.last_committed_seq}")
    print(f"Last remote persisted seq: {db.last_remote_persisted_seq}")
    
    # Test dirty vs non-dirty reads
    result_clean = db.get("key6", dirty=False, durability_filter=DurabilityLevel.MEMORY)
    result_dirty = db.get("key6", dirty=True, durability_filter=DurabilityLevel.MEMORY)
    print(f"Get key6 (dirty=False, Memory): {result_clean}")
    print(f"Get key6 (dirty=True, Memory): {result_dirty}")
    print()
    
    # Test 5: Show WAL buffer state
    print("Test 5: WAL buffer state")
    print("-" * 50)
    print(f"WAL buffer entries: {len(db.wal_buffer)}")
    print(f"Flushed WAL entries: {len(db.flushed_wal)}")
    print(f"Memtable entries: {len(db.memtable)}")
    print(f"Immutable memtable entries: {len(db.immutable_memtable)}")
    print(f"L0 levels: {len(db.l0)}")
    
    # Show some WAL buffer contents
    if db.wal_buffer:
        print("\nWAL buffer contents:")
        for key, value, seq in db.wal_buffer:
            print(f"  {key}={value} (seq={seq})")
    
    if db.flushed_wal:
        print("\nFlushed WAL contents:")
        for key, value, seq in db.flushed_wal:
            print(f"  {key}={value} (seq={seq})")
    
    print("\n=== Simulation Complete ===")