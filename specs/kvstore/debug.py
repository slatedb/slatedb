from enum import Enum
from typing import List, Tuple, Optional

class DurabilityLevel(Enum):
    MEMORY = 0
    LOCAL = 1
    REMOTE = 2

class SlateDb:
    def __init__(self):
        # Use list for unified storage
        self.wal_buffer: List[Tuple[str, str, int, DurabilityLevel]] = []
        self.flushed_wal: List[Tuple[str, str, int, DurabilityLevel]] = []
        self.memtable: List[Tuple[str, str, int]] = []
        self.immutable_memtable: List[Tuple[str, str, int]] = []
        self.l0: List[List[Tuple[str, str, int]]] = []  # L0 SST content stored directly

        # Key watermarks
        self.last_committed_seq = 0
        self.last_remote_persisted_seq = 0
        self.next_seq = 1
    
    def put(self, key: str, value: str, sync: DurabilityLevel = DurabilityLevel.REMOTE):
        """Write data"""
        seq = self.next_seq
        self.next_seq += 1
        
        # Add to WAL
        self.wal_buffer.append((key, value, seq, sync))
        
        # Update memtable
        self.memtable.append((key, value, seq))
    
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
        if not self.wal_buffer and not self.flushed_wal:
            return
 
        # Freeze current WAL
        if self.wal_buffer:
            self.flushed_wal.extend(self.wal_buffer)
            self.wal_buffer.clear()
 
        if self.flushed_wal:
            # Process all pending WAL entries
            max_seq = 0
            for key, value, seq, durability in self.flushed_wal:
                # Commit based on durability level
                if durability in [DurabilityLevel.LOCAL, DurabilityLevel.REMOTE]:
                    self.memtable.append((key, value, seq))
                    max_seq = max(max_seq, seq)
            
            # Update watermarks
            if max_seq > 0:
                self.last_committed_seq = max_seq
                self.last_remote_persisted_seq = max_seq
            
            self.flushed_wal.clear()
 
    def freeze_memtable(self):
        """Flush memtable to L0"""
        if not self.memtable:
            return
            
        # Freeze current memtable
        self.immutable_memtable.extend(self.memtable)
        self.memtable.clear()
 
    def flush_memtable(self):
        if self.immutable_memtable:
            # Write directly to L0
            self.l0.append(list(self.immutable_memtable))
            self.immutable_memtable.clear()


# Usage example
if __name__ == "__main__":
    db = SlateDb()
    
    # Example 1: Memory level immediate commit
    db.put("key1", "value1", DurabilityLevel.MEMORY)
    print(f"Memory put: {db.get('key1', dirty=True)}")  # value1
    print(f"Memory clean: {db.get('key1', dirty=False)}")  # value1
    
    # Example 2: Remote level wait for commit
    db.put("key2", "value2", DurabilityLevel.REMOTE)
    print(f"Remote before flush (dirty): {db.get('key2', dirty=True)}")  # None
    print(f"Remote before flush (clean): {db.get('key2', dirty=False)}")  # None
    
    db.flush_wal()
    print(f"Remote after flush (dirty): {db.get('key2', dirty=True)}")  # value2
    print(f"Remote after flush (clean): {db.get('key2', dirty=False)}")  # value2
    
    # Example 3: Multi-level persistence
    db.put("key3", "value3", DurabilityLevel.LOCAL)
    print(f"Local before flush: {db.get('key3', dirty=True)}")  # None
    db.flush_wal()
    print(f"Local after flush: {db.get('key3', dirty=True)}")  # value3
    
    # Example 4: memtable flush
    db.put("key4", "value4", DurabilityLevel.MEMORY)
    db.flush_memtable()
    print(f"After memtable flush: {db.get('key4', dirty=False)}")  # value4
