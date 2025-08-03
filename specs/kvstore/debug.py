from enum import Enum
from typing import List, Tuple, Optional

class DurabilityLevel(Enum):
    MEMORY = 0
    LOCAL = 1
    REMOTE = 2

class SlateDb:
    def __init__(self):
        # 统一使用list存储
        self.wal: List[Tuple[str, str, int, DurabilityLevel]] = []
        self.immutable_wal: List[Tuple[str, str, int, DurabilityLevel]] = []
        self.memtable: List[Tuple[str, str, int]] = []
        self.immutable_memtable: List[Tuple[str, str, int]] = []
        self.l0: List[List[Tuple[str, str, int]]] = []  # L0 SST内容直接存储

        # 关键水位线
        self.last_committed_seq = 0
        self.last_remote_persisted_seq = 0
        self.next_seq = 1
    
    def put(self, key: str, value: str, durability: DurabilityLevel = DurabilityLevel.REMOTE):
        """写入数据"""
        seq = self.next_seq
        self.next_seq += 1
        
        # 添加到WAL
        self.wal.append((key, value, seq, durability))
        
        # Memory级别立即commit
        if durability == DurabilityLevel.MEMORY:
            self.memtable.append((key, value, seq))
            self.last_committed_seq = seq
    
    def get(self, key: str, dirty: bool = True, 
            durability_filter: DurabilityLevel = DurabilityLevel.REMOTE) -> Optional[str]:
        """读取数据"""
        max_seq = self._calculate_max_seq(dirty, durability_filter)
        
        # 从memtable开始查找
        for entry_key, entry_value, entry_seq in reversed(self.memtable + self.immutable_memtable):
            if entry_key == key and (max_seq is None or entry_seq <= max_seq):
                return entry_value
        
        # 查L0
        for l0_content in reversed(self.l0):
            for entry_key, entry_value, entry_seq in reversed(l0_content):
                if entry_key == key and (max_seq is None or entry_seq <= max_seq):
                    return entry_value
        
        return None
    
    def _calculate_max_seq(self, dirty: bool, durability_filter: DurabilityLevel) -> Optional[int]:
        """计算最大可读序列号"""
        max_seq = None
        
        # durability_filter控制持久化级别
        if durability_filter == DurabilityLevel.REMOTE:
            max_seq = self.last_remote_persisted_seq
        
        # dirty控制commit级别
        if not dirty:
            if max_seq is None:
                max_seq = self.last_committed_seq
            else:
                max_seq = min(max_seq, self.last_committed_seq)
        
        return max_seq
    
    def flush_wal(self):
        """刷新WAL并触发commit"""
        if not self.wal and not self.immutable_wal:
            return
            
        # 冻结当前WAL
        if self.wal:
            self.immutable_wal.extend(self.wal)
            self.wal.clear()
        
        if self.immutable_wal:
            # 处理所有待刷新的WAL条目
            max_seq = 0
            for key, value, seq, durability in self.immutable_wal:
                # 根据durability级别commit
                if durability in [DurabilityLevel.LOCAL, DurabilityLevel.REMOTE]:
                    self.memtable.append((key, value, seq))
                    max_seq = max(max_seq, seq)
            
            # 更新水位线
            if max_seq > 0:
                self.last_committed_seq = max_seq
                self.last_remote_persisted_seq = max_seq
            
            self.immutable_wal.clear()
    
    def flush_memtable(self):
        """刷新memtable到L0"""
        if not self.memtable:
            return
            
        # 冻结当前memtable
        self.immutable_memtable.extend(self.memtable)
        self.memtable.clear()
        
        if self.immutable_memtable:
            # 直接写入L0
            self.l0.append(list(self.immutable_memtable))
            self.immutable_memtable.clear()


# 使用示例
if __name__ == "__main__":
    db = SlateDb()
    
    # 示例1：Memory级别立即commit
    db.put("key1", "value1", DurabilityLevel.MEMORY)
    print(f"Memory put: {db.get('key1', dirty=True)}")  # value1
    print(f"Memory clean: {db.get('key1', dirty=False)}")  # value1
    
    # 示例2：Remote级别等待commit
    db.put("key2", "value2", DurabilityLevel.REMOTE)
    print(f"Remote before flush (dirty): {db.get('key2', dirty=True)}")  # None
    print(f"Remote before flush (clean): {db.get('key2', dirty=False)}")  # None
    
    db.flush_wal()
    print(f"Remote after flush (dirty): {db.get('key2', dirty=True)}")  # value2
    print(f"Remote after flush (clean): {db.get('key2', dirty=False)}")  # value2
    
    # 示例3：多级持久化
    db.put("key3", "value3", DurabilityLevel.LOCAL)
    print(f"Local before flush: {db.get('key3', dirty=True)}")  # None
    db.flush_wal()
    print(f"Local after flush: {db.get('key3', dirty=True)}")  # value3
    
    # 示例4：memtable刷新
    db.put("key4", "value4", DurabilityLevel.MEMORY)
    db.flush_memtable()
    print(f"After memtable flush: {db.get('key4', dirty=False)}")  # value4
