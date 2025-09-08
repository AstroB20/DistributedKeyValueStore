import hashlib
import bisect
from typing import List, Dict

class ConsistentHashRing:
    def __init__(self, nodes: List[str], virtual_nodes: int = 150):
        self.nodes = nodes
        self.virtual_nodes = virtual_nodes
        self.ring: Dict[int, str] = {}
        self.sorted_keys: List[int] = []
        self._build_ring()
    
    def _hash(self, key: str) -> int:
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def _build_ring(self):
        """Build the consistent hash ring"""
        self.ring.clear()
        self.sorted_keys.clear()
        
        for node in self.nodes:
            for i in range(self.virtual_nodes):
                virtual_key = f"{node}:{i}"
                hash_value = self._hash(virtual_key)
                self.ring[hash_value] = node
                self.sorted_keys.append(hash_value)
        
        self.sorted_keys.sort()
        print(f"Hash ring built with {len(self.sorted_keys)} virtual nodes")
    
    def get_primary_node(self, key: str) -> str:
        """Get the primary node responsible for a key"""
        if not self.ring:
            raise ValueError("Hash ring is empty")
        
        hash_value = self._hash(key)
        
        # Find the first node clockwise from the hash
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        if idx == len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def get_replica_nodes(self, key: str, replica_count: int) -> List[str]:
        """Get replica nodes for a key"""
        if not self.ring:
            return []
        
        hash_value = self._hash(key)
        nodes = []
        seen_physical_nodes = set()
        
        # Start from the primary node position
        idx = bisect.bisect_right(self.sorted_keys, hash_value)
        
        while len(nodes) < replica_count and len(seen_physical_nodes) < len(self.nodes):
            if idx >= len(self.sorted_keys):
                idx = 0
            
            physical_node = self.ring[self.sorted_keys[idx]]
            if physical_node not in seen_physical_nodes:
                nodes.append(physical_node)
                seen_physical_nodes.add(physical_node)
            
            idx += 1
        
        return nodes
    
    def add_node(self, node: str):
        """Add a new node to the ring"""
        if node not in self.nodes:
            self.nodes.append(node)
            self._build_ring()
    
    def remove_node(self, node: str):
        """Remove a node from the ring"""
        if node in self.nodes:
            self.nodes.remove(node)
            self._build_ring()
    
    def get_key_distribution(self) -> Dict[str, int]:
        """Get the distribution of keys across nodes (for debugging)"""
        distribution = {node: 0 for node in self.nodes}
        for node in self.ring.values():
            distribution[node] += 1
        return distribution
