import asyncio
import logging
from typing import List
import httpx

class ReplicationManager:
    def __init__(self, node, replicas: int = 2):
        self.node = node
        self.replica_count = replicas
        self.logger = logging.getLogger(f"Replication-{node.node_id}")
    
    async def replicate_put(self, key: str, value: str):
        """Replicate PUT operation to replica nodes"""
        replica_nodes = self.node.hash_ring.get_replica_nodes(key, self.replica_count)
        
        # Remove self from replicas
        self_address = f"{self.node.host}:{self.node.port}"
        replica_nodes = [n for n in replica_nodes if n != self_address]
        
        # Replicate to all replica nodes
        tasks = []
        for replica in replica_nodes[:self.replica_count-1]:  # -1 because primary already has it
            task = self._replicate_to_node(replica, "PUT", key, value)
            tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_replicas = sum(1 for r in results if not isinstance(r, Exception))
            self.logger.info(f"Replicated {key} to {successful_replicas}/{len(tasks)} nodes")
    
    async def replicate_delete(self, key: str):
        """Replicate DELETE operation to replica nodes"""
        replica_nodes = self.node.hash_ring.get_replica_nodes(key, self.replica_count)
        
        # Remove self from replicas
        self_address = f"{self.node.host}:{self.node.port}"
        replica_nodes = [n for n in replica_nodes if n != self_address]
        
        # Replicate to all replica nodes
        tasks = []
        for replica in replica_nodes[:self.replica_count-1]:
            task = self._replicate_to_node(replica, "DELETE", key)
            tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            successful_replicas = sum(1 for r in results if not isinstance(r, Exception))
            self.logger.info(f"Replicated DELETE {key} to {successful_replicas}/{len(tasks)} nodes")
    
    async def _replicate_to_node(self, target_node: str, operation: str, key: str, value: str = None):
        """Replicate operation to a specific node"""
        try:
            payload = {
                "operation": operation,
                "key": key,
                "value": value
            }
            
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(
                    f"http://{target_node}/internal/replicate",
                    json=payload
                )
                
                if response.status_code == 200:
                    return True
                else:
                    self.logger.warning(f"Replication failed to {target_node}: {response.status_code}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Replication error to {target_node}: {e}")
            return False
