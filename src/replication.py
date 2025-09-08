import asyncio
import logging
from typing import List
import httpx

class ReplicationManager:
    def __init__(self, node, replicas: int = 3, write_quorum: int = 2, read_quorum: int = 2):
        self.node = node
        self.replica_count = replicas
        self.write_quorum = write_quorum
        self.read_quorum = read_quorum
        self.logger = logging.getLogger(f"Replication-{node.node_id}")

    async def replicate_put(self, key: str, value: str) -> bool:
        """Replicate PUT operation to replica nodes with quorum write"""
        replica_nodes = self.node.hash_ring.get_replica_nodes(key, self.replica_count)
        self_address = f"{self.node.host}:{self.node.port}"
        # Exclude self from replicas (already has data locally)
        replica_nodes = [n for n in replica_nodes if n != self_address]
        tasks = [self._replicate_to_node(replica, "PUT", key, value) for replica in replica_nodes[:self.replica_count-1]]

        if not tasks:
            return True  # No replicas configured, single node

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)

        if success_count + 1 >= self.write_quorum:
            self.logger.info(f"Quorum achieved for PUT {key}: {success_count + 1}/{self.write_quorum}")
            return True
        else:
            self.logger.warning(f"Quorum NOT achieved for PUT {key}: {success_count + 1}/{self.write_quorum}")
            return False

    async def replicate_delete(self, key: str) -> bool:
        """Replicate DELETE operation to replica nodes with quorum write"""
        replica_nodes = self.node.hash_ring.get_replica_nodes(key, self.replica_count)
        self_address = f"{self.node.host}:{self.node.port}"
        replica_nodes = [n for n in replica_nodes if n != self_address]
        tasks = [self._replicate_to_node(replica, "DELETE", key) for replica in replica_nodes[:self.replica_count-1]]

        if not tasks:
            return True

        results = await asyncio.gather(*tasks, return_exceptions=True)
        success_count = sum(1 for r in results if r is True)

        if success_count + 1 >= self.write_quorum:
            self.logger.info(f"Quorum achieved for DELETE {key}: {success_count + 1}/{self.write_quorum}")
            return True
        else:
            self.logger.warning(f"Quorum NOT achieved for DELETE {key}: {success_count + 1}/{self.write_quorum}")
            return False

    async def _replicate_to_node(self, target_node: str, operation: str, key: str, value: str = None):
        try:
            payload = {
                "operation": operation,
                "key": key,
                "value": value
            }
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.post(f"http://{target_node}/internal/replicate", json=payload)
                if response.status_code == 200:
                    return True
                else:
                    self.logger.warning(f"Replication failed to {target_node}: {response.status_code}")
                    return False
        except Exception as e:
            self.logger.error(f"Replication error to {target_node}: {e}")
            return False
