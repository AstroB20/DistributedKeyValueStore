import asyncio
import json
import logging
import time
from typing import Dict, List, Optional, Set
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import uvicorn
from consistent_hash import ConsistentHashRing
from replication import ReplicationManager
from leader_election import LeaderElection

# Request/Response models
class PutRequest(BaseModel):
    key: str
    value: str

class GetResponse(BaseModel):
    value: Optional[str] = None
    found: bool = False

class NodeStatus(BaseModel):
    node_id: str
    is_leader: bool
    peers: List[str]
    keys_count: int
    leader_id: Optional[str] = None

class DistributedNode:
    def __init__(self, node_id: str, host: str, port: int, peers: List[str]):
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peers = peers
        self.storage: Dict[str, str] = {}
        
        # Initialize distributed systems components
        all_nodes = peers + [f"{host}:{port}"]
        self.hash_ring = ConsistentHashRing(all_nodes)
        self.replication_manager = ReplicationManager(self, replicas=2)
        self.leader_election = LeaderElection(self)
        
        # FastAPI app
        self.app = FastAPI(title=f"KV-Store Node {node_id}")
        self.setup_routes()
        
        # HTTP client for peer communication
        self.client = httpx.AsyncClient(timeout=5.0)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(f"Node-{node_id}")
        
    def setup_routes(self):
        @self.app.put("/put")
        async def put_key(request: PutRequest):
            return await self.handle_put(request.key, request.value)
            
        @self.app.get("/get/{key}")
        async def get_key(key: str):
            return await self.handle_get(key)
            
        @self.app.delete("/delete/{key}")
        async def delete_key(key: str):
            return await self.handle_delete(key)
            
        @self.app.get("/status")
        async def get_status():
            return NodeStatus(
                node_id=self.node_id,
                is_leader=self.leader_election.is_leader,
                peers=self.peers,
                keys_count=len(self.storage),
                leader_id=self.leader_election.current_leader
            )
            
        # Internal endpoints for replication
        @self.app.post("/internal/replicate")
        async def replicate_data(request: dict):
            key = request["key"]
            value = request.get("value")
            operation = request["operation"]
            
            if operation == "PUT":
                self.storage[key] = value
                self.logger.info(f"🔄 Replicated PUT {key}={value}")
            elif operation == "DELETE":
                self.storage.pop(key, None)
                self.logger.info(f"🔄 Replicated DELETE {key}")
                
            return {"status": "replicated", "node": self.node_id}
            
        @self.app.post("/internal/heartbeat")
        async def heartbeat():
            return {"node_id": self.node_id, "timestamp": time.time(), "status": "alive"}

        # Leader election endpoints
        @self.app.post("/internal/leader_announce")
        async def leader_announce(announcement: dict):
            self.leader_election.handle_leader_announcement(announcement)
            return {"status": "acknowledged"}

        @self.app.post("/internal/leader_heartbeat") 
        async def leader_heartbeat(heartbeat: dict):
            self.leader_election.handle_leader_heartbeat(heartbeat)
            return {"status": "received"}

    async def handle_put(self, key: str, value: str):
        """Handle PUT request with consistent hashing and replication"""
        primary_node = self.hash_ring.get_primary_node(key)
        self_address = f"{self.host}:{self.port}"
        
        if primary_node == self_address:
            # This node is responsible for the key
            self.storage[key] = value
            self.logger.info(f"📝 Stored {key}={value} locally (primary)")
            
            # Replicate to other nodes
            await self.replication_manager.replicate_put(key, value)
            return {"status": "stored", "node": self.node_id, "primary": True}
        else:
            # Forward to the responsible node
            self.logger.info(f"📤 Forwarding PUT {key} to {primary_node}")
            return await self.forward_request(primary_node, "PUT", key, value)
    
    async def handle_get(self, key: str):
        """Handle GET request with consistent hashing"""
        primary_node = self.hash_ring.get_primary_node(key)
        self_address = f"{self.host}:{self.port}"
        
        if primary_node == self_address:
            # Check local storage first
            if key in self.storage:
                self.logger.info(f"📖 Found {key} locally (primary)")
                return GetResponse(value=self.storage[key], found=True)
            
            # Try replicas
            replica_nodes = self.hash_ring.get_replica_nodes(key, 2)
            for replica in replica_nodes:
                if replica != self_address:
                    try:
                        response = await self.client.get(f"http://{replica}/get/{key}")
                        if response.status_code == 200:
                            data = response.json()
                            if data["found"]:
                                self.logger.info(f"📖 Found {key} on replica {replica}")
                                return GetResponse(value=data["value"], found=True)
                    except Exception as e:
                        self.logger.warning(f"Failed to contact replica {replica}: {e}")
            
            return GetResponse(found=False)
        else:
            # Forward to responsible node
            self.logger.info(f"📤 Forwarding GET {key} to {primary_node}")
            result = await self.forward_request(primary_node, "GET", key)
            return result if result else GetResponse(found=False)
    
    async def handle_delete(self, key: str):
        """Handle DELETE request with replication"""
        primary_node = self.hash_ring.get_primary_node(key)
        self_address = f"{self.host}:{self.port}"
        
        if primary_node == self_address:
            deleted = self.storage.pop(key, None) is not None
            if deleted:
                self.logger.info(f"🗑️ Deleted {key} locally (primary)")
                await self.replication_manager.replicate_delete(key)
            return {"deleted": deleted, "node": self.node_id}
        else:
            self.logger.info(f"📤 Forwarding DELETE {key} to {primary_node}")
            return await self.forward_request(primary_node, "DELETE", key)
    
    async def forward_request(self, target_node: str, operation: str, key: str, value: str = None):
        """Forward request to the responsible node"""
        try:
            if operation == "PUT":
                response = await self.client.put(
                    f"http://{target_node}/put",
                    json={"key": key, "value": value}
                )
            elif operation == "GET":
                response = await self.client.get(f"http://{target_node}/get/{key}")
            elif operation == "DELETE":
                response = await self.client.delete(f"http://{target_node}/delete/{key}")
            
            return response.json() if response.status_code == 200 else None
        except Exception as e:
            self.logger.error(f"Failed to forward {operation} to {target_node}: {e}")
            return None

# Global node instance for background tasks
node_instance = None

async def start_background_tasks():
    """Start background tasks after server starts"""
    if node_instance:
        await node_instance.leader_election.start_election_loop()

def create_app(node_id: str, host: str, port: int, peers: List[str]):
    global node_instance
    node_instance = DistributedNode(node_id, host, port, peers)
    
    @node_instance.app.on_event("startup")
    async def startup_event():
        # Start background tasks
        asyncio.create_task(node_instance.leader_election.start_election_loop())
        node_instance.logger.info(f"🚀 Node {node_id} started on {host}:{port}")
        node_instance.logger.info(f"👥 Peers: {peers}")
    
    return node_instance.app

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python node.py <node_id> <host> <port> <peer1:port1,peer2:port2,...>")
        sys.exit(1)
    
    node_id = sys.argv[1]
    host = sys.argv[2]
    port = int(sys.argv[3])
    peers = sys.argv[4].split(",") if len(sys.argv) > 4 and sys.argv[4] else []
    
    app = create_app(node_id, host, port, peers)
    uvicorn.run(app, host=host, port=port, log_level="info")
