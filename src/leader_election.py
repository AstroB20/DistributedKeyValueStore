import asyncio
import time
import logging
import httpx
from typing import Optional

class LeaderElection:
    def __init__(self, node):
        self.node = node
        self.is_leader = False
        self.current_leader: Optional[str] = None
        self.last_heartbeat = time.time()
        self.election_timeout = 10.0  # seconds
        self.heartbeat_interval = 3.0  # seconds
        self.logger = logging.getLogger(f"Leader-{node.node_id}")
        self.election_in_progress = False
    
    async def start_election_loop(self):
        """Main leader election loop"""
        await asyncio.sleep(3)  # Initial delay for cluster stabilization
        
        self.logger.info("🗳️ Starting leader election process...")
        
        while True:
            try:
                if not self.is_leader and not self.election_in_progress and self._should_start_election():
                    await self._start_election()
                elif self.is_leader:
                    await self._send_heartbeats()
                    await asyncio.sleep(self.heartbeat_interval)
                else:
                    await asyncio.sleep(2)
                    
                # Check if leader is still alive
                if self.current_leader and not self.is_leader:
                    if time.time() - self.last_heartbeat > self.election_timeout:
                        self.logger.warning(f"⚠️ Leader {self.current_leader} seems dead, starting election")
                        self.current_leader = None
                        
            except Exception as e:
                self.logger.error(f"Election loop error: {e}")
                await asyncio.sleep(5)
    
    def _should_start_election(self) -> bool:
        """Check if election should be started"""
        if self.current_leader is None:
            return True
        
        # Start election if no heartbeat from leader
        return time.time() - self.last_heartbeat > self.election_timeout
    
    async def _start_election(self):
        """Start leader election using lowest-ID wins approach"""
        self.election_in_progress = True
        self.logger.info("🗳️ Starting leader election...")
        
        try:
            # Simple election: lowest node_id becomes leader
            all_nodes = [f"{self.node.host}:{self.node.port}"] + self.node.peers
            active_nodes = await self._get_active_nodes(all_nodes)
            
            if active_nodes:
                # Sort nodes and pick the lowest ID  
                active_nodes.sort()
                new_leader = active_nodes[0]
                
                self.logger.info(f"🏆 Election result: {new_leader} should be leader")
                
                if new_leader == f"{self.node.host}:{self.node.port}":
                    await self._become_leader()
                else:
                    self._follow_leader(new_leader)
            else:
                self.logger.warning("No active nodes found for election")
                
        finally:
            self.election_in_progress = False
    
    async def _get_active_nodes(self, nodes):
        """Get list of currently active nodes"""
        active_nodes = []
        
        for node in nodes:
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    response = await client.post(f"http://{node}/internal/heartbeat")
                    if response.status_code == 200:
                        active_nodes.append(node)
                        self.logger.debug(f"✅ Node {node} is active")
                    else:
                        self.logger.debug(f"❌ Node {node} returned {response.status_code}")
            except Exception as e:
                self.logger.debug(f"❌ Node {node} unreachable: {e}")
                continue
        
        self.logger.info(f"Active nodes: {active_nodes}")
        return active_nodes
    
    async def _become_leader(self):
        """Become the cluster leader"""
        if not self.is_leader:
            self.is_leader = True
            self.current_leader = f"{self.node.host}:{self.node.port}"
            self.last_heartbeat = time.time()
            self.logger.info(f"👑 BECAME CLUSTER LEADER!")
            
            # Notify other nodes about leadership
            await self._announce_leadership()
    
    def _follow_leader(self, leader_address: str):
        """Follow a different leader"""
        if self.current_leader != leader_address:
            self.is_leader = False
            self.current_leader = leader_address
            self.last_heartbeat = time.time()
            self.logger.info(f"👥 Following leader: {leader_address}")
    
    async def _announce_leadership(self):
        """Announce leadership to all peers"""
        announcement = {
            "leader_id": self.node.node_id,
            "leader_address": f"{self.node.host}:{self.node.port}",
            "timestamp": time.time()
        }
        
        for peer in self.node.peers:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    response = await client.post(f"http://{peer}/internal/leader_announce", json=announcement)
                    if response.status_code == 200:
                        self.logger.info(f"📢 Announced leadership to {peer}")
            except Exception as e:
                self.logger.warning(f"Failed to announce leadership to {peer}: {e}")
    
    async def _send_heartbeats(self):
        """Send heartbeats to all followers"""
        heartbeat = {
            "leader_id": self.node.node_id,
            "leader_address": f"{self.node.host}:{self.node.port}",
            "timestamp": time.time()
        }
        
        for peer in self.node.peers:
            try:
                async with httpx.AsyncClient(timeout=2.0) as client:
                    response = await client.post(f"http://{peer}/internal/leader_heartbeat", json=heartbeat)
                    if response.status_code == 200:
                        self.logger.debug(f"💓 Sent heartbeat to {peer}")
            except Exception as e:
                self.logger.debug(f"Heartbeat failed to {peer}: {e}")
    
    def handle_leader_announcement(self, announcement: dict):
        """Handle leadership announcement from another node"""
        leader_address = announcement.get("leader_address")
        if leader_address != f"{self.node.host}:{self.node.port}":
            self.is_leader = False
            self.current_leader = leader_address
            self.last_heartbeat = time.time()
            self.logger.info(f"📢 Received leadership announcement from {announcement.get('leader_id')}")
    
    def handle_leader_heartbeat(self, heartbeat_data: dict):
        """Handle heartbeat from leader"""
        leader_address = heartbeat_data.get("leader_address")
        if leader_address and leader_address != f"{self.node.host}:{self.node.port}":
            if self.is_leader:
                self.logger.warning(f"⚠️ Another leader detected: {heartbeat_data.get('leader_id')}, stepping down")
                self.is_leader = False
            
            self.current_leader = leader_address
            self.last_heartbeat = time.time()
            self.logger.debug(f"💓 Received heartbeat from leader {heartbeat_data.get('leader_id')}")
