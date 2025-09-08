import asyncio
import subprocess
import time
import sys
from typing import List

class ClusterManager:
    def __init__(self):
        self.processes: List[subprocess.Popen] = []
        self.nodes = [
            {"id": "node1", "host": "localhost", "port": 8001},
            {"id": "node2", "host": "localhost", "port": 8002},
            {"id": "node3", "host": "localhost", "port": 8003},
        ]
    
    def start_cluster(self):
        """Start all nodes in the cluster"""
        print("🚀 Starting Distributed Key-Value Store Cluster...")
        
        # Build peer lists for each node
        for i, node in enumerate(self.nodes):
            peers = [f"{n['host']}:{n['port']}" for j, n in enumerate(self.nodes) if i != j]
            peer_str = ",".join(peers)
            
            cmd = [
                sys.executable, "src/node.py",
                node["id"],
                node["host"],
                str(node["port"]),
                peer_str
            ]
            
            print(f"Starting {node['id']} on {node['host']}:{node['port']}")
            try:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.processes.append(process)
                time.sleep(3)  # Increased delay for stability
            except Exception as e:
                print(f"❌ Failed to start {node['id']}: {e}")
                continue
        
        print(f"✅ Cluster started with {len(self.processes)} nodes")
        print("Nodes:")
        for node in self.nodes:
            print(f"  - {node['id']}: http://{node['host']}:{node['port']}")
        
        print("\n🎯 To test the cluster:")
        print("python src/client.py --nodes localhost:8001,localhost:8002,localhost:8003")
        print("\nPress Ctrl+C to stop the cluster")
        
        try:
            # Monitor processes
            while True:
                time.sleep(1)
                # Check if any process has died
                for i, process in enumerate(self.processes):
                    if process.poll() is not None:
                        print(f"⚠️ Node {self.nodes[i]['id']} has stopped")
        except KeyboardInterrupt:
            self.stop_cluster()
    
    def stop_cluster(self):
        """Stop all cluster nodes"""
        print("\n🛑 Stopping cluster...")
        for process in self.processes:
            try:
                process.terminate()
            except:
                pass
        
        # Wait for graceful shutdown
        time.sleep(2)
        
        # Force kill if needed
        for process in self.processes:
            try:
                if process.poll() is None:
                    process.kill()
            except:
                pass
        
        print("✅ Cluster stopped")

if __name__ == "__main__":
    manager = ClusterManager()
    try:
        manager.start_cluster()
    except KeyboardInterrupt:
        manager.stop_cluster()
