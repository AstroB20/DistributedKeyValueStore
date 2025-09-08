import asyncio
import httpx
from typing import List

class KVStoreClient:
    def __init__(self, cluster_nodes: List[str]):
        self.nodes = cluster_nodes

    async def get_active_node(self):
        """Find an active node"""
        for node in self.nodes:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    response = await client.get(f"http://{node}/status")
                    if response.status_code == 200:
                        return node
            except:
                continue
        raise Exception("No active nodes found")

    async def put(self, key: str, value: str):
        try:
            node = await self.get_active_node()
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.put(f"http://{node}/put", json={"key": key, "value": value})
                if response.status_code == 200:
                    result = response.json()
                    primary = "🎯" if result.get("primary") else "🔄"
                    print(f"✅ PUT {key}={value} → {primary} Node {result.get('node', 'unknown')}")
                else:
                    print(f"❌ PUT failed: {response.status_code}")
        except Exception as e:
            print(f"❌ PUT error: {e}")

    async def get(self, key: str):
        try:
            node = await self.get_active_node()
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(f"http://{node}/get/{key}")
                if response.status_code == 200:
                    result = response.json()
                    if result["found"]:
                        print(f"✅ GET {key} → {result['value']}")
                    else:
                        print(f"❌ GET {key} → Not found")
                else:
                    print(f"❌ GET failed: {response.status_code}")
        except Exception as e:
            print(f"❌ GET error: {e}")

    async def delete(self, key: str):
        try:
            node = await self.get_active_node()
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.delete(f"http://{node}/delete/{key}")
                if response.status_code == 200:
                    result = response.json()
                    if result["deleted"]:
                        print(f"✅ DELETE {key} → Deleted from node {result.get('node', 'unknown')}")
                    else:
                        print(f"❌ DELETE {key} → Not found")
                else:
                    print(f"❌ DELETE failed: {response.status_code}")
        except Exception as e:
            print(f"❌ DELETE error: {e}")

    async def status(self):
        """Show cluster status"""
        print("\n🔍 Cluster Status:")
        print("-" * 50)
        for node in self.nodes:
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    response = await client.get(f"http://{node}/status")
                    if response.status_code == 200:
                        status = response.json()
                        leader_icon = "👑" if status["is_leader"] else "🔸"
                        leader_info = f" (Leader: {status.get('leader_id', 'unknown')})" if status.get('leader_id') else ""
                        print(f"{leader_icon} Node {status['node_id']} ({node}){leader_info}")
                        print(f" Keys: {status['keys_count']}")
                        print(f" Is Leader: {status['is_leader']}")
                    else:
                        print(f"❌ Node {node} - HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ Node {node} - Unreachable: {str(e)[:50]}")
            print()

async def interactive_mode(client: KVStoreClient):
    print("🚀 Distributed Key-Value Store Client")
    print("Commands: put <key> <value>, get <key>, delete <key>, status, quit")
    print("-" * 60)
    while True:
        try:
            command = input("kvstore> ").strip().split()
            if not command:
                continue
            cmd = command[0].lower()
            if cmd in ["quit", "exit"]:
                break
            elif cmd == "put" and len(command) >= 3:
                key = command[1]
                value = " ".join(command[2:])
                await client.put(key, value)
            elif cmd == "get" and len(command) == 2:
                await client.get(command[1])
            elif cmd == "delete" and len(command) == 2:
                await client.delete(command[1])
            elif cmd == "status":
                await client.status()
            else:
                print("Usage:")
                print(" put <key> <value> - Store key-value pair")
                print(" get <key> - Retrieve value by key")
                print(" delete <key> - Delete key")
                print(" status - Show cluster status")
                print(" quit - Exit client")
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
    print("👋 Goodbye!")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Distributed KV Store Client")
    parser.add_argument("--nodes", default="localhost:8001,localhost:8002,localhost:8003")
    args = parser.parse_args()
    nodes = [node.strip() for node in args.nodes.split(",")]
    client = KVStoreClient(nodes)
    asyncio.run(interactive_mode(client))
