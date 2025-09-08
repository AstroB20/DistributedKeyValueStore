# 🗄️ Distributed Key-Value Store

A simple yet powerful distributed key-value store showcasing **leader election**, **consistent hashing**, **replication**, and **fault tolerance**.

## ✅ Features

### 1. **Leader Election**

- Cluster elects a leader automatically
- All nodes recognize the elected leader (e.g., `localhost:8001`)
- Leader status is shared across the cluster

### 2. **Consistent Hashing Distribution**

- Keys are distributed across nodes based on their hash values
- Example: `user1` is routed to `node2` (not the leader)

### 3. **Replication**

- Each key is stored on a primary + replica(s) for fault tolerance
- Example: `user1` stored on `node2` (primary) and `node3` (replica)
- Leader may not store the key at all (acts as coordinator only)

---

## 🚀 Usage

### Start the Cluster

Run nodes in separate terminals:

```bash
python src/node.py node1 localhost 8001 localhost:8002,localhost:8003
python src/node.py node2 localhost 8002 localhost:8001,localhost:8003
python src/node.py node3 localhost 8003 localhost:8001,localhost:8002
```

Start the CLI:

```bash
python src/cli.py
```

---

## 🧪 Testing Scenarios

### Test 1: Add Keys and Check Distribution

```bash
kvstore> put user2 Bob
kvstore> put user3 Charlie
kvstore> put user4 David
kvstore> put user5 Eve
kvstore> status
```

👉 Keys should be distributed across nodes with replication.

### Test 2: Retrieve Data

```bash
kvstore> get user1   # → Alice (from node2 or replica)
kvstore> get user2   # → Bob
kvstore> get user3   # → Charlie
```

### Test 3: Leader Failover

```bash
kvstore> status      # check current leader
# Kill node1 (Ctrl+C in Terminal 1)
# Wait 10-15s
kvstore> status      # new leader elected
kvstore> put after_failover test_data
kvstore> get user1   # still works via replica
```

### Test 4: Node Recovery

```bash
# Restart the killed node
python src/node.py node1 localhost 8001 localhost:8002,localhost:8003

kvstore> status
# node1 rejoins as follower
```

---

## 🔎 Architecture Overview

1. **Consistent Hashing** → Deterministic key-to-node mapping
2. **Replication** → Data stored on primary + replicas
3. **Leader Election** → One leader coordinates operations
4. **Load Distribution** → Keys spread across cluster
5. **Fault Tolerance** → Cluster survives node failures

---

## 🧩 Key Insights

- **Leader ≠ Data Store** → Leader coordinates, not necessarily stores data
- **Hash-Based Routing** → Same key always goes to same node
- **Automatic Replication** → Built-in fault tolerance
- **Resilience** → Operations continue despite failures

---

## 🔮 Next Steps

- Add more keys and observe distribution
- Kill/restart different nodes for failure testing
- Watch logs for deeper insight into cluster mechanics

---

✨ This project demonstrates core distributed systems concepts:

- ✅ Consistent Hashing
- ✅ Replication
- ✅ Leader Election
- ✅ Fault Tolerance
- ✅ Load Distribution

---
