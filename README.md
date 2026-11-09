# Java Redis – Build Your Own Redis in Java

A feature-rich Redis-inspired in-memory database built from scratch using Java. This project implements core Redis concepts, data structures, persistence, replication, automatic failover, and Sentinel-based monitoring.

## 🚀 Project Overview

This project is an educational implementation of Redis using Java and TCP socket programming.

It demonstrates how an in-memory database works internally, including client-server communication, data persistence, master-replica replication, fault detection, and automatic failover.

## ✨ Features

### Core Redis Features
- TCP Client-Server Architecture
- String Operations
- Concurrent Client Handling
- Key Expiration (TTL)
- Lists, Sets and Hashes
- Sorted Sets
- RESP Protocol
- Transactions
- WATCH Support
- Pub/Sub Messaging

### Storage and Persistence
- Snapshot Persistence
- Append-Only File (AOF)
- Active Key Expiration
- LRU Eviction
- Authentication
- Automatic Authentication
- Data Type Safety

### Replication and High Availability
- Master-Replica Replication
- Multiple Replica Support
- Full Synchronization
- Partial Synchronization
- Replication Backlog
- Replication ID and Epoch
- Replica Health Monitoring
- Heartbeat Monitoring
- Manual Failover
- Automatic Failover
- Replica Recovery

### SentinelLite Monitoring
- Continuous Node Monitoring
- Master Failure Detection
- Automatic Replica Promotion
- Quorum-Based Failover Protection
- Split-Brain Protection
- Persistent Sentinel State
- Cluster Health Dashboard
- Replica Lag Monitoring
- Recovery Alerts
- Event Logging

## 🏗️ Architecture

The project uses a three-node Redis cluster:

| Port | Role |
|---|---|
| 6380 | Redis Master |
| 6381 | Redis Replica |
| 6382 | Redis Replica |

SentinelLite monitors all three nodes and manages automatic failover.

The current master can change following a failover.

## 🛠️ Technology Stack

- Java
- Java Socket Programming
- Multithreading
- TCP/IP
- RESP Protocol
- File I/O
- Object Serialization
- PowerShell
- Visual Studio Code
- Git and GitHub

## 📁 Project Structure

```text
Build-Your-Own-Redis-Java/
│
├── src/
│   ├── RedisServer.java
│   ├── RedisClient.java
│   ├── SentinelLite.java
│   ├── AofManager.java
│   ├── AuthManager.java
│   ├── FailoverManager.java
│   ├── FullSyncPayload.java
│   ├── KeyVersionManager.java
│   ├── NodeRole.java
│   ├── PubSubManager.java
│   ├── RedisSubscriber.java
│   ├── ReplicaHealth.java
│   ├── ReplicaTarget.java
│   ├── ReplicationBacklog.java
│   ├── ReplicationId.java
│   ├── ReplicationManager.java
│   ├── ReplicationMetadata.java
│   ├── RespProtocol.java
│   └── SortedSetStore.java
│
├── backup-stage24/
│
├── .gitignore
└── README.md
```

## ⚙️ Requirements

- Java JDK 21 or later
- Visual Studio Code (recommended)
- Windows PowerShell or another terminal

## 💻 Installation and Setup

### 1. Clone the Repository

```bash
git clone https://github.com/AbhiThakur16/Build-Your-Own-Redis-Java.git
```

### 2. Navigate to the Project

```bash
cd Build-Your-Own-Redis-Java
```

### 3. Navigate to Source Directory

```bash
cd src
```

### 4. Compile Java Files

```bash
javac *.java
```

### 5. Start the Redis Master

```bash
java RedisServer master 6380
```

### 6. Start Replica 1

```bash
java RedisServer replica 6381
```

### 7. Start Replica 2

```bash
java RedisServer replica 6382
```

### 8. Start SentinelLite

Open another terminal:

```bash
java SentinelLite
```

Keep each server and SentinelLite running in separate terminals.

## 🔌 Connecting to Redis

Connect to the master:

```bash
java RedisClient 6380
```

Connect to a replica:

```bash
java RedisClient 6381
```

## 🧪 Example Commands

```text
SET name Abhi
GET name

SET project "Java Redis"
GET project

PING
INFO
REPLICATIONINFO
```

The project also supports additional Redis-inspired commands for lists, sets, hashes, sorted sets, transactions, Pub/Sub, and administration.

## 🛡️ High Availability Testing

The project has been tested for:

1. Master failure detection.
2. Quorum verification.
3. Automatic replica promotion.
4. Data availability after failover.
5. Replication to the new master.
6. Old master recovery.
7. Full synchronization after recovery.
8. Final cluster health verification.

## 📊 Development Progress

The project was developed incrementally through 29 stages, covering:

- Redis core functionality
- Data structures
- Persistence
- Transactions and Pub/Sub
- Replication
- Synchronization
- Failover
- Sentinel monitoring
- Quorum and split-brain protection
- Recovery and consistency testing

## ⚠️ Disclaimer

This is an educational Redis-inspired implementation built from scratch in Java. It is not a production replacement for official Redis and has not been validated for production-grade reliability, security, or performance.

## 👨‍💻 Author

**Abhi Thakur**

- GitHub: [AbhiThakur16](https://github.com/AbhiThakur16)
- LinkedIn: [Abhi Thakur](https://www.linkedin.com/in/abhi-thakur-530728372/)

---

⭐ If you find this project interesting, consider giving it a star!