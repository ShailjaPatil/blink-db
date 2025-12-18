# BlinkDB – Redis-Inspired Tiered In-Memory Database

BlinkDB is an academic project inspired by **Redis** that aims to understand how a high-performance, in-memory key–value store works internally. The project focuses on **networking, concurrency, caching, eviction, and benchmarking**, and applies these concepts to a simplified real-world system.

---

## Problem Statement

Modern applications require extremely fast data access with low latency and high concurrency. Systems like Redis achieve this by storing data in memory, using efficient I/O multiplexing, and supporting persistence for durability.

The goal of this project was to:

* Understand how Redis works internally
* Design a simplified Redis-like system from scratch
* Build a scalable key–value store with persistence and eviction
* Evaluate its performance under concurrent client load

---

## Key Features

* **In-memory key–value store** with string keys and values
* **Warm cache with LRU eviction policy**
* **Disk persistence** for evicted keys (recoverability)
* **epoll-based TCP server** for handling large numbers of concurrent clients
* **RESP-2 protocol** support for Redis compatibility
* **Single-threaded event-driven architecture** (Redis-style)
* **Extensive benchmarking using redis-benchmark**

---

## System Architecture (High-Level)

1. **Client Layer**

   * Clients connect using TCP
   * Commands are sent using RESP-2 format (e.g., GET, SET)

2. **Network Layer**

   * Non-blocking sockets
   * `epoll` used for I/O multiplexing
   * Single main thread waits on events using `epoll_wait`

3. **Command Processing Layer**

   * RESP command parsing
   * GET / SET execution
   * In-memory lookup

4. **Storage Layer**

   * Warm cache (in-memory)
   * LRU eviction policy
   * Disk persistence on eviction

---

## Data Structures Used

* **Hash map** for O(1) average key lookup
* **Doubly linked list** for LRU tracking
* **Hash + List indexing** for efficient eviction

---

## Persistence Model

* Data is stored **in memory by default** for fast access
* When the warm cache reaches capacity:

  * Least Recently Used (LRU) key is evicted
  * Evicted key–value pair is **written to disk**
* Disk acts as a cold storage layer
* Disk reads are slower (~1–2 ms) compared to memory (<10 µs)

Note: Unlike Redis AOF, BlinkDB persists data **only on eviction**, not on every write.

---

## Networking & Concurrency

* Uses **epoll** for scalable I/O
* Single-threaded, event-driven model
* Handles thousands of concurrent connections efficiently
* Avoids locks and race conditions by design

Why single-threaded?

* Simpler concurrency model
* Predictable latency
* Matches Redis’s core design philosophy

---

## Benchmarking & Performance Evaluation

Benchmarking was performed using **redis-benchmark**.

### Test Parameters

* Request counts: 10K, 100K, 1M
* Concurrent clients: 10, 100, 1000
* Operations tested: GET, SET

### Peak Results (Balanced Workload)

* **GET:** ~92,000 requests/sec
* **SET:** ~84,000 requests/sec

### Latency

* In-memory access: < 10 µs
* Disk retrieval: ~1–2 ms

Benchmark logs are stored in files such as:

```
results_1000000_1000.txt
```

---

## Sample redis-benchmark Command

```
redis-benchmark -h 127.0.0.1 -p 9001 -n 1000000 -c 1000 -t get,set
```

---
## Limitations

* No replication or clustering
* No authentication or ACLs
* Persistence only on eviction
* Single-node deployment

---

## Future Improvements

* Append-Only File (AOF) persistence
* Multithreaded worker pool
* Priority-based connection handling
* Replication and sharding
* TTL support for keys

---

## Tech Stack

* Language: C / C++
* Networking: TCP/IP sockets
* I/O Multiplexing: epoll
* Protocol: RESP-2
* Benchmarking: redis-benchmark

---

## Academic Context

**Course:** Design Lab
**Instructor:** Prof. Mainack Mondal
**Semester:** Spring 2025

---

## Takeaway

BlinkDB demonstrates how real-world systems like Redis achieve high throughput and low latency using:

* Event-driven I/O
* Efficient data structures
* Careful trade-offs between performance and durability

This project strengthened understanding of **systems programming, networking, caching, and performance engineering**.
