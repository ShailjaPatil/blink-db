# BlinkDB - High Performance Key-Value Store

![Project Logo](KGP_LOGO.png)

A lightweight, high-performance in-memory key-value store with LRU eviction policy, compatible with Redis protocol (RESP-2).

## Project Structure
part-b/
├── src/
│ ├── blinkdb.cpp # Main server implementation
│ └── run_benchmarks.sh # Benchmarking script
│ └── results/ # Directory for benchmark results
└── docs/
  ├── DesignDoc_PartB.pdf # Design Documentation
  ├── Doxygen_Documentation_PartB.pdf # Doxygen generated Documentation (PDF)
  └── html/ # HTML Documentation Doxygen generated
    └── index.html # Entry point for web docs


## Prerequisites

- Linux environment (tested on Ubuntu)
- g++ compiler (supporting C++11)
- Redis CLI (for benchmarking)
- Doxygen (optional, for documentation)
- LaTeX tools (optional, for PDF docs)

## Quick Start

### 1. Compile the Server

```bash
cd src
g++ -std=c++11 -o blinkdb blinkdb.cpp

2. Run the Server (Terminal 1)
```bash
./blinkdb --server
Server will start on port 9001

3. Run Benchmarks (Terminal 2)
```bash
chmod +x run_benchmarks.sh
./run_benchmarks.sh


Benchmark Results
The script will automatically test different configurations:

Requests	Connections	Output File
10,000	10	results/result_10000_10.txt
100,000	10	results/result_100000_10.txt
1,000,000	10	results/result_1000000_10.txt
...	...	...
1,000,000	1000	results/result_1000000_1000.txt

Example output:

Creating benchmark file: results/result_10000_10.txt
Completed: results/result_10000_10.txt
----------------------------------
Creating benchmark file: results/result_100000_10.txt
Completed: results/result_100000_10.txt
----------------------------------

Key Features
RESP-2 protocol compatibility

LRU eviction policy

Epoll-based high performance server

Thread-safe operations

Comprehensive benchmarking


This README includes:

1. Clear visual hierarchy with logo
2. Step-by-step execution flow
3. Benchmark matrix showing test configurations
4. License placeholder




