/**
 * @file blinkdb.cpp
 * @brief High -Performance in-memory key-value store with RESp2 specification
 * @author SHAILJA PATIL 24CS60R49 <>
 * @details Implements a Redis-compatible server using RESP-2 protocol with epoll for high concurrency
 */

 //g++ -std=c++11 -o blinkdb blinkdb.cpp

 

 // blinkdb_server.cpp
// g++ -std=c++11 -O2 -o blinkdb_server blinkdb_server.cpp
// ./blinkdb_server --server

#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>
#include <fstream>
#include <sstream>
#include <chrono>
#include <algorithm>
#include <filesystem>
#include <random>
#include <cerrno>
#include <cstring>

// sockets / epoll headers (Linux)
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>

#define PORT 9001
#define MAX_EVENTS 10000
#define BUFFER_SIZE 4096

// ---------------------------
// DiskStorage
// ---------------------------
class DiskStorage {
private:
    std::string dataDir;
    std::unordered_map<std::string,std::string> keyToFileMap;

    std::string getFilePath(const std::string& key) {
        std::hash<std::string> hasher;
        size_t h = hasher(key);
        std::string sub = std::to_string(h % 1000);
        std::string dir = dataDir + "/" + sub;
        std::filesystem::create_directories(dir);
        // sanitize key for filename? here use key as-is; in production, escape problematic chars
        return dir + "/" + key + ".data";
    }

    void saveIndex() {
        std::string indexFile = dataDir + "/index.dat";
        std::ofstream out(indexFile, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) return;
        for (const auto &p : keyToFileMap) {
            size_t ks = p.first.size();
            size_t vs = p.second.size();
            out.write(reinterpret_cast<const char*>(&ks), sizeof(ks));
            out.write(p.first.data(), ks);
            out.write(reinterpret_cast<const char*>(&vs), sizeof(vs));
            out.write(p.second.data(), vs);
        }
    }

    void loadIndex() {
        std::string indexFile = dataDir + "/index.dat";
        std::ifstream in(indexFile, std::ios::binary);
        if (!in.is_open()) return;
        while (in.peek() != EOF) {
            size_t ks=0, ps=0;
            if (!in.read(reinterpret_cast<char*>(&ks), sizeof(ks))) break;
            std::string key(ks, '\0');
            in.read(&key[0], ks);
            in.read(reinterpret_cast<char*>(&ps), sizeof(ps));
            std::string path(ps, '\0');
            in.read(&path[0], ps);
            keyToFileMap[key] = path;
        }
    }

public:
    DiskStorage(const std::string &dir = "./blinkdb_data") : dataDir(dir) {
        std::filesystem::create_directories(dataDir);
        loadIndex();
    }
    ~DiskStorage() {
        saveIndex();
    }

    void saveToDisk(const std::string &key, const std::string &value) {
        std::string path = getFilePath(key);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        if (!out.is_open()) {
            std::cerr << "[DiskStorage] Failed to open file for key " << key << "\n";
            return;
        }
        out.write(value.data(), value.size());
        keyToFileMap[key] = path;
        // optional fsync for durability (omitted for perf)
    }

    std::string loadFromDisk(const std::string &key) {
        auto it = keyToFileMap.find(key);
        if (it == keyToFileMap.end()) return "";
        std::ifstream in(it->second, std::ios::binary);
        if (!in.is_open()) return "";
        std::stringstream ss;
        ss << in.rdbuf();
        return ss.str();
    }

    void removeFromDisk(const std::string &key) {
        auto it = keyToFileMap.find(key);
        if (it == keyToFileMap.end()) return;
        std::error_code ec;
        std::filesystem::remove(it->second, ec);
        keyToFileMap.erase(it);
    }

    bool existsOnDisk(const std::string &key) {
        return keyToFileMap.find(key) != keyToFileMap.end();
    }
};

// ---------------------------
// MultiLevelCache (hot + warm) with warm->disk persistence
// ---------------------------
class MultiLevelCache {
private:
    // L1 - hot
    std::unordered_map<std::string,std::string> hotCache;
    std::list<std::string> hotLRU;
    std::unordered_map<std::string, std::list<std::string>::iterator> hotMap;
    size_t hotMax;

    // L2 - warm
    std::unordered_map<std::string,std::string> warmCache;
    std::list<std::string> warmLRU;
    std::unordered_map<std::string, std::list<std::string>::iterator> warmMap;
    size_t warmMax;

    // access counters (simple promotion policy)
    std::unordered_map<std::string,int> accessCnt;
    const int PROMOTION_THRESHOLD = 3;

    DiskStorage &disk; // reference to disk storage (shared)

    void touchLRU(std::list<std::string> &lru, std::unordered_map<std::string,std::list<std::string>::iterator> &mp, const std::string &key) {
        auto it = mp.find(key);
        if (it != mp.end()) {
            lru.erase(it->second);
        }
        lru.push_front(key);
        mp[key] = lru.begin();
    }

    void evictFromWarm() {
        if (warmLRU.empty()) return;
        std::string k = warmLRU.back();
        std::string v = warmCache[k];
        // persist to disk
        disk.saveToDisk(k, v);
        // remove from warm
        warmCache.erase(k);
        warmMap.erase(k);
        warmLRU.pop_back();
        accessCnt.erase(k);
        // debug
        // std::cout << "[Cache] Evicted warm->disk: " << k << "\n";
    }

    void evictFromHotToWarm() {
        if (hotLRU.empty()) return;
        std::string k = hotLRU.back();
        std::string v = hotCache[k];
        // remove from hot
        hotCache.erase(k);
        hotMap.erase(k);
        hotLRU.pop_back();
        // if warm full, evict warm -> disk
        if (warmCache.size() >= warmMax) {
            evictFromWarm();
        }
        // move to warm
        warmCache[k] = v;
        warmLRU.push_front(k);
        warmMap[k] = warmLRU.begin();
    }

public:
    MultiLevelCache(size_t hotSize, size_t warmSize, DiskStorage &ds) : hotMax(hotSize), warmMax(warmSize), disk(ds) {}

    std::string get(const std::string &key) {
        auto h = hotCache.find(key);
        if (h != hotCache.end()) {
            touchLRU(hotLRU, hotMap, key);
            accessCnt[key]++;
            return h->second;
        }
        auto w = warmCache.find(key);
        if (w != warmCache.end()) {
            touchLRU(warmLRU, warmMap, key);
            accessCnt[key]++;
            if (accessCnt[key] >= PROMOTION_THRESHOLD) {
                // promote to hot
                std::string val = w->second;
                // remove from warm
                warmCache.erase(key);
                warmMap.erase(key);
                warmLRU.remove(key); // safe
                // ensure space in hot
                if (hotCache.size() >= hotMax) evictFromHotToWarm();
                hotCache[key] = val;
                hotLRU.push_front(key);
                hotMap[key] = hotLRU.begin();
                accessCnt[key] = 1;
            }
            return w->second;
        }
        return ""; // not found in memory
    }

    void set(const std::string &key, const std::string &value) {
        // if present in warm, remove (we'll put in hot)
        if (warmCache.find(key) != warmCache.end()) {
            warmCache.erase(key);
            if (warmMap.find(key) != warmMap.end()) {
                warmLRU.erase(warmMap[key]);
                warmMap.erase(key);
            }
        }
        // if already in hot, update LRU
        if (hotCache.find(key) != hotCache.end()) {
            hotCache[key] = value;
            touchLRU(hotLRU, hotMap, key);
            accessCnt[key] = 1;
            return;
        }
        // need space in hot?
        if (hotCache.size() >= hotMax) {
            evictFromHotToWarm();
        }
        // insert into hot
        hotCache[key] = value;
        hotLRU.push_front(key);
        hotMap[key] = hotLRU.begin();
        accessCnt[key] = 1;
    }

    void remove(const std::string &key) {
        if (hotCache.find(key) != hotCache.end()) {
            hotCache.erase(key);
            if (hotMap.find(key) != hotMap.end()) {
                hotLRU.erase(hotMap[key]);
                hotMap.erase(key);
            }
        }
        if (warmCache.find(key) != warmCache.end()) {
            warmCache.erase(key);
            if (warmMap.find(key) != warmMap.end()) {
                warmLRU.erase(warmMap[key]);
                warmMap.erase(key);
            }
        }
        accessCnt.erase(key);
    }

    bool existsInMemory(const std::string &key) {
        return hotCache.find(key) != hotCache.end() || warmCache.find(key) != warmCache.end();
    }

    size_t hotSize() const { return hotCache.size(); }
    size_t warmSize() const { return warmCache.size(); }
    size_t hotCapacity() const { return hotMax; }
    size_t warmCapacity() const { return warmMax; }
};

// ---------------------------
// BlinkDB storage engine
// ---------------------------
class BlinkDB {
private:
    DiskStorage disk;
    MultiLevelCache cache;
    size_t hits, misses, diskReads, diskWrites;

public:
    BlinkDB(size_t hotSize=10000, size_t warmSize=50000)
    : disk("./blinkdb_data"), cache(hotSize, warmSize, disk),
      hits(0), misses(0), diskReads(0), diskWrites(0) {}

    // set: store in memory (hot); if exists on disk remove disk copy
    void set(const std::string &key, const std::string &value) {
        if (disk.existsOnDisk(key)) {
            disk.removeFromDisk(key);
            diskWrites++; // removing disk entry counts as write-ish
        }
        cache.set(key, value);
    }

    // get: memory first; if miss and disk has it -> load & return
    std::string get(const std::string &key) {
        std::string v = cache.get(key);
        if (!v.empty()) {
            hits++;
            return v;
        }
        misses++;
        if (disk.existsOnDisk(key)) {
            std::string val = disk.loadFromDisk(key);
            if (!val.empty()) {
                // load into hot cache
                cache.set(key, val);
                diskReads++;
                return val;
            }
        }
        return ""; // not found
    }

    // del: remove from memory and disk
    bool del(const std::string &key) {
        bool removed = cache.existsInMemory(key);
        cache.remove(key);
        if (disk.existsOnDisk(key)) {
            disk.removeFromDisk(key);
            diskWrites++;
            removed = true;
        }
        return removed;
    }

    // stats
    void printStats() {
        double hitRate = (hits + misses)>0 ? (double)hits/(hits+misses)*100.0 : 0.0;
        std::cout << "=== BlinkDB Stats ===\n";
        std::cout << "Hits: " << hits << " Misses: " << misses << " HitRate: " << hitRate << "%\n";
        std::cout << "DiskReads: " << diskReads << " DiskWrites: " << diskWrites << "\n";
        std::cout << "Hot: " << cache.hotSize() << "/" << cache.hotCapacity()
                  << " Warm: " << cache.warmSize() << "/" << cache.warmCapacity() << "\n";
    }
};

// ---------------------------
// RESP-2 parsing: parses first full command from buffer and removes consumed portion
// returns empty vector if a full command not yet available
// ---------------------------
static std::vector<std::string> parse_resp_command(std::string &buf) {
    std::vector<std::string> args;
    size_t pos = 0;
    if (buf.size() < 1) return args;
    if (buf[pos] != '*') return args; // not an array (RESP) -> ignore
    size_t rn = buf.find("\r\n", pos);
    if (rn == std::string::npos) return args;
    int num = 0;
    try {
        num = std::stoi(buf.substr(1, rn - 1));
    } catch (...) { return args; }
    pos = rn + 2;
    for (int i = 0; i < num; ++i) {
        if (pos >= buf.size() || buf[pos] != '$') return std::vector<std::string>(); // incomplete
        size_t rn2 = buf.find("\r\n", pos);
        if (rn2 == std::string::npos) return std::vector<std::string>();
        int len = 0;
        try {
            len = std::stoi(buf.substr(pos+1, rn2 - pos - 1));
        } catch (...) { return std::vector<std::string>(); }
        pos = rn2 + 2;
        if (pos + len > buf.size()) return std::vector<std::string>(); // incomplete
        args.push_back(buf.substr(pos, len));
        pos += len;
        // expect CRLF after data
        if (pos + 1 >= buf.size()) return std::vector<std::string>();
        if (buf[pos] != '\r' || buf[pos+1] != '\n') return std::vector<std::string>();
        pos += 2;
    }
    // successful parse: remove consumed portion
    buf.erase(0, pos);
    return args;
}

// ---------------------------
// Helper: make fd non-blocking
// ---------------------------
static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}
//
// ---------------------------
// BlinkDBServer: epoll + RESP handling
// ---------------------------
class BlinkDBServer {
private:
    BlinkDB db;
    int server_fd;
    int epoll_fd;
    std::unordered_map<int, std::string> clientBuffers; // fd -> pending data

public:
    BlinkDBServer(size_t hotSize=10000, size_t warmSize=50000) : db(hotSize, warmSize) {}

    std::string process_command_resp(const std::vector<std::string> &cmds) {
        if (cmds.empty()) return "-ERR empty\r\n";
        std::string cmd = cmds[0];
        std::string uc = cmd;
        std::transform(uc.begin(), uc.end(), uc.begin(), ::toupper);

        if (uc == "SET") {
            if (cmds.size() < 3) return "-ERR wrong number of args for 'set'\r\n";
            db.set(cmds[1], cmds[2]);
            return "+OK\r\n";
        } else if (uc == "GET") {
            if (cmds.size() < 2) return "-ERR wrong number of args for 'get'\r\n";
            std::string v = db.get(cmds[1]);
            if (v.empty()) return "$-1\r\n";
            return "$" + std::to_string(v.size()) + "\r\n" + v + "\r\n";
        } else if (uc == "DEL") {
            if (cmds.size() < 2) return "-ERR wrong number of args for 'del'\r\n";
            bool removed = db.del(cmds[1]);
            return (removed ? ":1\r\n" : ":0\r\n");
        } else if (uc == "PING") {
            if (cmds.size() > 1) return "+" + cmds[1] + "\r\n";
            return "+PONG\r\n";
        } else if (uc == "QUIT") {
            return "+OK\r\n";
        }
        return "-ERR unknown command\r\n";
    }

    void start(int port=PORT) {
        // create server socket
        server_fd = socket(AF_INET, SOCK_STREAM, 0);
        if (server_fd == -1) { perror("socket"); exit(1); }

        // allow reuse
        int opt = 1;
        if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) == -1) {
            perror("setsockopt"); close(server_fd); exit(1);
        }

        // bind
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);
        if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) == -1) {
            perror("bind"); close(server_fd); exit(1);
        }

        if (listen(server_fd, 10000) == -1) { perror("listen"); close(server_fd); exit(1); }

        // epoll
        epoll_fd = epoll_create1(0);
        if (epoll_fd == -1) { perror("epoll_create1"); close(server_fd); exit(1); }

        struct epoll_event ev;
        ev.events = EPOLLIN;
        ev.data.fd = server_fd;
        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
            perror("epoll_ctl: server_fd"); close(server_fd); close(epoll_fd); exit(1);
        }

        std::cout << "BlinkDB server listening on port " << port << "\n";

        struct epoll_event events[MAX_EVENTS];

        while (true) {
            int n = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
            if (n == -1) {
                if (errno == EINTR) continue;
                perror("epoll_wait"); break;
            }
            for (int i = 0; i < n; ++i) {
                int fd = events[i].data.fd;
                if (fd == server_fd) {
                    // accept loop (non-blocking accept)
                    while (true) {
                        struct sockaddr_in cli;
                        socklen_t clilen = sizeof(cli);
                        int client = accept(server_fd, (struct sockaddr*)&cli, &clilen);
                        if (client == -1) {
                            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                            perror("accept");
                            break;
                        }
                        set_nonblocking(client);
                        struct epoll_event cev;
                        cev.events = EPOLLIN | EPOLLET;
                        cev.data.fd = client;
                        if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client, &cev) == -1) {
                            perror("epoll_ctl: add client");
                            close(client);
                            continue;
                        }
                        clientBuffers[client] = "";
                        // optional: print client addr
                        // char ip[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &(cli.sin_addr), ip, INET_ADDRSTRLEN);
                        // std::cout << "Accepted: " << ip << ":" << ntohs(cli.sin_port) << " fd=" << client << "\n";
                    }
                } else {
                    // read data (edge-triggered): read until EAGAIN
                    bool closed = false;
                    while (true) {
                        char buf[BUFFER_SIZE];
                        ssize_t r = read(fd, buf, sizeof(buf));
                        if (r > 0) {
                            clientBuffers[fd].append(buf, buf + r);
                        } else if (r == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                            break;
                        } else {
                            // client closed or error
                            closed = true;
                            break;
                        }
                    }
                    if (closed) {
                        // cleanup
                        clientBuffers.erase(fd);
                        close(fd);
                        continue;
                    }

                    // try to parse as many RESP commands as available
                    while (true) {
                        std::vector<std::string> args = parse_resp_command(clientBuffers[fd]);
                        if (args.empty()) break; // no complete command yet
                        std::string resp = process_command_resp(args);
                        // handle QUIT special: send reply then close
                        std::string cmd = args[0];
                        std::string uc = cmd;
                        std::transform(uc.begin(), uc.end(), uc.begin(), ::toupper);
                        ssize_t w = write(fd, resp.data(), resp.size());
                        (void)w;
                        if (uc == "QUIT") {
                            clientBuffers.erase(fd);
                            close(fd);
                            break;
                        }
                    }
                }
            }
        }

        // cleanup
        close(server_fd);
        close(epoll_fd);
    }
};

// ---------------------------
// main: run server
// ---------------------------
int main(int argc, char* argv[]) {
    if (argc > 1 && std::string(argv[1]) == "--server") {
        BlinkDBServer server;
        server.start(PORT);
        return 0;
    } else {
        std::cout << "Usage: " << argv[0] << " --server\n";
        std::cout << "Run server and use a Redis client or redis-benchmark to test.\n";
    }
    return 0;
}

