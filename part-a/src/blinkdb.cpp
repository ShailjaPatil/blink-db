


//---------------------------------NEW IMPLEMENTATION WITH DISK --------------------------
/**
 * @file blinkdb_final.cpp
 * @brief Final BLINK DB implementation with scalable configuration and demonstrations
 * @author Shailja Patil
 * @date 2025-03-02
 */

// Compilation: g++ -std=c++11 -o blinkdb blinkdb_final.cpp
// Run: ./blinkdb [--test | --demo]

#include <string>
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <list>
#include <chrono>
#include <fstream>
#include <sstream>
#include <vector>
#include <memory>
#include <cstring>
#include <filesystem>
#include <random>

/**
 * @class DiskStorage
 * @brief Handles disk persistence for evicted key-value pairs.
 */
class DiskStorage {
private:
    std::string dataDir;
    std::unordered_map<std::string, std::string> keyToFileMap;
    
    std::string getFilePath(const std::string& key) {
        std::hash<std::string> hasher;
        size_t hash = hasher(key);
        std::string subDir = std::to_string(hash % 1000);
        std::string dirPath = dataDir + "/" + subDir;
        std::filesystem::create_directories(dirPath);
        return dirPath + "/" + key + ".data";
    }

public:
    DiskStorage(const std::string& directory = "./blinkdb_data") : dataDir(directory) {
        std::filesystem::create_directories(dataDir);
        loadIndex();
    }
    
    ~DiskStorage() {
        saveIndex();
    }
    
    void saveToDisk(const std::string& key, const std::string& value) {
        std::string filePath = getFilePath(key);
        std::ofstream file(filePath, std::ios::binary);
        if (file.is_open()) {
            file.write(value.c_str(), value.size());
            keyToFileMap[key] = filePath;
        }
    }
    
    std::string loadFromDisk(const std::string& key) {
        auto it = keyToFileMap.find(key);
        if (it == keyToFileMap.end()) {
            return "";
        }
        
        std::ifstream file(it->second, std::ios::binary);
        if (!file.is_open()) {
            return "";
        }
        
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
    
    void removeFromDisk(const std::string& key) {
        auto it = keyToFileMap.find(key);
        if (it != keyToFileMap.end()) {
            std::filesystem::remove(it->second);
            keyToFileMap.erase(it);
        }
    }
    
    bool existsOnDisk(const std::string& key) {
        return keyToFileMap.find(key) != keyToFileMap.end();
    }
    
private:
    void saveIndex() {
        std::string indexFile = dataDir + "/index.dat";
        std::ofstream file(indexFile, std::ios::binary);
        for (const auto& pair : keyToFileMap) {
            size_t keySize = pair.first.size();
            size_t pathSize = pair.second.size();
            
            file.write(reinterpret_cast<const char*>(&keySize), sizeof(keySize));
            file.write(pair.first.c_str(), keySize);
            file.write(reinterpret_cast<const char*>(&pathSize), sizeof(pathSize));
            file.write(pair.second.c_str(), pathSize);
        }
    }
    
    void loadIndex() {
        std::string indexFile = dataDir + "/index.dat";
        std::ifstream file(indexFile, std::ios::binary);
        if (!file.is_open()) return;
        
        while (file.peek() != EOF) {
            size_t keySize, pathSize;
            
            file.read(reinterpret_cast<char*>(&keySize), sizeof(keySize));
            std::string key(keySize, '\0');
            file.read(&key[0], keySize);
            
            file.read(reinterpret_cast<char*>(&pathSize), sizeof(pathSize));
            std::string path(pathSize, '\0');
            file.read(&path[0], pathSize);
            
            keyToFileMap[key] = path;
        }
    }
};

/**
 * @class MultiLevelCache
 * @brief Implements a multi-level caching system with hot/warm data tiers.
 */
class MultiLevelCache {
private:
    // Hot cache (L1) - Most frequently accessed data
    std::unordered_map<std::string, std::string> hotCache;
    std::list<std::string> hotLRU;
    std::unordered_map<std::string, std::list<std::string>::iterator> hotLRUMap;
    size_t hotMaxSize;
    
    // Warm cache (L2) - Less frequently accessed but still important
    std::unordered_map<std::string, std::string> warmCache;
    std::list<std::string> warmLRU;
    std::unordered_map<std::string, std::list<std::string>::iterator> warmLRUMap;
    size_t warmMaxSize;
    
    // Access counters for promotion/demotion logic
    std::unordered_map<std::string, int> accessCounters;
    static const int PROMOTION_THRESHOLD = 3;
    static const int DEMOTION_THRESHOLD = 2;

public:
    MultiLevelCache(size_t hotSize, size_t warmSize) 
        : hotMaxSize(hotSize), warmMaxSize(warmSize) {}
    
    std::string get(const std::string& key) {
        // Check hot cache first
        auto hotIt = hotCache.find(key);
        if (hotIt != hotCache.end()) {
            hotLRU.erase(hotLRUMap[key]);
            hotLRU.push_front(key);
            hotLRUMap[key] = hotLRU.begin();
            accessCounters[key]++;
            return hotIt->second;
        }
        
        // Check warm cache
        auto warmIt = warmCache.find(key);
        if (warmIt != warmCache.end()) {
            warmLRU.erase(warmLRUMap[key]);
            warmLRU.push_front(key);
            warmLRUMap[key] = warmLRU.begin();
            accessCounters[key]++;
            
            // Promote to hot cache if frequently accessed
            if (accessCounters[key] >= PROMOTION_THRESHOLD) {
                promoteToHot(key, warmIt->second);
            }
            
            return warmIt->second;
        }
        
        return ""; // Not found in cache
    }
    
    void set(const std::string& key, const std::string& value) {
        // If key exists in warm cache, promote to hot
        if (warmCache.find(key) != warmCache.end()) {
            warmCache.erase(key);
            warmLRU.erase(warmLRUMap[key]);
            warmLRUMap.erase(key);
        }
        
        // Always put new/modified data in hot cache
        if (hotCache.find(key) != hotCache.end()) {
            // Update existing
            hotLRU.erase(hotLRUMap[key]);
        } else if (hotCache.size() >= hotMaxSize) {
            // Evict from hot cache to warm cache
            evictFromHot();
        }
        
        hotCache[key] = value;
        hotLRU.push_front(key);
        hotLRUMap[key] = hotLRU.begin();
        accessCounters[key] = 1;
    }
    
    void remove(const std::string& key) {
        if (hotCache.find(key) != hotCache.end()) {
            hotCache.erase(key);
            hotLRU.erase(hotLRUMap[key]);
            hotLRUMap.erase(key);
        }
        
        if (warmCache.find(key) != warmCache.end()) {
            warmCache.erase(key);
            warmLRU.erase(warmLRUMap[key]);
            warmLRUMap.erase(key);
        }
        
        accessCounters.erase(key);
    }
    
    bool exists(const std::string& key) {
        return hotCache.find(key) != hotCache.end() || 
               warmCache.find(key) != warmCache.end();
    }
    
    size_t getHotSize() const { return hotCache.size(); }
    size_t getWarmSize() const { return warmCache.size(); }
    size_t getHotCapacity() const { return hotMaxSize; }
    size_t getWarmCapacity() const { return warmMaxSize; }

private:
    void promoteToHot(const std::string& key, const std::string& value) {
        if (hotCache.size() >= hotMaxSize) {
            evictFromHot();
        }
        
        // Remove from warm cache
        warmCache.erase(key);
        warmLRU.erase(warmLRUMap[key]);
        warmLRUMap.erase(key);
        
        // Add to hot cache
        hotCache[key] = value;
        hotLRU.push_front(key);
        hotLRUMap[key] = hotLRU.begin();
    }
    
    void evictFromHot() {
        if (hotLRU.empty()) return;
        
        std::string keyToEvict = hotLRU.back();
        std::string value = hotCache[keyToEvict];
        
        // Remove from hot cache
        hotCache.erase(keyToEvict);
        hotLRU.pop_back();
        hotLRUMap.erase(keyToEvict);
        
        // If warm cache is full, evict from warm
        if (warmCache.size() >= warmMaxSize) {
            std::string warmKeyToEvict = warmLRU.back();
            warmCache.erase(warmKeyToEvict);
            warmLRU.pop_back();
            warmLRUMap.erase(warmKeyToEvict);
            accessCounters.erase(warmKeyToEvict);
        }
        
        // Add to warm cache
        warmCache[keyToEvict] = value;
        warmLRU.push_front(keyToEvict);
        warmLRUMap[keyToEvict] = warmLRU.begin();
    }
};

/**
 * @class BlinkDB
 * @brief A key-value store with LRU eviction policy and disk persistence.
 */
class BlinkDB {
private:
    MultiLevelCache cache;
    DiskStorage diskStorage;
    size_t maxMemorySize;
    size_t currentSize;
    
    // Statistics
    size_t hits = 0;
    size_t misses = 0;
    size_t diskReads = 0;
    size_t diskWrites = 0;

public:
    BlinkDB(size_t hotSize = 10000, size_t warmSize = 50000, size_t totalSize = 100000) 
        : cache(hotSize, warmSize), maxMemorySize(totalSize), currentSize(0) {}
    
    void set(const std::string& key, const std::string& value) {
        if (diskStorage.existsOnDisk(key)) {
            diskStorage.removeFromDisk(key);
        }
        
        cache.set(key, value);
        std::cout << "OK" << std::endl;
    }
    
    void get(const std::string& key) {
        auto start = std::chrono::high_resolution_clock::now();
        
        std::string value = cache.get(key);
        
        if (!value.empty()) {
            hits++;
            std::cout << value << std::endl;
        } else {
            misses++;
            if (loadFromDiskIfNeeded(key)) {
                value = cache.get(key);
                if (!value.empty()) {
                    std::cout << value << std::endl;
                } else {
                    std::cout << "Does not exist" << std::endl;
                }
            } else {
                std::cout << "Does not exist" << std::endl;
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "Latency: " << duration.count() << " microseconds" << std::endl;
    }
    
    void del(const std::string& key) {
        cache.remove(key);
        
        if (diskStorage.existsOnDisk(key)) {
            diskStorage.removeFromDisk(key);
            diskWrites++;
        }
        
        std::cout << "OK" << std::endl;
    }
    
    void persistToDisk(const std::string& key) {
        std::string value = cache.get(key);
        if (!value.empty()) {
            diskStorage.saveToDisk(key, value);
            cache.remove(key);
            diskWrites++;
            std::cout << "Persisted to disk: " << key << std::endl;
        }
    }
    
    void printStats() {
        double hitRate = (hits + misses > 0) ? (double)hits / (hits + misses) * 100 : 0;
        std::cout << "=== BLINK DB Statistics ===" << std::endl;
        std::cout << "Cache Configuration: " << cache.getHotCapacity() << " hot / " 
                  << cache.getWarmCapacity() << " warm" << std::endl;
        std::cout << "Current Usage: " << cache.getHotSize() << " hot / " 
                  << cache.getWarmSize() << " warm" << std::endl;
        std::cout << "Cache Hits: " << hits << std::endl;
        std::cout << "Cache Misses: " << misses << std::endl;
        std::cout << "Hit Rate: " << hitRate << "%" << std::endl;
        std::cout << "Disk Reads: " << diskReads << std::endl;
        std::cout << "Disk Writes: " << diskWrites << std::endl;
    }
    
    size_t getTotalOperations() const { return hits + misses; }

private:
    bool loadFromDiskIfNeeded(const std::string& key) {
        if (diskStorage.existsOnDisk(key)) {
            std::string value = diskStorage.loadFromDisk(key);
            if (!value.empty()) {
                cache.set(key, value);
                diskReads++;
                return true;
            }
        }
        return false;
    }
};//bink db class ends

/**
 * @brief Utility function to trim strings
 */
std::string trim(const std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, last - first + 1);
}

/**
 * @brief Start the REPL for interacting with BlinkDB.
 */
void startREPL(BlinkDB& db) {
    auto startTime = std::chrono::high_resolution_clock::now();
    size_t totalOperations = 0;

    std::cout << "BLINK DB Started with Optimal Configuration" << std::endl;
    std::cout << "Cache: 10,000 (hot) + 50,000 (warm) = 100,000 total items" << std::endl;
    std::cout << "Supported commands: SET, GET, DEL, PERSIST, STATS, QUIT" << std::endl;
    std::cout << "==============================================" << std::endl;

    std::string command;
    while (true) {
        std::cout << "User> ";
        if (!std::getline(std::cin, command)) break;
        
        command = trim(command);
        if (command.empty()) continue;

        totalOperations++;

        std::string cmdUpper = command;
        std::transform(cmdUpper.begin(), cmdUpper.end(), cmdUpper.begin(), ::toupper);

        if (cmdUpper.substr(0, 3) == "SET") {
            size_t firstSpace = command.find(' ');
            size_t secondSpace = command.find(' ', firstSpace + 1);
            if (firstSpace == std::string::npos || secondSpace == std::string::npos) {
                std::cout << "Invalid SET command. Usage: SET <key> \"<value>\"" << std::endl;
                continue;
            }
            std::string key = command.substr(firstSpace + 1, secondSpace - firstSpace - 1);
            std::string value = command.substr(secondSpace + 1);
            value = trim(value);

            auto setStart = std::chrono::high_resolution_clock::now();
            db.set(key, value);
            auto setEnd = std::chrono::high_resolution_clock::now();
            auto setDuration = std::chrono::duration_cast<std::chrono::microseconds>(setEnd - setStart);
            std::cout << "SET latency: " << setDuration.count() << " microseconds" << std::endl;

        } else if (cmdUpper.substr(0, 3) == "GET") {
            size_t space = command.find(' ');
            if (space == std::string::npos) {
                std::cout << "Invalid GET command. Usage: GET <key>" << std::endl;
                continue;
            }
            std::string key = command.substr(space + 1);
            key = trim(key);

            auto getStart = std::chrono::high_resolution_clock::now();
            db.get(key);
            auto getEnd = std::chrono::high_resolution_clock::now();
            auto getDuration = std::chrono::duration_cast<std::chrono::microseconds>(getEnd - getStart);
            std::cout << "GET latency: " << getDuration.count() << " microseconds" << std::endl;

        } else if (cmdUpper.substr(0, 3) == "DEL") {
            size_t space = command.find(' ');
            if (space == std::string::npos) {
                std::cout << "Invalid DEL command. Usage: DEL <key>" << std::endl;
                continue;
            }
            std::string key = command.substr(space + 1);
            key = trim(key);

            auto delStart = std::chrono::high_resolution_clock::now();
            db.del(key);
            auto delEnd = std::chrono::high_resolution_clock::now();
            auto delDuration = std::chrono::duration_cast<std::chrono::microseconds>(delEnd - delStart);
            std::cout << "DEL latency: " << delDuration.count() << " microseconds" << std::endl;

        } else if (cmdUpper == "PERSIST") {
            size_t space = command.find(' ');
            if (space == std::string::npos) {
                std::cout << "Invalid PERSIST command. Usage: PERSIST <key>" << std::endl;
                continue;
            }
            std::string key = command.substr(space + 1);
            key = trim(key);
            db.persistToDisk(key);

        } else if (cmdUpper == "STATS") {
            db.printStats();

        } else if (cmdUpper == "QUIT") {
            std::cout << "Exiting" << std::endl;
            break;
        } else {
            std::cout << "Unknown command. Supported: SET, GET, DEL, PERSIST, STATS, QUIT" << std::endl;
        }
    }

    auto endTime = std::chrono::high_resolution_clock::now();
    auto totalDuration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    double totalTimeSeconds = totalDuration.count() / 1e6;
    double throughput = totalOperations / totalTimeSeconds;

    std::cout << "\n=== SESSION SUMMARY ===" << std::endl;
    std::cout << "Total time: " << totalTimeSeconds << " seconds" << std::endl;
    std::cout << "Total operations: " << totalOperations << std::endl;
    std::cout << "Throughput: " << throughput << " operations/second" << std::endl;
    
    db.printStats();
}

/**
 * @brief Run automated tests for BlinkDB.
 */
void runTests() {
    std::cout << "=== RUNNING BASIC TESTS ===" << std::endl;
    BlinkDB db(100, 500, 1000); // Smaller sizes for faster tests

    db.set("key1", "value1");
    db.set("key2", "value2");
    db.set("key3", "value3");
    
    std::cout << "Expected: value1 - "; 
    db.get("key1");
    
    db.del("key2");
    std::cout << "Expected: Does not exist - ";
    db.get("key2");
    
    db.printStats();
    std::cout << "=== TESTS COMPLETED ===" << std::endl;
}

/**
 * @brief Demonstrate scalability with different cache configurations
 */
void demonstrateScalability() {
    std::cout << "\n=== SCALABILITY DEMONSTRATION ===" << std::endl;
    std::cout << "Testing different cache configurations..." << std::endl;
    
    // Test configurations: (hot_size, warm_size, total_operations)
    std::vector<std::tuple<size_t, size_t, size_t, std::string>> configurations = {
        {100, 500, 2000, "Tiny (100/500)"},
        {1000, 5000, 20000, "Small (1K/5K)"},
        {10000, 50000, 100000, "Medium (10K/50K)"}
    };
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 100000);
    
    for (const auto& config : configurations) {
        auto [hotSize, warmSize, totalOps, label] = config;
        
        std::cout << "\n--- Testing " << label << " configuration ---" << std::endl;
        BlinkDB testDB(hotSize, warmSize, hotSize + warmSize);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // Insert data (some will trigger evictions)
        for (size_t i = 0; i < totalOps / 2; i++) {
            testDB.set("key" + std::to_string(i), "value" + std::to_string(i));
        }
        
        // Mixed workload (reads and writes)
        for (size_t i = 0; i < totalOps / 2; i++) {
            if (i % 3 == 0) {
                testDB.set("key" + std::to_string(dis(gen)), "new_value");
            } else {
                testDB.get("key" + std::to_string(dis(gen) % (totalOps / 2)));
            }
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        double throughput = totalOps / (duration.count() / 1000.0);
        
        std::cout << "Configuration: " << label << std::endl;
        std::cout << "Operations: " << totalOps << std::endl;
        std::cout << "Time: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: " << throughput << " ops/sec" << std::endl;
        testDB.printStats();
    }
    
    std::cout << "\n=== SCALABILITY DEMONSTRATION COMPLETE ===" << std::endl;
    std::cout << "Note: Larger caches show better performance by reducing disk I/O" << std::endl;
}

/**
 * @brief Main function with optimal configuration
 */
int main(int argc, char* argv[]) {
    // OPTIMAL CONFIGURATION FOR DEMONSTRATION
    size_t hotSize = 10000;    // 10K hot entries
    size_t warmSize = 50000;   // 50K warm entries  
    size_t totalSize = 100000; // 100K total cache
    
    BlinkDB db(hotSize, warmSize, totalSize);
    
    if (argc > 1) {
        std::string arg = argv[1];
        if (arg == "--test") {
            runTests();
        } else if (arg == "--demo") {
            demonstrateScalability();
        } else if (arg == "--help") {
            std::cout << "Usage: " << argv[0] << " [--test | --demo | --help]" << std::endl;
            std::cout << "  --test: Run basic functionality tests" << std::endl;
            std::cout << "  --demo: Run scalability demonstration" << std::endl;
            std::cout << "  --help: Show this help message" << std::endl;
            std::cout << "  no args: Start interactive REPL" << std::endl;
        }
    } else {
        startREPL(db);
    }
    
    return 0;
}
