# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++11

# Target executable
TARGET = blinkdb

# Source files
SRCS = blinkdb.cpp

# Object files
OBJS = $(SRCS:.cpp=.o)

# Benchmark files
BENCHMARKS = read_heavy.txt write_heavy.txt balanced.txt

# Default target
all: $(TARGET)

# Rule to build the target executable
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

# Rule to compile source files into object files
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# Target to run all benchmarks - fixed by adding $(TARGET) dependency
run: $(TARGET)
	@echo "Running all benchmarks..."
	@for benchmark in $(BENCHMARKS); do \
		if [ -f "$$benchmark" ]; then \
			echo "=== Running $$benchmark ==="; \
			./$(TARGET) < $$benchmark; \
			echo "============================"; \
		else \
			echo "Benchmark file $$benchmark not found!"; \
		fi \
	done

# Target to run automated tests
test: $(TARGET)
	./$(TARGET) --test

# Target to clean build files
clean:
	rm -f $(OBJS) $(TARGET)

# Phony targets (not actual files)
.PHONY: all run test clean