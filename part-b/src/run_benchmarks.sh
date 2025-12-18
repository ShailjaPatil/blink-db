#!/bin/bash

mkdir -p results

for conns in 10 100 1000; do
    for reqs in 10000 100000 1000000; do
        output_file="results/result_${reqs}_${conns}.txt"
        
        echo "Creating benchmark file: $output_file"
        
        # Run benchmark and redirect all output to file
        redis-benchmark -p 9001 -t set,get -n $reqs -c $conns > "$output_file" 2>&1
        
        echo "Completed: $output_file"
        echo "----------------------------------"
    done
done