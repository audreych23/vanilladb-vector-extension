# Final Project

Extend a relational database system to support storing and querying over vectors. 

## Change Log

- Recall calculation is now fast by assuming data fits in memory. However, you will not get any points by attempting to use the provided code for recall calculation.

## How to Run
This is a final project of NTHU CS 471000 Introduction to Database Management System course, so vanilladb is used as a base for this project
1. Start the server
2. Load the provided Approximate Nearest Neighbor (ANN) dataset
3. Stop the server to flush all the changes 
4. Restart the server
5. Run the provided ANN benchmark
6. Check the benchmark result

Note: Your improvement will be evaluated on the provided properties (10k items with 48-dimensional vector embedding each)

## Hints

- We will only use `HeuristicQueryPlanner` for our vector search operations.
- Our naive implementation sorts all the vector and return the top-k closest records to the client.
- You can easily beat our performance by implementing any indexing algorithms for the vector search. Note that you still have to consider correctness because we will measure recall.
- Make sure `TablePlanner` calls your index, if you choose to implement one.
- You can look into `org.vanilladb.core.sql.distfn.EuclideanFn` to implement SIMD. Note: Our benchmark will only use `EuclideanFn`. You may choose not to implement SIMD for CosineFn.
- Make sure you run java with `add-modules jdk.incubator.vector` flag to enable SIMD in Java.

## Experiments

Based on the workload we provide, show the followings:
- Throughput
- Recall

Show the comparison between the performance of the unmodified source code and the performance of your modification.

You can then think about the parameter settings that really show your improvements.

## Report

- Briefly explain what has been done
    - How you implement your indexes
    - How you implement SIMD
    - Other improvements you made to speed up the search

- Experiments
    - Your experiment environment (a list of your hardware components, your operating system)
        - e.g. Intel Core i5-3470 CPU @ 3.2GHz, 16 GB RAM, 128 GB SSD, CentOS 7
    - Based on the workload we provide:
        - Show your improvement using graphs
    - Your benchmark parameters
    - Analysis on the results of the experiments
<<<<<<< HEAD

## Reference Paper
https://dl.acm.org/doi/pdf/10.1145/3318464.3386131

P.s This project was version controlled in gitlab before, however no commits are transferred

