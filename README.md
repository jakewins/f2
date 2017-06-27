[![Build Status](https://travis-ci.org/jakewins/f2.svg?branch=master)](https://travis-ci.org/jakewins/f2)

# F2

An experimental lock manager for Neo4j 3.2.x and 3.3.x.

> ### NOTE
> This is a personal proof of concept, it is not in any way supported by Neo Technology or anyone else.
> This code is not production ready. Bugs in here may cause irrecoverable data corruption, use at your own risk.

## Usage

- Put [f2.jar](https://github.com/jakewins/f2/releases/download/0.0.1/f2.jar) in the `plugins` directory of your Neo4j 3.2.x or 3.3.x system.
- Add `unsupported.dbms.lock_manager=f2` to your `neo4j.conf`
- Restart Neo4j

## Configuration

F2 provides one configuration option to tune the number of partitions to use, a higher number 
here means more memory overhead but less contention:

    unsupported.dbms.f2.partitions=128

The partition number must be a factor of 2.

## Building

    mvn clean package -P build-extension

## Overview

F2 is a partitioned lock manager, allowing concurrent work to happen in separate partitions, but
ordering accesses within one partition using a semaphore. 

F2 offers several nice features:

- Deterministic deadlock detection with no false positives
- Exact deadlock descriptions
- Thread notification-based waiting, using substantially less system resources than Forseti

Good place to start reading is in [F2Client#acquire](src/main/java/com/jakewins/f2/F2Client.java).

## Performance

Please see [F2Locks_PerfTest](src/main/java/com/jakewins/f2/F2Locks_PerfTest.java) for micro benchmarks.

Performance compared to Forseti at `d69c50c` on `Intel(R) Core(TM) i7-6600U CPU @ 2.60GHz`:

    Benchmark                                             Mode  Cnt     Score      Error   Units
    F2Locks_PerfTest.f2AcquireContendedExclusive         thrpt    5   528.829 ±   89.808  ops/ms
    F2Locks_PerfTest.forsetiAcquireContendedExclusive    thrpt    5  1213.495 ±  252.209  ops/ms
    
    F2Locks_PerfTest.f2AcquireContendedShared            thrpt    5  9645.799 ± 1132.557  ops/ms
    F2Locks_PerfTest.forsetiAcquireContendedShared       thrpt    5  5540.280 ± 1330.020  ops/ms
    
    F2Locks_PerfTest.f2SchemaExclusiveCombintation       thrpt    5    12.366 ±   30.350  ops/ms
    F2Locks_PerfTest.forsetiSchemaExclusiveCombintation  thrpt    5     6.329 ±    2.242  ops/ms

## License

AGPL
