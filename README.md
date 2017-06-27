# F2

An experimental lock manager for Neo4j 3.2.x and 3.3.x.

> # NOTE
> This is a personal proof of concept, it is not in any way supported by Neo Technology or anyone else.
> This code is not production ready. Bugs in here may cause irrecoverable data corruption, use at your own risk.

## Usage

- Put `f2.jar` in the `plugins` directory of your Neo4j 3.2.x or 3.3.x system.
- Add `unsupported.dbms.lock_manager=f2` to your `neo4j.conf`
- Restart Neo4j

## Configuration

F2 provides one configuration option to tune the number of partitions to use, a higher number 
here means more memory overhead but less contention:

    unsupported.dbms.f2.partitions=128

The partition number must be a factor of 2.

## Overview

F2 is a partitioned lock manager, allowing concurrent work to happen in separate partitions, but
ordering accesses within one partition using a semaphore. 

F2 offers several nice features:

- Deterministic deadlock detection with no false positives
- Exact deadlock descriptions
- Thread notification-based waiting, using substantially less system resources than Forseti

Good place to start reading is in [F2Client#acquire](src/main/java/com/jakewins/f2/F2Client.java).

## License

AGPL
