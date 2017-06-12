package com.jakewins.f2;

import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

enum LockMode {
    EXCLUSIVE,
    SHARED
}

enum AcquireMode {
    BLOCKING,
    NONBLOCKING
}

enum AcquireOutcome {
    ACQUIRED,
    NOT_ACQUIRED,
    TIMEOUT
}

public class F2Locks implements Locks {
    private final F2Partitions partitions;
    private final DeadlockDetector deadlockDetector;

    public F2Locks(ResourceType[] resourceTypes, int numPartitions) {
        this.partitions = new F2Partitions(resourceTypes.length, numPartitions);
        this.deadlockDetector = new DeadlockDetector();
    }

    public Client newClient() {
        return new F2Client(partitions, deadlockDetector);
    }

    public void close() {

    }
}

