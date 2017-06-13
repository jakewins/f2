package com.jakewins.f2;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.ResourceType;

enum LockMode {
    EXCLUSIVE(0),
    SHARED(1);

    final int index;

    LockMode(int index) {
        this.index = index;
    }

    public static int numberOfModes() {
        return LockMode.values().length;
    }
}

enum AcquireMode {
    BLOCKING,
    NONBLOCKING
}

public class F2Locks implements Locks {
    private final F2Partitions partitions;
    private final DeadlockDetector deadlockDetector;
    private final ResourceType[] resourceTypes;

    public F2Locks(ResourceType[] resourceTypes, int numPartitions) {
        this.resourceTypes = resourceTypes;
        this.partitions = new F2Partitions(resourceTypes.length, numPartitions);
        this.deadlockDetector = new DeadlockDetector();
    }

    public Client newClient() {
        return new F2Client(resourceTypes.length, partitions, deadlockDetector);
    }

    @Override
    public void accept(Visitor visitor) {
        throw new UnsupportedOperationException();
    }

    public void close() {

    }
}

