package com.jakewins.f2;

import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.ResourceType;

import java.util.concurrent.atomic.AtomicLong;

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
    private AtomicLong clientCounter = new AtomicLong();

    public F2Locks(ResourceType[] resourceTypes, int numPartitions) {
        this.resourceTypes = resourceTypes;
        this.partitions = new F2Partitions(resourceTypes.length, numPartitions);
        this.deadlockDetector = new DeadlockDetector();
    }

    @Override
    public Client newClient() {
        F2Client client = new F2Client(resourceTypes.length, partitions, deadlockDetector);
        client.setName(String.format("%d", clientCounter.getAndIncrement()));
        return client;
    }

    @Override
    public void accept(Visitor visitor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }
}

