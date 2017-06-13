package com.jakewins.f2;

import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;
import com.sun.xml.internal.bind.v2.model.runtime.RuntimeElement;

enum LockMode {
    EXCLUSIVE,
    SHARED
}

enum AcquireMode {
    BLOCKING,
    NONBLOCKING
}

class AcquireOutcome {
    final static AcquireOutcome ACQUIRED = new AcquireOutcome();
    final static AcquireOutcome NOT_ACQUIRED = new AcquireOutcome();
}

class Deadlock extends AcquireOutcome {
    private final String description;

    Deadlock(String description) {
        this.description = description;
    }

    public String deadlockDescription() {
        return description;
    }
}

class AcquireError extends AcquireOutcome {
    private final Exception cause;

    AcquireError(Exception cause) {
        this.cause = cause;
    }

    public RuntimeException asRuntimeException() {
        if(cause instanceof RuntimeException) {
            return (RuntimeException)cause;
        }
        return new RuntimeException(cause);
    }
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

