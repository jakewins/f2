package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
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
    NOT_ACQUIRED
}

public class F2Locks implements Locks {
    public Client newClient() {
        return new F2LockClient();
    }

    public void close() {

    }
}

