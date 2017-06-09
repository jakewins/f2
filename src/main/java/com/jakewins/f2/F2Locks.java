package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

public class F2Locks implements Locks {
    class ClientLockData {
        /** Next client holding same lock */
        ClientLockData nextForLock;
        /** Next lock held by same client */
        ClientLockData nextForClient;
    }

    public Client newClient() {
        return new F2LockClient();
    }

    public void close() {

    }
}

enum AcquireMode {
    BLOCKING,
    NONBLOCKING
}

enum AcquireOutcome {
    ACQUIRED,
    NOT_ACQUIRED
}

enum LockMode {
    EXCLUSIVE,
    SHARED
}

class F2LockClient implements Locks.Client {
    public void acquireShared(ResourceType resourceType, long resourceId) throws AcquireLockTimeoutException {
        acquire(AcquireMode.BLOCKING, LockMode.SHARED, resourceType, resourceId);
    }

    public void acquireExclusive(ResourceType resourceType, long resourceId) throws AcquireLockTimeoutException {
        acquire(AcquireMode.BLOCKING, LockMode.EXCLUSIVE, resourceType, resourceId);
    }

    public void releaseShared(ResourceType resourceType, long resourceId) {
        release(LockMode.SHARED, resourceType, resourceId);
    }

    public void releaseExclusive(ResourceType resourceType, long resourceId) {
        release(LockMode.EXCLUSIVE, resourceType, resourceId);
    }

    public boolean trySharedLock(ResourceType resourceType, long resourceId) {
        return acquire(AcquireMode.NONBLOCKING, LockMode.SHARED, resourceType, resourceId) == AcquireOutcome.ACQUIRED;
    }

    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        return acquire(AcquireMode.NONBLOCKING, LockMode.EXCLUSIVE, resourceType, resourceId) == AcquireOutcome.ACQUIRED;
    }

    public boolean reEnterShared(ResourceType resourceType, long resourceId) {
        throw new UnsupportedOperationException("Sorry.");
    }

    public boolean reEnterExclusive(ResourceType resourceType, long resourceId) {
        throw new UnsupportedOperationException("Sorry.");
    }


    public void stop() {
        throw new UnsupportedOperationException("Sorry.");
    }

    public void close() {

    }

    public int getLockSessionId() {
        throw new UnsupportedOperationException("Sorry.");
    }

    public long activeLockCount() {
        throw new UnsupportedOperationException("Sorry.");
    }

    private AcquireOutcome acquire(AcquireMode acquireMode, LockMode mode, ResourceType resourceType, long resourceId) {
        return null;
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {

    }
}

class F2Lock {
    enum Outcome {
        ACQUIRED,
        NOT_ACQUIRED,
        /** Caller is on waitlist, *has to* wait until it's off of it */
        MUST_WAIT
    }

    /** Single entry of current exclusive holder */
    private LockEntry exclusiveHolder = null;

    /** Linked list of holders of shared lock */
    private LockEntry sharedHolderList = null;

    /** Linked list of waiting holders */
    private LockEntry waitList = null;

    // NOTE: Must hold partition lock before calling
    public Outcome sharedLock(LockEntry entry, AcquireMode mode) {
        if(exclusiveHolder != null) {
            return handleAcquireFailed(entry, mode);
        }

        entry.nextHolder = sharedHolderList;
        sharedHolderList = entry;
        return Outcome.ACQUIRED;
    }

    // NOTE: Must hold partition lock before calling
    public Outcome exclusiveLock(LockEntry entry, AcquireMode mode) {
        if(exclusiveHolder != null || sharedHolderList != null) {
            return handleAcquireFailed(entry, mode);
        }

        exclusiveHolder = entry;
        return Outcome.ACQUIRED;
    }

    private Outcome handleAcquireFailed(LockEntry entry, AcquireMode mode) {
        if(mode == AcquireMode.BLOCKING) {
            entry.nextHolder = waitList;
            waitList = entry;
            return Outcome.MUST_WAIT;
        }
        return Outcome.NOT_ACQUIRED;
    }
}

/** Tracks one client's holding of one lock */
class LockEntry {
    /**
     * If the lock is held: The next entry holding the same lock
     * If the lock is waited on: The next entry waiting for the same lock
     */
    LockEntry nextHolder = null;
}