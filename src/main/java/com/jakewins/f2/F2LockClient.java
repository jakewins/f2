package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

/**
 * Created by jakewins on 6/9/17.
 */
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
