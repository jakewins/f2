package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

class F2LockClient implements Locks.Client {

    private final F2Partitions partitions;

    F2LockClient(F2Partitions partitions) {
        this.partitions = partitions;
    }

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
        LockEntry entry = newLockEntry(mode, resourceType, resourceId);
        F2Partition partition = partitions.getPartition(resourceId);

        F2Lock.Outcome outcome = partition.acquire(acquireMode, entry);

        return null;
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {

    }

    private LockEntry newLockEntry(LockMode mode, ResourceType resourceType, long resourceId) {
        LockEntry entry = new LockEntry();
        entry.lockMode = mode;
        entry.resourceType = resourceType;
        entry.resourceId = resourceId;
        return entry;
    }
}
