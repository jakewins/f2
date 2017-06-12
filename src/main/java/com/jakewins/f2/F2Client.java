package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

class F2Client implements Locks.Client {
    private static final int CHECK_DEADLOCK_AFTER_MS = 1000;

    private final F2Partitions partitions;

    Semaphore latch = new Semaphore(0);

    F2Client(F2Partitions partitions) {
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
        return acquire(AcquireMode.NONBLOCKING, LockMode.SHARED, resourceType, resourceId) == com.jakewins.f2.AcquireOutcome.ACQUIRED;
    }

    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        return acquire(AcquireMode.NONBLOCKING, LockMode.EXCLUSIVE, resourceType, resourceId) == com.jakewins.f2.AcquireOutcome.ACQUIRED;
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

        F2Partition partition = partitions.getPartition(resourceId);
        F2Lock newLock = partition.nextFreeLock != null ? partition.nextFreeLock : new F2Lock();
        F2ClientEntry entry = partition.newClientEntry(this, mode, resourceType, resourceId);

        F2Lock lock;
        F2Lock.AcquireOutcome outcome;
        long stamp = partition.partitionLock.writeLock();
        try {
            lock = partition.putIfAbsent(resourceType, resourceId, newLock);

            // If we ended up inserting the lock we got off of the partition freelist, we need to update the freelist
            if(lock == partition.nextFreeLock) {
                partition.nextFreeLock = lock.nextFree;
                lock.nextFree = null;
            }

            outcome = lock.acquire(acquireMode, entry);

            if(outcome == F2Lock.AcquireOutcome.ACQUIRED) {
                return com.jakewins.f2.AcquireOutcome.ACQUIRED;
            }

            if(outcome == F2Lock.AcquireOutcome.NOT_ACQUIRED) {
                partition.releaseClientEntry(entry);
                return com.jakewins.f2.AcquireOutcome.NOT_ACQUIRED;
            }
        } finally {
            partition.partitionLock.unlock(stamp);
        }

        try {
            assert outcome == F2Lock.AcquireOutcome.MUST_WAIT;
            // At this point, we are on the wait list for the lock we want, and we *have* to wait for it.
            // The way this works is that, eventually, someone ahead of us on the wait list will grant us the lock
            // and wake us up via {@link latch}. Until then, we wait.

            for(;;) {
                latch.tryAcquire(CHECK_DEADLOCK_AFTER_MS, TimeUnit.MILLISECONDS);
            }
        } catch(Exception e) {
            // Current thread was interrupted while waiting on a lock, not good.
            // We are on the wait list for the lock, so we can't simply leave, need cleanup.
            errorCleanupWhileOnWaitList(partition, entry, lock);
        }


        return null;
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {

    }

    private void errorCleanupWhileOnWaitList(F2Partition partition, F2ClientEntry entry, F2Lock lock) {
        long stamp = partition.partitionLock.writeLock();
        try {
            lock.errorCleanup(entry);
            partition.releaseClientEntry(entry);
        } finally {
            partition.partitionLock.unlock(stamp);
        }
    }
}
