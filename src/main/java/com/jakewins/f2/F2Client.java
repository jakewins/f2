package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

class F2Client implements Locks.Client {
    private static final int CHECK_DEADLOCK_AFTER_MS = 1000;

    /** Signal when client is granted a lock it is waiting on */
    Semaphore latch = new Semaphore(0);
    /** Lock entry this client is currently waiting on, or null */
    F2ClientEntry waitsFor;

    private String name;
    private final F2Partitions partitions;
    private final DeadlockDetector deadlockDetector;

    F2Client(F2Partitions partitions, DeadlockDetector deadlockDetector) {
        this.partitions = partitions;
        this.deadlockDetector = deadlockDetector;
    }

    public void setName(String name) {
        this.name = name;
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

    @Override
    public String toString() {
        return name;
    }

    private AcquireOutcome acquire(AcquireMode acquireMode, LockMode mode, ResourceType resourceType, long resourceId) {

        F2Partition partition = partitions.getPartition(resourceId);
        F2ClientEntry entry;

        F2Lock lock;
        F2Lock.AcquireOutcome outcome;
        long stamp = partition.partitionLock.writeLock();
        try {
            entry = partition.newClientEntry(this, mode, resourceType, resourceId);

            lock = partition.getOrCreate(resourceType, resourceId);

            outcome = lock.acquire(acquireMode, entry);

            if(outcome == F2Lock.AcquireOutcome.ACQUIRED) {
                return com.jakewins.f2.AcquireOutcome.ACQUIRED;
            }

            if(outcome == F2Lock.AcquireOutcome.NOT_ACQUIRED) {
                partition.releaseClientEntry(entry);
                return com.jakewins.f2.AcquireOutcome.NOT_ACQUIRED;
            }

            waitsFor = entry;
        } finally {
            partition.partitionLock.unlock(stamp);
        }

        try {
            assert outcome == F2Lock.AcquireOutcome.MUST_WAIT;
            // At this point, we are on the wait list for the lock we want, and we *have* to wait for it.
            // The way this works is that, eventually, someone ahead of us on the wait list will grant us the lock
            // and wake us up via {@link latch}. Until then, we wait.

            for(;;) {
                boolean gotLock = latch.tryAcquire(CHECK_DEADLOCK_AFTER_MS, TimeUnit.MILLISECONDS);
                if(gotLock) {
                    // Someone told us we got the lock!
                    return AcquireOutcome.ACQUIRED;
                } else {
                    // We timed out; need to do deadlock detection
                    detectDeadlock(lock);
                }
            }
        } catch(Exception e) {
            // Current thread was interrupted while waiting on a lock, not good.
            // We are on the wait list for the lock, so we can't simply leave, need cleanup.
            errorCleanupWhileOnWaitList(partition, entry, lock);
        }

        return null;
    }

    private void detectDeadlock(F2Lock lock) {
        partitions.stopTheWorld();
        try {
            deadlockDetector.detectDeadlock(this); // TODO in medias
        } finally {
            partitions.resumeTheWorld();
        }
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private void errorCleanupWhileOnWaitList(F2Partition partition, F2ClientEntry entry, F2Lock lock) {
        ResourceType resourceType = entry.resourceType;
        long resourceId = entry.resourceId;

        long stamp = partition.partitionLock.writeLock();
        try {
            F2Lock.ReleaseOutcome outcome = lock.errorCleanup(entry);
            if(outcome == F2Lock.ReleaseOutcome.LOCK_IDLE) {
                // If the lock ended up idle, we need to remove it from the lock table before wrapping up
                partition.removeLock(resourceType, resourceId);
            }
            partition.releaseClientEntry(entry);

            // After removing ourselves from the wait list, we know nobody will undo our latch anymore;
            // however, we don't know that someone didn't already, before we grabbed the partition lock.
            // Hence, under the partition lock, bring the latch back to a known state.
            latch.drainPermits();
        } finally {
            partition.partitionLock.unlock(stamp);
        }
    }
}
