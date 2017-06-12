package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;
import com.sun.org.apache.bcel.internal.generic.F2L;

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
                boolean latchTripped = latch.tryAcquire(CHECK_DEADLOCK_AFTER_MS, TimeUnit.MILLISECONDS);
                if(latchTripped) {
                    // Someone told us we got the lock!
                    return AcquireOutcome.ACQUIRED;
                } else {
                    // We timed out; need to do deadlock detection
                    detectDeadlock();
                }
            }
        } catch(Exception e) {
            // Current thread was interrupted while waiting on a lock, not good.
            // We are on the wait list for the lock, so we can't simply leave, need cleanup.
            errorCleanupWhileOnWaitList(partition, entry);
        }

        return null;
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    private void errorCleanupWhileOnWaitList(F2Partition partition, F2ClientEntry entry) {
        long stamp = partition.partitionLock.writeLock();
        try {
            cleanUpErrorWhileWaiting_locked(partition, entry);
        } finally {
            partition.partitionLock.unlock(stamp);
        }
    }

    private void detectDeadlock() {
        partitions.stopTheWorld();
        try {
            DeadlockDescription deadlock = deadlockDetector.detectDeadlock(this);
            if(deadlock == DeadlockDetector.NONE) {
                return;
            }

            // TODO: We could easily tell any waiter in the deadlock chain to abort by signalling;
            //       eg. we could abort a client with lower prio than us, or whatever.
            // For now, abort the client that firsts discovers the deadlock
            F2Partition partition = partitions.getPartition(waitsFor.resourceId);
            cleanUpErrorWhileWaiting_locked(partition, waitsFor);
        } finally {
            partitions.resumeTheWorld();
        }
    }

    /**
     * NOTE: Must hold at least partition lock
     */
    private void cleanUpErrorWhileWaiting_locked(F2Partition partition, F2ClientEntry entry) {
        ResourceType resourceType = entry.resourceType;
        long resourceId = entry.resourceId;

        F2Lock.ReleaseOutcome outcome = entry.lock.errorCleanup(entry);
        if(outcome == F2Lock.ReleaseOutcome.LOCK_IDLE) {
            // If the lock ended up idle, we need to remove it from the lock table before wrapping up
            partition.removeLock(resourceType, resourceId);
        }
        partition.releaseClientEntry(entry);

        // After removing ourselves from the wait list, we know nobody will undo our latch anymore;
        // however, we don't know that someone didn't already, before we grabbed the partition lock.
        // Hence, under the partition lock, bring the latch back to a known state.
        latch.drainPermits();
    }
}
