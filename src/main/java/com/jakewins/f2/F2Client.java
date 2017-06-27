package com.jakewins.f2;

import com.jakewins.f2.F2Lock.AcquireOutcome;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.jakewins.f2.infrastructure.SingleWaiterLatch;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;

class ClientAcquireOutcome {
    final static ClientAcquireOutcome ACQUIRED = new ClientAcquireOutcome();
    final static ClientAcquireOutcome NOT_ACQUIRED = new ClientAcquireOutcome();
}

class Deadlock extends ClientAcquireOutcome {
    private final String description;

    Deadlock(String description) {
        this.description = description;
    }

    String deadlockDescription() {
        return description;
    }
}

class ClientAcquireError extends ClientAcquireOutcome {
    private final Throwable cause;

    ClientAcquireError(Throwable cause) {
        this.cause = cause;
    }

    RuntimeException asRuntimeException() {
        if(cause instanceof RuntimeException) {
            return (RuntimeException)cause;
        }
        return new RuntimeException(cause);
    }
}

class F2Client implements Locks.Client {
    private static final int CHECK_DEADLOCK_AFTER_MS = 1000;
    private static AtomicInteger ID_GEN = new AtomicInteger();

    /** Signal when client is granted a lock it is waiting on */
    SingleWaiterLatch latch = new SingleWaiterLatch();

    /**
     * Lock entry this client is currently waiting on, or null; this is set by the lock when we're added to
     * wait list, cleared when someone grants us the lock.
     *
     * NOTE: Must hold partition lock of the entry to write to this
     */
    F2ClientEntry waitsFor;

    private int clientId = ID_GEN.incrementAndGet();
    private String name;

    private final F2Partitions partitions;
    private final DeadlockDetector deadlockDetector;
    private final F2ClientLocks heldLocks;

    F2Client(int numResourceTypes, F2Partitions partitions, DeadlockDetector deadlockDetector) {
        this.partitions = partitions;
        this.deadlockDetector = deadlockDetector;
        this.heldLocks = new F2ClientLocks(numResourceTypes);
    }

    @Override
    public void acquireShared(LockTracer lockTracer, ResourceType resourceType, long... resourceIds) throws AcquireLockTimeoutException {
        for(long resourceId : resourceIds) {
            ClientAcquireOutcome outcome = acquire(AcquireMode.BLOCKING, LockMode.SHARED, resourceType, resourceId);
            handleAcquireOutcome(outcome);
        }
    }

    @Override
    public void acquireExclusive(LockTracer lockTracer, ResourceType resourceType, long... resourceIds) throws AcquireLockTimeoutException {
        for(long resourceId : resourceIds) {
            ClientAcquireOutcome outcome = acquire(AcquireMode.BLOCKING, LockMode.EXCLUSIVE, resourceType, resourceId);
            handleAcquireOutcome(outcome);
        }
    }

    private static void handleAcquireOutcome(ClientAcquireOutcome outcome) {
        if(outcome == ClientAcquireOutcome.ACQUIRED || outcome == ClientAcquireOutcome.NOT_ACQUIRED) {
            return;
        }
        if(outcome instanceof Deadlock) {
            throw new DeadlockDetectedException(((Deadlock) outcome).deadlockDescription());
        }
        if(outcome instanceof ClientAcquireError) {
            throw ((ClientAcquireError) outcome).asRuntimeException();
        }
    }

    @Override
    public void releaseShared(ResourceType resourceType, long resourceId) {
        release(LockMode.SHARED, resourceType, resourceId);
    }

    @Override
    public void releaseExclusive(ResourceType resourceType, long resourceId) {
        release(LockMode.EXCLUSIVE, resourceType, resourceId);
    }

    @Override
    public boolean trySharedLock(ResourceType resourceType, long resourceId) {
        return acquire(AcquireMode.NONBLOCKING, LockMode.SHARED, resourceType, resourceId) == ClientAcquireOutcome.ACQUIRED;
    }

    @Override
    public boolean tryExclusiveLock(ResourceType resourceType, long resourceId) {
        return acquire(AcquireMode.NONBLOCKING, LockMode.EXCLUSIVE, resourceType, resourceId) == ClientAcquireOutcome.ACQUIRED;
    }

    @Override
    public boolean reEnterShared(ResourceType resourceType, long resourceId) {
        return heldLocks.tryLocalAcquire(resourceType, resourceId, LockMode.SHARED) == LockMode.NONE;
    }

    @Override
    public boolean reEnterExclusive(ResourceType resourceType, long resourceId) {
        return heldLocks.tryLocalAcquire(resourceType, resourceId, LockMode.EXCLUSIVE) == LockMode.NONE;
    }

    @Override
    public void close() {
        // Step 1: Group locks by partition, so we can release in each partition in bulk
        List<F2ClientEntry>[] heldByPartition = new List[partitions.numberOfPartitions()];
        heldLocks.releaseAll(partitions, heldByPartition);

        // Step 2: Release locks in each partition in bulk
        for(int partitionIndex=0;partitionIndex < heldByPartition.length;partitionIndex++) {
            List<F2ClientEntry> entries = heldByPartition[partitionIndex];
            if(entries == null) {
                continue;
            }

            F2Partition partition = partitions.getPartitionByIndex(partitionIndex);
            partition.lock();
            try {
                for (F2ClientEntry entry : entries) {
                    release_partitionLockHeld(partition, entry);
                }
            } finally {
                partition.unlock();
            }
        }
    }

    @Override
    public int getLockSessionId() {
        return clientId;
    }

    @Override
    public Stream<? extends ActiveLock> activeLocks() {
        return heldLocks.asStream();
    }

    @Override
    public long activeLockCount() {
        return heldLocks.activeLockCount();
    }

    @Override
    public String toString() {
        return name;
    }

    public String name() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    // TODO
    @Override
    public void stop() {
        throw new UnsupportedOperationException("Out-of-band interrupting F2 locks is not yet implemented.");
    }

    private ClientAcquireOutcome acquire(AcquireMode acquireMode, LockMode requestedLockMode, ResourceType resourceType, long resourceId) {
        F2Lock lock;
        F2ClientEntry entry;
        AcquireOutcome outcome;

        // If we already hold this lock, no need to globally synchronize
        LockMode lockMode = heldLocks.tryLocalAcquire(resourceType, resourceId, requestedLockMode);
        if(lockMode == LockMode.NONE) {
            return ClientAcquireOutcome.ACQUIRED;
        }

        // We don't hold this lock already, go to work on the relevant partition
        F2Partition partition = partitions.getPartition(resourceId);
        partition.lock();
        try {
            entry = partition.newClientEntry(this, lockMode, resourceType, resourceId);

            lock = partition.getOrCreateLock(resourceType, resourceId);

            outcome = lock.acquire(acquireMode, entry);

            if (outcome == AcquireOutcome.NOT_ACQUIRED) {
                partition.releaseClientEntry(entry);
                return ClientAcquireOutcome.NOT_ACQUIRED;
            }
        } finally {
            partition.unlock();
        }

        if (outcome == AcquireOutcome.ACQUIRED) {
            heldLocks.globallyAcquired(entry);
            return ClientAcquireOutcome.ACQUIRED;
        }

        try {
            assert outcome == AcquireOutcome.MUST_WAIT;

            // At this point, we are on the wait list for the lock we want, and we *have* to wait for it.
            // The way this works is that, eventually, someone ahead of us on the wait list will grant us the lock
            // and wake us up via {@link latch}. Until then, we wait; if it takes to long we wake up and check deadlock.
            for (; ; ) {
                boolean latchTripped = latch.tryAcquire(CHECK_DEADLOCK_AFTER_MS, TimeUnit.MILLISECONDS);
                if (latchTripped) {
                    // Someone told us we got the lock!
                    assert waitsFor == null: String.format("Should not be marked waiting if lock was granted, %s.waitsFor=%s", this, waitsFor);
                    heldLocks.globallyAcquired(entry);
                    return ClientAcquireOutcome.ACQUIRED;
                } else {
                    // We timed out; need to do deadlock detection
                    Deadlock deadlock = detectDeadlock();
                    if (deadlock != null) {
                        return deadlock;
                    }
                }
            }
        } catch (Throwable e) {
            // Current thread was interrupted while waiting on a lock, not good.
            // We are on the wait list for the lock, so we can't simply leave, need cleanup.
            if(this.waitsFor != null) {
                cleanUpErrorWhileWaiting(partition, entry);
            }
            return new ClientAcquireError(e);
        }
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {
        // Start by reducing the count of locally held locks; if we're lucky that's all we need
        F2ClientEntry entry = heldLocks.tryLocalRelease(lockMode, resourceType, resourceId);
        if(entry == null) {
            return;
        }

        // If we end up here, we've brought our counter of lock re-entrancy to zero, meaning it's time to release the
        // actual lock; hence we lock the relevant partition and go to work.
        F2Partition partition = partitions.getPartition(resourceId);
        partition.lock();
        try {
            release_partitionLockHeld(partition, entry);
        } finally {
            partition.unlock();
        }
    }

    private void release_partitionLockHeld(F2Partition partition, F2ClientEntry entry) {
        ResourceType resourceType = entry.resourceType;
        long resourceId = entry.resourceId;

        F2Lock.ReleaseOutcome outcome = entry.lock.release(entry);
        partition.releaseClientEntry(entry);


        if(outcome == F2Lock.ReleaseOutcome.LOCK_HELD) {
            return;
        }

        // If the lock is idle (eg. there are no holders and no waiters) then its our job to remove it
        // from the lock table.
        assert outcome == F2Lock.ReleaseOutcome.LOCK_IDLE : "Lock idle is only allowed state here.";
        partition.removeLock(resourceType, resourceId);
    }

    private Deadlock detectDeadlock() {
        partitions.stopTheWorld();
        try {
            DeadlockDescription description = deadlockDetector.detectDeadlock(this);
            if(description == DeadlockDetector.NONE) {
                return null;
            }

            Deadlock deadlock = new Deadlock(description.toString());

            // TODO: We could easily tell any waiter in the deadlock chain to abort by signalling;
            //       eg. we could abort a client with lower prio than us, or whatever.
            // For now, abort the client that firsts discovers the deadlock
            F2Partition partition = partitions.getPartition(waitsFor.resourceId);
            cleanUpErrorWhileWaiting_partitionLockHeld(partition, waitsFor);

            return deadlock;
        } finally {
            partitions.resumeTheWorld();
        }
    }

    private void cleanUpErrorWhileWaiting(F2Partition partition, F2ClientEntry entry) {
        long stamp = partition.partitionLock.writeLock();
        try {
            cleanUpErrorWhileWaiting_partitionLockHeld(partition, entry);
        } finally {
            partition.partitionLock.unlock(stamp);
        }
    }

    /**
     * NOTE: Must hold at least partition lock
     */
    private void cleanUpErrorWhileWaiting_partitionLockHeld(F2Partition partition, F2ClientEntry entry) {
        ResourceType resourceType = entry.resourceType;
        long resourceId = entry.resourceId;

        F2Lock.ReleaseOutcome outcome = entry.lock.errorCleanup(entry);
        if(outcome == F2Lock.ReleaseOutcome.LOCK_IDLE) {
            // If the lock ended up idle, we need to remove it from the lock table before wrapping up
            partition.removeLock(resourceType, resourceId);
        }
        partition.releaseClientEntry(entry);
    }
}
