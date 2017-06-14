package com.jakewins.f2;

import com.jakewins.f2.F2Lock.AcquireOutcome;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.jakewins.f2.infrastructure.Latch;
import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
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
    private final Exception cause;

    ClientAcquireError(Exception cause) {
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

    /** Signal when client is granted a lock it is waiting on */
    Latch latch = new Latch();

    /**
     * Lock entry this client is currently waiting on, or null; this is set by the lock when we're added to
     * wait list, cleared when someone grants us the lock.
     *
     * NOTE: Must hold partition lock of the entry to write to this
     */
    F2ClientEntry waitsFor;

    private String name;

    private final F2Partitions partitions;
    private final DeadlockDetector deadlockDetector;
    private final PrimitiveLongObjectMap<F2ClientEntry>[] heldLocks;

    F2Client(int numResourceTypes, F2Partitions partitions, DeadlockDetector deadlockDetector) {
        this.partitions = partitions;
        this.deadlockDetector = deadlockDetector;
        this.heldLocks = new PrimitiveLongObjectMap[numResourceTypes];
        for(int i=0;i<numResourceTypes;i++) {
            this.heldLocks[i] = Primitive.longObjectMap(32);
        }
    }

    void setName(String name) {
        this.name = name;
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

    private void handleAcquireOutcome(ClientAcquireOutcome outcome) {
        if(outcome == ClientAcquireOutcome.ACQUIRED || outcome == ClientAcquireOutcome.NOT_ACQUIRED) {
            return;
        }
        if(outcome instanceof Deadlock) {
            throw new RuntimeException(((Deadlock) outcome).deadlockDescription());
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
        throw new UnsupportedOperationException("Sorry.");
    }

    @Override
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

    @Override
    public Stream<? extends ActiveLock> activeLocks() {
        return null;
    }

    public long activeLockCount() {
        throw new UnsupportedOperationException("Sorry.");
    }

    @Override
    public String toString() {
        return name;
    }

    private ClientAcquireOutcome acquire(AcquireMode acquireMode, LockMode lockMode, ResourceType resourceType, long resourceId) {
        // If we already hold this lock, no need to globally synchronize
        // TODO there is some sort of bug with reentrancy
        F2ClientEntry entry = heldLocks[resourceType.typeId()].get(resourceId);
        if(entry != null) {
            if(entry.heldcount[lockMode.index] > 0) {
                entry.heldcount[lockMode.index] += 1;
                return ClientAcquireOutcome.ACQUIRED;
            }
        }

        // We don't hold this lock already, go to work on the relevant partition
        F2Partition partition = partitions.getPartition(resourceId);
        F2Lock lock;
        AcquireOutcome outcome;
        long stamp = partition.partitionLock.writeLock();
        try {
            if(entry == null) {
                entry = partition.newClientEntry(this, lockMode, resourceType, resourceId);
            }

            lock = partition.getOrCreate(resourceType, resourceId);

            outcome = lock.acquire(acquireMode, entry);

            if(outcome == AcquireOutcome.NOT_ACQUIRED) {
                releaseEntryIfUnused(partition, entry);
                return ClientAcquireOutcome.NOT_ACQUIRED;
            }
        } finally {
            partition.partitionLock.unlock(stamp);
        }

        if(outcome == AcquireOutcome.ACQUIRED) {
            entry.heldcount[lockMode.index] = 1;
            heldLocks[resourceType.typeId()].put(resourceId, entry);
            return ClientAcquireOutcome.ACQUIRED;
        }

        try {
            assert outcome == AcquireOutcome.MUST_WAIT;

            // At this point, we are on the wait list for the lock we want, and we *have* to wait for it.
            // The way this works is that, eventually, someone ahead of us on the wait list will grant us the lock
            // and wake us up via {@link latch}. Until then, we wait; if it takes to long we wake up and check deadlock.
            for(;;) {
                boolean latchTripped = latch.tryAcquire(CHECK_DEADLOCK_AFTER_MS, TimeUnit.MILLISECONDS);
                if(latchTripped) {
                    // Someone told us we got the lock!
                    entry.heldcount[lockMode.index] = 1;
                    heldLocks[resourceType.typeId()].put(resourceId, entry);
                    return ClientAcquireOutcome.ACQUIRED;
                } else {
                    // We timed out; need to do deadlock detection
                    Deadlock deadlock = detectDeadlock();
                    if(deadlock != null) {
                        return deadlock;
                    }
                }
            }
        } catch(Exception e) {
            // Current thread was interrupted while waiting on a lock, not good.
            // We are on the wait list for the lock, so we can't simply leave, need cleanup.
            cleanUpErrorWhileWaiting(partition, entry);
            return new ClientAcquireError(e);
        }
    }

    private void release(LockMode lockMode, ResourceType resourceType, long resourceId) {
        // Start by reducing the count of locally held locks; if we're lucky that's all we need
        F2ClientEntry entry = heldLocks[resourceType.typeId()].get(resourceId);

        assert entry != null : "Releasing lock that's not held";
        assert entry.heldcount[lockMode.index] > 0 : "Releasing a lock the client does not hold.";

        entry.heldcount[lockMode.index] -= 1;
        if(entry.heldcount[lockMode.index] > 0) {
            return;
        }

        // If we end up here, we've brought our counter of lock re-entrancy to zero, meaning it's time to release the
        // actual lock; hence we lock the relevant partition and go to work.
        F2Partition partition = partitions.getPartition(resourceId);
        F2Lock lock = entry.lock;
        long stamp = partition.partitionLock.writeLock();
        try {
            F2Lock.ReleaseOutcome outcome = lock.release(entry);
            releaseEntryIfUnused(partition, entry);

            if(outcome == F2Lock.ReleaseOutcome.LOCK_HELD) {
                return;
            }

            // If the lock is idle (eg. there are no holders and no waiters) then its our job to remove it
            // from the lock table.
            assert outcome == F2Lock.ReleaseOutcome.LOCK_IDLE : "Lock idle is only allowed state here.";
            partition.removeLock(resourceType, resourceId);
        } finally {
            partition.partitionLock.unlock(stamp);
        }
    }

    private void cleanUpErrorWhileWaiting(F2Partition partition, F2ClientEntry entry) {
        long stamp = partition.partitionLock.writeLock();
        try {
            cleanUpErrorWhileWaiting_lockHeld(partition, entry);
        } finally {
            partition.partitionLock.unlock(stamp);
        }
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
            cleanUpErrorWhileWaiting_lockHeld(partition, waitsFor);

            return deadlock;
        } finally {
            partitions.resumeTheWorld();
        }
    }

    /**
     * NOTE: Must hold at least partition lock
     */
    private void cleanUpErrorWhileWaiting_lockHeld(F2Partition partition, F2ClientEntry entry) {
        ResourceType resourceType = entry.resourceType;
        long resourceId = entry.resourceId;

        F2Lock.ReleaseOutcome outcome = entry.lock.errorCleanup(entry);
        if(outcome == F2Lock.ReleaseOutcome.LOCK_IDLE) {
            // If the lock ended up idle, we need to remove it from the lock table before wrapping up
            partition.removeLock(resourceType, resourceId);
        }
        releaseEntryIfUnused(partition, entry);
    }

    /**
     * NOTE: Must hold at least partition lock
     */
    private void releaseEntryIfUnused(F2Partition partition, F2ClientEntry entry) {
        if(!entry.holdsLocks()) {
            heldLocks[entry.resourceType.typeId()].remove(entry.resourceId);
            partition.releaseClientEntry(entry);
        }
    }
}
