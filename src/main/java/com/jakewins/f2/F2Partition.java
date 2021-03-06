package com.jakewins.f2;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.storageengine.api.lock.ResourceType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Stream;

/**
 * F2 locks are split into partitions; operations within a partition must be guarded by the partition lock.
 */
class F2Partition {
    // TODO: Get a view of the approximate max number of CPU instructions a holder of this might want, and how often Linux will reschedule
    StampedLock partitionLock = new StampedLock();
    private long currentHolderStamp;

    private F2Lock nextFreeLock = null;
    private F2ClientEntry nextFreeClientEntry = null;

    private final int partitionIndex;
    private final PrimitiveLongObjectMap<F2Lock>[] locks;

    F2Partition(int partitionIndex, int numResourceTypes) {
        // numResourceTypes must be a power of two; so if the input isn't, round it up to the nearest one
        if (Long.bitCount(numResourceTypes) != 1) {
            numResourceTypes = (int) Math.pow(2, Math.ceil(Math.log(numResourceTypes) / Math.log(2)));
        }

        this.partitionIndex = partitionIndex;
        this.locks = new PrimitiveLongObjectMap[numResourceTypes];
        for(int resourceType=0;resourceType<numResourceTypes;resourceType++) {
            this.locks[resourceType] = Primitive.longObjectMap(128);
        }
    }

    /**
     * NOTE: Must hold {@link #partitionLock}
     */
    F2Lock getOrCreateLock(ResourceType resourceType, long resourceId) {
        PrimitiveLongObjectMap<F2Lock> map = locks[resourceType.typeId()];
        F2Lock lock = map.get(resourceId);
        if(lock == null) {
            if(nextFreeLock != null) {
                lock = nextFreeLock;
                nextFreeLock = nextFreeLock.next;
            } else {
                lock = new F2Lock();
            }
            map.put(resourceId, lock);
        }

        lock.resourceType = resourceType;
        lock.resourceId = resourceId;
        lock.next = null;

        return lock;
    }

    /**
     * Remove a lock that is no longer in use. Client must
     *
     * NOTE: Must hold {@link #partitionLock}
     */
    void removeLock(ResourceType resourceType, long resourceId) {
        PrimitiveLongObjectMap<F2Lock> map = locks[resourceType.typeId()];
        F2Lock lock = map.remove(resourceId);

        assert lock.sharedHolderList == null : String.format("Removed lock with shared holders: %s ", lock.sharedHolderList);
        assert lock.exclusiveHolder == null : String.format("Removed lock with exclusive holder: %s ", lock.exclusiveHolder);
        assert lock.waitList == null : String.format("Removed lock with wait list! %s ", lock.waitList);

        // TODO: Some operations could feasibly temporarily allocate like a hundred million locks; should cap freelist
        lock.next = nextFreeLock;
        nextFreeLock = lock;
    }

    /**
     * NOTE: Must hold {@link #partitionLock}
     */
    F2ClientEntry newClientEntry(F2Client owner, LockMode lockMode, ResourceType resourceType, long resourceId) {
        F2ClientEntry entry = nextFreeClientEntry != null ? nextFreeClientEntry : new F2ClientEntry();
        nextFreeClientEntry = entry.next;

        entry.owner = owner;
        entry.lockMode = lockMode;
        entry.resourceType = resourceType;
        entry.resourceId = resourceId;

        return entry;
    }

    /**
     * NOTE: Must hold {@link #partitionLock}
     */
    void releaseClientEntry(F2ClientEntry entry) {
        entry.owner = null;
        entry.lock = null;
        entry.lockMode = null;
        entry.resourceType = null;
        entry.resourceId = -1;
        entry.reentrancyCounter = 0;

        entry.next = nextFreeClientEntry;
        nextFreeClientEntry = entry;
    }

    int index() {
        return partitionIndex;
    }

    void lock() {
        currentHolderStamp = partitionLock.writeLock();
    }

    void unlock() {
        partitionLock.unlock(currentHolderStamp);
    }

    Stream<F2Lock> activeLocks() {
        List<F2Lock> out = new ArrayList<>();
        for (PrimitiveLongObjectMap<F2Lock> lockMap : locks) {
            PrimitiveLongIterator iter = lockMap.iterator();
            while (iter.hasNext()) {
                out.add(lockMap.get(iter.next()));
            }
        }
        return out.stream();
    }
}

class F2Partitions {
    private final F2Partition[] partitions;
    private final int bitwiseModulo;

    F2Partitions(int numResourceTypes, int numPartitions) {
        assert Long.bitCount(numPartitions) == 1 : "numPartitions must be power of two.";

        this.bitwiseModulo = numPartitions - 1;
        this.partitions = new F2Partition[numPartitions];
        for(int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
            this.partitions[partitionIndex] = new F2Partition(partitionIndex, numResourceTypes);
        }
    }

    F2Partition getPartition(long resourceId) {
        return partitions[Long.hashCode(resourceId) & bitwiseModulo];
    }

    F2Partition getPartitionByIndex(int partitionIndex) {
        return partitions[partitionIndex];
    }

    /** Lock every partition lock in  order of partition id (so this can be done without deadlocks */
    void stopTheWorld() {
        for (F2Partition partition : partitions) {
            partition.lock();
        }
    }

    /** Resume spinning */
    void resumeTheWorld() {
        for (F2Partition partition : partitions) {
            partition.unlock();
        }
    }

    int numberOfPartitions() {
        return partitions.length;
    }
}