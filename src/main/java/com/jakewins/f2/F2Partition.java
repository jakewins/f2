package com.jakewins.f2;

import com.jakewins.f2.include.ResourceType;

import java.util.HashMap;
import java.util.concurrent.locks.StampedLock;

// Intended to be replaced by something spiffy
class SomeFastMapImplementation {
    private final HashMap<Long, F2Lock> map = new HashMap<>();

    F2Lock putIfAbsent(long resourceId, F2Lock toInsert) {
        return map.putIfAbsent(resourceId, toInsert);
    }
}

/**
 * F2 locks are split into partitions; operations within a partition must be guarded by the partition lock.
 */
class F2Partition {
    // TODO: Get a view of the approximate max number of CPU instructions a holder of this might want, and how often Linux will reschedule
    StampedLock partitionLock = new StampedLock();
    F2Lock nextFreeLock = null;
    F2ClientEntry nextFreeClientEntry = null;

    private final SomeFastMapImplementation[] locks;

    F2Partition(int numResourceTypes) {
        assert Long.bitCount(numResourceTypes) == 1 : "numResourceTypes must be power of two.";

        this.locks = new SomeFastMapImplementation[numResourceTypes];
        for(int resourceType=0;resourceType<numResourceTypes;resourceType++) {
            this.locks[resourceType] = new SomeFastMapImplementation();
        }
    }

    /**
     * NOTE: Must hold {@link #partitionLock}
     */
    F2Lock putIfAbsent(ResourceType resourceType, long resourceId, F2Lock newLock) {
        SomeFastMapImplementation map = locks[resourceType.typeId()];
        return map.putIfAbsent(resourceId, newLock);
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
        entry.lockMode = null;
        entry.resourceType = null;
        entry.resourceId = -1;
        entry.next = nextFreeClientEntry;
        nextFreeClientEntry = entry;
    }
}

class F2Partitions {
    private final F2Partition[] partitions;
    private final int bitwiseModulo;

    F2Partitions(int numResourceTypes, int numPartitions) {
        assert Long.bitCount(numPartitions) == 1 : "numPartitions must be power of two.";

        this.bitwiseModulo = numPartitions - 1;
        this.partitions = new F2Partition[numPartitions];
        for(int partition = 0; partition < numPartitions; partition++) {
            this.partitions[partition] = new F2Partition(numResourceTypes);
        }
    }

    F2Partition getPartition(long resourceId) {
        return partitions[Long.hashCode(resourceId) & bitwiseModulo];
    }
}