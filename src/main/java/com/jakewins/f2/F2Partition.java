package com.jakewins.f2;

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
    StampedLock partitionLock = new StampedLock();

    private final SomeFastMapImplementation[] locks;

    F2Partition(int numResourceTypes) {
        assert Long.bitCount(numResourceTypes) == 1 : "numResourceTypes must be power of two.";

        this.locks = new SomeFastMapImplementation[numResourceTypes];
        for(int resourceType=0;resourceType<numResourceTypes;resourceType++) {
            this.locks[resourceType] = new SomeFastMapImplementation();
        }
    }

    /** Must hold {@link #partitionLock} */
    F2Lock.Outcome acquire(AcquireMode acquireMode, LockEntry entry) {
        SomeFastMapImplementation map = locks[entry.resourceType.typeId()];
        // TODO freelist, these will stay for long-ish periods, meaning they likely get promoted, meaning they will cause fragmentation
        // hence, keep a freelist per partition instead.
        F2Lock newLock = new F2Lock();

        F2Lock lock = map.putIfAbsent(entry.resourceId, newLock);
        return lock.acquire(acquireMode, entry);
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