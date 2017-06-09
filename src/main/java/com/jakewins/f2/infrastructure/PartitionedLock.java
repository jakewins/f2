package com.jakewins.f2.infrastructure;

import java.util.concurrent.locks.StampedLock;

/**
 * Stamped lock that exclusively locks partitioned keys; the keys are hashed over the number of partitions,
 * so more partitions == less likely two key's "collide" and block each other.
 *
 * This is a memory optimization: Assuming you have a very large set of resources identified by keys, and you
 * want to protect access to them with a lock, rather than allocate millions of locks, you group your resources
 * into partitions and have one lock per partition.
 */
public class PartitionedLock {

    private final StampedLock[] partitions;
    private final int bitwiseModulo;

    public PartitionedLock(int nPartitions) {
        assert Long.bitCount(nPartitions) == 1 : "Partition count must be a power-of-two";

        partitions = new StampedLock[nPartitions];
        bitwiseModulo = nPartitions - 1;
    }

    /**
     * Acquire an exclusive lock on the partition the given resource is part of.
     *
     * @param resourceId any long, you decide what it represents
     * @return a stamp, see StampedLock for details
     */
    public long acquire(long resourceId) {
        return partitions[index(resourceId)].writeLock();
    }

    /**
     * Release an exclusively held lock
     *
     * @param resourceId any long, you decide what it represents
     * @param stamp the stamp you got from {@link #acquire(long)}
     */
    public void release(long resourceId, long stamp) {
        partitions[index(resourceId)].unlock(stamp);
    }

    private int index(long key) {
        return Long.hashCode(key) & bitwiseModulo;
    }
}
