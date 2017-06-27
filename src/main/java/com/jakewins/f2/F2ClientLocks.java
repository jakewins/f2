package com.jakewins.f2;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.storageengine.api.lock.ResourceType;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tracks the locks held by a single client, managing the logic of combining that state with requests to release
 * or acquire new locks to determine which actual lock operations need doing in the global lock table.
 */
class F2ClientLocks {
    /**
     * Map of linked list (via {@link F2ClientEntry#ownerNext}) of entries held per resource,
     * reentrancy is tracked by lock mode via {@link F2ClientEntry#reentrancyCounter}.
     */
    private final PrimitiveLongObjectMap<F2ClientEntry>[] locksByResourceType;

    F2ClientLocks(int numResourceTypes) {
        locksByResourceType = new PrimitiveLongObjectMap[numResourceTypes];
        for(int resourceIndex=0;resourceIndex<numResourceTypes;resourceIndex++) {
            locksByResourceType[resourceIndex] = Primitive.longObjectMap();
        }
    }

    /**
     * If we already hold a lock locally, there is no need to go back to the global lock table for it.
     * Try to acquire a lock locally and return any action we need to take globally to acquire the lock
     * the user wants.
     *
     * The thing to remember here is that if a user holds a share lock and wants an exclusive lock, we
     * represent that as an upgrade lock request internally; that change from a user asking for exclusive
     * to us asking for upgrade happens between the lock mode passed in and the lock mode returned here.
     */
    LockMode tryLocalAcquire(ResourceType resourceType, long resourceId, LockMode requestedLockMode) {
        // HEAD of linked list of entries we hold on this resource
        F2ClientEntry entry = locksByResourceType[resourceType.typeId()].get(resourceId);

        // Go through the entries we already hold, if any, increment reentrancy counter if that's enough,
        // or request we grab some lock globally.
        boolean holdsShareLocks = false;
        for(;entry != null; entry = entry.ownerNext) {
            if(entry.lockMode == LockMode.SHARED) {
                holdsShareLocks = true;
                if(requestedLockMode == LockMode.SHARED) {
                    entry.reentrancyCounter += 1;
                    return LockMode.NONE;
                }
            } else if(entry.lockMode == LockMode.EXCLUSIVE || entry.lockMode == LockMode.UPGRADE) {
                if(requestedLockMode == LockMode.EXCLUSIVE || requestedLockMode == LockMode.UPGRADE) {
                    entry.reentrancyCounter += 1;
                    return LockMode.NONE;
                }
            }
        }

        if (requestedLockMode == LockMode.EXCLUSIVE && holdsShareLocks) {
            // Client holds a share lock and wants exclusive; ask global tables for an upgrade lock
            return LockMode.UPGRADE;
        }
        return requestedLockMode;
    }

    /** Record that we globally acquired the lock defined by the given entry */
    void globallyAcquired(F2ClientEntry entry) {
        entry.reentrancyCounter = 1;

        entry.ownerNext = locksByResourceType[entry.resourceType.typeId()].get(entry.resourceId);
        locksByResourceType[entry.resourceType.typeId()].put(entry.resourceId, entry);
    }

    /**
     * Release a locally held lock. If this is the last reentrant lock of this type we hold on the resource,
     * return the related lock entry to be globally released.
     */
    F2ClientEntry tryLocalRelease(LockMode requestedLockMode, ResourceType resourceType, long resourceId) {
        // HEAD of linked list of entries we hold on this resource
        F2ClientEntry previous = null;
        F2ClientEntry entry = locksByResourceType[resourceType.typeId()].get(resourceId);

        for(;entry != null; entry = entry.ownerNext) {
            if(requestedLockMode == LockMode.EXCLUSIVE && (entry.lockMode == LockMode.EXCLUSIVE || entry.lockMode == LockMode.UPGRADE)) {
                break;
            } else if(requestedLockMode == LockMode.SHARED && entry.lockMode == LockMode.SHARED) {
                break;
            }
            previous = entry;
        }

        assert entry != null : String.format("Trying to release lock that isn't held: %s %s %s", requestedLockMode, resourceType, resourceId);

        if(entry.reentrancyCounter > 1) {
            entry.reentrancyCounter -= 1;
            return null;
        }

        // This entry needs to be released globally
        if(previous == null) {
            if(entry.ownerNext == null) {
                // This was the only entry we had, so clear the map
                locksByResourceType[resourceType.typeId()].remove(resourceId);
            } else {
                locksByResourceType[resourceType.typeId()].put(resourceId, entry.ownerNext);
            }
        } else {
            previous.ownerNext = entry.ownerNext;
        }

        return entry;
    }

    /**
     * Group all the entries by partition, and clear the local locks table.
     */
    void releaseAll(F2Partitions partitions, List<F2ClientEntry>[] toReleaseByPartition) {
        for (PrimitiveLongObjectMap<F2ClientEntry> locks : locksByResourceType) {
            locks.visitEntries((resourceId, entry) -> {
                F2Partition partition = partitions.getPartition(resourceId);

                List<F2ClientEntry> heldInThisPartition = toReleaseByPartition[partition.index()];
                if(heldInThisPartition == null) {
                    toReleaseByPartition[partition.index()] = heldInThisPartition = new LinkedList<>();
                }

                for(; entry != null; entry = entry.ownerNext) {
                    heldInThisPartition.add(entry);
                }
                return false;
            });
            locks.clear();
        }
    }

    Stream<? extends ActiveLock> asStream() {
        return Arrays.asList(locksByResourceType)
                .stream()
                .flatMap((locks) -> {
                    LinkedList<ActiveLock> found = new LinkedList<>();
                    locks.visitEntries( (resourceId, entry) -> {
                        found.add(F2ActiveLock.fromEntry(entry));
                        return false;
                    });
                    return found.stream();
                });
    }

    long activeLockCount() {
        return Arrays.asList(locksByResourceType)
                .stream()
                .mapToInt((locks) -> locks.size())
                .sum();
    }

    private static class F2ActiveLock implements ActiveLock {
        private final String mode;
        private final ResourceType resourceType;
        private final long resourceId;

        static F2ActiveLock fromEntry(F2ClientEntry entry) {
            return new F2ActiveLock(entry.lockMode.name(), entry.resourceType, entry.resourceId);
        }

        F2ActiveLock(String mode, ResourceType resourceType, long resourceId) {
            this.mode = mode;
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public String mode() {
            return mode;
        }

        @Override
        public ResourceType resourceType() {
            return resourceType;
        }

        @Override
        public long resourceId() {
            return resourceId;
        }
    }
}
