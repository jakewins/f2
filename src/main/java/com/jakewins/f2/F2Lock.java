package com.jakewins.f2;


import org.neo4j.storageengine.api.lock.ResourceType;

import static com.jakewins.f2.F2Lock.AcquireOutcome.ACQUIRED;
import static com.jakewins.f2.F2Lock.AcquireOutcome.MUST_WAIT;
import static com.jakewins.f2.F2Lock.AcquireOutcome.NOT_ACQUIRED;
import static com.jakewins.f2.F2Lock.ReleaseOutcome.LOCK_IDLE;
import static com.jakewins.f2.F2Lock.ReleaseOutcome.LOCK_HELD;

class F2Lock {
    enum AcquireOutcome {
        ACQUIRED,
        NOT_ACQUIRED,
        MUST_WAIT
    }
    enum ReleaseOutcome {
        LOCK_HELD,
        LOCK_IDLE
    }

    /** The type of resource this lock guards */
    ResourceType resourceType;

    /** The id of the resource this lock guards */
    long resourceId;

    /** Single entry of current exclusive holder */
    F2ClientEntry exclusiveHolder = null;

    /** Linked list of holders of shared lock */
    F2ClientEntry sharedHolderList = null;

    /** Linked list of waiting holders */
    F2ClientEntry waitList = null;

    /** When on a freelist, next free lock after this one */
    F2Lock next;

    /**
     * Try to acquire this lock. If that's not currently possible, then acquireMode determines if the entry will be
     * added to the locks wait list or if we'll simply return.
     *
     * NOTE: Must hold partition lock before calling
     * @param acquireMode BLOCKING or NON_BLOCKING (eg. put on wait list or not if lock can't be acquired)
     * @param entry the client entry to add to the lock
     * @return the outcome; grabbed, not grabbed, or on wait list
     */
    AcquireOutcome acquire(AcquireMode acquireMode, F2ClientEntry entry) {
        assert entry.owner.waitsFor == null : String.format("Client marked as waiting, cannot acquire: Client(%s).waitsFor = %s, acquiring=%s", entry.owner, entry.owner.waitsFor, entry);

        if(entry.lockMode == LockMode.EXCLUSIVE) {
            return acquireExclusive(acquireMode, entry);
        } else if(entry.lockMode == LockMode.SHARED) {
            return acquireShared(acquireMode, entry);
        } else if(entry.lockMode == LockMode.UPGRADE) {
            return acquireUpgrade(acquireMode, entry);
        } else {
            throw new AssertionError(String.format("Unknown lock mode %s", entry.lockMode));
        }
    }

    /**
     * Release a lock held.
     * NOTE: Must hold partition lock before calling
     * @param entry the client's entry with this lock
     */
    ReleaseOutcome release(F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE || entry.lockMode == LockMode.UPGRADE) {
            return releaseExclusiveOrUpgrade(entry);
        } else if(entry.lockMode == LockMode.SHARED) {
            return releaseShared(entry);
        } else {
            throw new AssertionError(String.format("Unknown lock mode %s", entry.lockMode));
        }
    }

    /**
     * Called when something fails while a client is waiting to get a lock. Because it does not hold
     * the partition lock while it waits, it may actually have gotten the lock right around the time
     * whatever error occurred. Hence, either the client is on the wait list, or it's holding the lock,
     * and the end result should be that the given entry should be disassociated with the lock.
     *
     * NOTE: The client may already hold a shared lock that it is upgrading! That share lock should remain in place.
     * NOTE: Must hold partition lock before calling
     * @param entry the lock entry to clear out
     */
    ReleaseOutcome errorCleanup(F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            // Check that this entry hasn't been granted the lock
            if(exclusiveHolder == entry) {
                return release(entry);
            }
        } else {
            // Check that this entry hasn't already been granted the shared lock
            F2ClientEntry current = sharedHolderList;
            while(current != null) {
                if(current == entry) {
                    // This entry holds a share lock; needs to cleanly release it
                    return release(entry);
                }
            }
        }
        removeFromWaitList(entry);
        entry.owner.waitsFor = null;
        if(exclusiveHolder == null && sharedHolderList == null) {
            return LOCK_IDLE;
        }
        return LOCK_HELD;
    }

    @Override
    public String toString() {
        return "Lock(" + resourceType.name() +
                "[" + resourceId + "])";
    }

    private AcquireOutcome acquireShared(AcquireMode acquireMode, F2ClientEntry entry) {
        if(exclusiveHolder != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        entry.next = sharedHolderList;
        sharedHolderList = entry;

        entry.lock = this;

        return ACQUIRED;
    }

    private AcquireOutcome acquireExclusive(AcquireMode acquireMode, F2ClientEntry entry) {
        if(exclusiveHolder != null || sharedHolderList != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        exclusiveHolder = entry;

        entry.lock = this;

        return ACQUIRED;
    }

    private AcquireOutcome acquireUpgrade(AcquireMode acquireMode, F2ClientEntry entry) {
        assert exclusiveHolder == null : String.format("Client asking for upgrade on lock that is exclusively held: %s, %s", entry, exclusiveHolder);

        // The upgrade can be granted immediately if the client asking for the upgrade is the only shared holder,
        // otherwise the client must wait for all other shared holders to drop out before it can upgrade.
        // We can test for this rapidly by checking if there's at least one `next` entry:
        if(sharedHolderList.next != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        exclusiveHolder = entry;

        entry.lock = this;

        return ACQUIRED;
    }

    private AcquireOutcome handleAcquireFailed(F2ClientEntry entry, AcquireMode mode) {
        if(mode == AcquireMode.BLOCKING) {
            entry.next = null;
            entry.lock = this;
            entry.owner.waitsFor = entry;

            // Find the right place in the wait list to add us to. In a naive fairness sense, this would always
            // be at the end of the wait list. However, there are several classes of deadlocks that can be avoided
            // if we allow some flexibility to fairness, see specific scenarios in the loop below.
            F2ClientEntry waitListSpot = null, nextWaitListEntry;
            for(nextWaitListEntry = waitList; nextWaitListEntry != null; ) {
                if(entry.lockMode == LockMode.UPGRADE) {
                    // Placing an upgrade lock behind an exclusive waiter will immediately deadlock, because
                    // the UPGRADE client holds a share lock, blocking the EXCLUSIVE client, and the EXCLUSIVE
                    // client will block the UPGRADE request. Avoid this by letting the UPGRADE to go ahead of the
                    // EXCLUSIVE request.
                    if(nextWaitListEntry.lockMode == LockMode.EXCLUSIVE) {
                        break;
                    }
                }

                waitListSpot = nextWaitListEntry;
                nextWaitListEntry = nextWaitListEntry.next;
            }

            if(waitListSpot == null) {
                entry.next = waitList;
                waitList = entry;
            } else {
                entry.next = waitListSpot.next;
                waitListSpot.next = entry;
            }
            return MUST_WAIT;
        }
        return NOT_ACQUIRED;
    }

    private ReleaseOutcome releaseExclusiveOrUpgrade(F2ClientEntry entry) {
        assert exclusiveHolder == entry : String.format("%s releasing exclusive lock held by %s.", entry, exclusiveHolder);

        exclusiveHolder = null;

        return grantLockToWaiters();
    }

    private ReleaseOutcome releaseShared(F2ClientEntry entry) {
        if(sharedHolderList == entry) {
            sharedHolderList = entry.next;
        } else {
            for(F2ClientEntry current = sharedHolderList; current.next != null; current = current.next) {
                if(current.next == entry) {
                    current.next = entry.next;
                    break;
                }
            }
        }

        entry.next = null;

        // Null check on exclusive holder because we may be releasing just shared portion of upgrade lock
        if(exclusiveHolder == null && (sharedHolderList == null || canGrantUpgradeLock())) {
            return grantLockToWaiters();
        }
        return LOCK_HELD;
    }

    /**
     * Hand the lock over to whoever is waiting for the lock; possibly many waiters if there's a list of clients
     * that all want a shared lock.
     * @return LOCK_IDLE if nobody holds the lock, or LOCK_HELD if there's at least one lock holder
     */
    private ReleaseOutcome grantLockToWaiters() {
        // Note that, in the case of releasing an upgrade lock, client may still hold a shared lock

        F2ClientEntry nextWaiter;
        ReleaseOutcome outcome = sharedHolderList == null ? LOCK_IDLE : LOCK_HELD;
        for(;;) {
            nextWaiter = waitList;
            if(nextWaiter == null) {
                return outcome;
            }
            if(nextWaiter.lockMode == LockMode.EXCLUSIVE) {
                if(sharedHolderList != null) {
                    // Looks like we've already handed out shared locks, so this exclusive needs to keep waiting
                    return outcome;
                }

                // Mark as exclusive owner
                exclusiveHolder = nextWaiter;

                // Remove from wait list
                waitList = nextWaiter.next;
                nextWaiter.next = null;

                // Signal the waiting client
                nextWaiter.owner.waitsFor = null;
                nextWaiter.owner.latch.release();
                return LOCK_HELD;
            } else if(nextWaiter.lockMode == LockMode.SHARED){
                // Highlight that the lock has at least one new holder
                outcome = LOCK_HELD;

                // Remove from wait list
                waitList = nextWaiter.next;

                // Add to shared list
                nextWaiter.next = sharedHolderList;
                sharedHolderList = nextWaiter;

                // Signal the waiting client
                nextWaiter.owner.waitsFor = null;
                nextWaiter.owner.latch.release();
            } else if(nextWaiter.lockMode == LockMode.UPGRADE) {
                if(sharedHolderList != null && sharedHolderList.next != null) {
                    // There's at least two shared holders; can't grant upgrade until all but the one held by the
                    // upgrader remains.
                    return outcome;
                }

                // Mark as exclusive owner
                exclusiveHolder = nextWaiter;

                // Remove from wait list
                waitList = nextWaiter.next;
                nextWaiter.next = null;

                // Signal the waiting client
                nextWaiter.owner.waitsFor = null;
                nextWaiter.owner.latch.release();
            } else {
                throw new AssertionError(String.format("Unknown lock mode: %s", nextWaiter));
            }
        }
    }

    private void removeFromWaitList(F2ClientEntry entry) {
        F2ClientEntry prev = null;
        for(F2ClientEntry current = waitList; current != null; ) {
            if(current == entry) {
                // Found our entry; remove it from wait list
                if(prev == null) {
                    waitList = entry.next;
                } else {
                    prev.next = entry.next;
                }
                return;
            }
            prev = current;
            current = current.next;
        }

        assert false : "Asked to removeLock client from wait list, but client was not on it.";
    }

    private boolean canGrantUpgradeLock() {
        return sharedHolderList.next == null && waitList != null && waitList.lockMode == LockMode.UPGRADE;
    }
}
