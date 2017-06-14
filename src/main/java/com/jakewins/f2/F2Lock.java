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
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            return acquireExclusive(acquireMode, entry);
        } else {
            return acquireShared(acquireMode, entry);
        }
    }

    /**
     * Release a lock held.
     * NOTE: Must hold partition lock before calling
     * @param entry the client's entry with this lock
     */
    ReleaseOutcome release(F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            return releaseExclusive(entry);
        } else {
            return releaseShared(entry);
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

    private AcquireOutcome handleAcquireFailed(F2ClientEntry entry, AcquireMode mode) {
        if(mode == AcquireMode.BLOCKING) {
            entry.next = null;
            entry.lock = this;
            entry.owner.waitsFor = entry;

            if(waitList == null) {
                waitList = entry;
                return MUST_WAIT;
            }

            F2ClientEntry current;
            for(current = waitList; current.next != null; ) {
                current = current.next;
            }

            current.next = entry;
            return MUST_WAIT;
        }
        return NOT_ACQUIRED;
    }

    private ReleaseOutcome releaseExclusive(F2ClientEntry entry) {
        assert exclusiveHolder == entry : String.format("%s releasing exclusive lock held by %s.", entry, exclusiveHolder);

        exclusiveHolder = null;

        // TODO: Handle upgrade lock (eg. same client is on shared lock list (or?))
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
        if(sharedHolderList == null) {
            return grantLockToWaiters();
        }
        return LOCK_HELD;
    }

    /**
     * Hand the lock over to whoever is waiting for the lock; possibly many waiters if there's a list of clients
     * that all want a shared lock.
     * @return LOCK_IDLE if there was nobody waiting to get the lock, or LOCK_HELD if there was at least one waiter
     */
    private ReleaseOutcome grantLockToWaiters() {
        F2ClientEntry nextWaiter;
        ReleaseOutcome outcome = LOCK_IDLE;
        for(;;) {
            nextWaiter = waitList;
            if(nextWaiter == null) {
                return outcome;
            }
            if(nextWaiter.lockMode == LockMode.EXCLUSIVE) {
                if(outcome != LOCK_IDLE) {
                    // If the outcome has been marked as not idle, there are already shared grantees;
                    // meaning we can't grant this exclusive lock. Stop granting and return.
                    return outcome;
                }

                // Remove from wait list
                waitList = nextWaiter.next;

                // Mark as exclusive owner
                exclusiveHolder = nextWaiter;

                // Signal the waiting client
                nextWaiter.owner.waitsFor = null;
                nextWaiter.owner.latch.release();
                return LOCK_HELD;
            } else {
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
                entry.owner.waitsFor = null;
                entry.owner.latch.release();
                return;
            }
            prev = current;
            current = current.next;
        }

        assert false : "Asked to removeLock client from wait list, but client was not on it.";
    }
}
