package com.jakewins.f2;


import static com.jakewins.f2.F2Lock.Outcome.ACQUIRED;
import static com.jakewins.f2.F2Lock.Outcome.MUST_WAIT;
import static com.jakewins.f2.F2Lock.Outcome.NOT_ACQUIRED;

class F2Lock {
    enum Outcome {
        ACQUIRED,
        NOT_ACQUIRED,
        MUST_WAIT
    }

    /** Single entry of current exclusive holder */
    F2ClientEntry exclusiveHolder = null;

    /** Linked list of holders of shared lock */
    F2ClientEntry sharedHolderList = null;

    /** Linked list of waiting holders */
    F2ClientEntry waitList = null;

    /** When on a freelist, next free lock after this one */
    F2Lock nextFree;

    /**
     * Try to acquire this lock. If that's not currently possible, then acquireMode determines if the entry will be
     * added to the locks wait list or if we'll simply return.
     *
     * NOTE: Must hold partition lock before calling
     * @param acquireMode BLOCKING or NON_BLOCKING (eg. put on wait list or not if lock can't be acquired)
     * @param entry the client entry to add to the lock
     * @return the outcome; grabbed, not grabbed, or on wait list
     */
    Outcome acquire(AcquireMode acquireMode, F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            return exclusiveLock(acquireMode, entry);
        } else {
            return sharedLock(acquireMode, entry);
        }
    }

    /**
     * Release a lock held.
     * NOTE: Must hold partition lock before calling
     * @param entry the client's entry with this lock
     */
    void release(F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            throw new UnsupportedOperationException("not implemented");
        } else {
            throw new UnsupportedOperationException("not implemented");
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
    void errorCleanup(F2ClientEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            // Check that this entry hasn't been granted the lock
            if(exclusiveHolder == entry) {
                release(entry);
                return;
            }
        } else {
            // Check that this entry hasn't already been granted the shared lock
            F2ClientEntry current = sharedHolderList;
            while(current != null) {
                if(current == entry) {
                    // This entry holds a share lock; needs to cleanly release it
                    release(entry);
                    return;
                }
            }
        }
        removeFromWaitList(entry);
    }

    private void removeFromWaitList(F2ClientEntry entry) {
        if(waitList == entry) {
            waitList = entry.next;
            return;
        }

        F2ClientEntry current;
        for(current = waitList; current.next != null; ) {
            if(current.next == entry) {
                current.next = entry.next;
                return;
            }
            current = current.next;
        }

        assert false : "Asked to remove client from wait list, but client was not on it.";
    }

    private Outcome sharedLock(AcquireMode acquireMode, F2ClientEntry entry) {
        if(exclusiveHolder != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        entry.next = sharedHolderList;
        sharedHolderList = entry;
        return ACQUIRED;
    }

    private Outcome exclusiveLock(AcquireMode acquireMode, F2ClientEntry entry) {
        if(exclusiveHolder != null || sharedHolderList != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        exclusiveHolder = entry;
        return ACQUIRED;
    }


    private Outcome handleAcquireFailed(F2ClientEntry entry, AcquireMode mode) {
        if(mode == AcquireMode.BLOCKING) {
            entry.next = null;
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
}
