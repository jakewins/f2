package com.jakewins.f2;

class F2Lock {
    enum Outcome {
        ACQUIRED,
        NOT_ACQUIRED,
        /** Caller is on waitlist, *has to* wait until it's off of it */
        MUST_WAIT
    }

    /** Single entry of current exclusive holder */
    private LockEntry exclusiveHolder = null;

    /** Linked list of holders of shared lock */
    private LockEntry sharedHolderList = null;

    /** Linked list of waiting holders */
    private LockEntry waitList = null;

    Outcome acquire(AcquireMode acquireMode, LockEntry entry) {
        if(entry.lockMode == LockMode.EXCLUSIVE) {
            return exclusiveLock(acquireMode, entry);
        } else {
            return sharedLock(acquireMode, entry);
        }
    }

    // NOTE: Must hold partition lock before calling
    private Outcome sharedLock(AcquireMode acquireMode, LockEntry entry) {
        if(exclusiveHolder != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        entry.nextHolder = sharedHolderList;
        sharedHolderList = entry;
        return Outcome.ACQUIRED;
    }

    // NOTE: Must hold partition lock before calling
    private Outcome exclusiveLock(AcquireMode acquireMode, LockEntry entry) {
        if(exclusiveHolder != null || sharedHolderList != null) {
            return handleAcquireFailed(entry, acquireMode);
        }

        exclusiveHolder = entry;
        return Outcome.ACQUIRED;
    }

    private Outcome handleAcquireFailed(LockEntry entry, AcquireMode mode) {
        if(mode == AcquireMode.BLOCKING) {
            entry.nextHolder = waitList;
            waitList = entry;
            return Outcome.MUST_WAIT;
        }
        return Outcome.NOT_ACQUIRED;
    }
}
