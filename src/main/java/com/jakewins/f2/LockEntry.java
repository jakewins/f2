package com.jakewins.f2;

/** Tracks one client's holding of one lock */
class LockEntry {
    /**
     * If the lock is held: The next entry holding the same lock
     * If the lock is waited on: The next entry waiting for the same lock
     */
    LockEntry nextHolder = null;
}
