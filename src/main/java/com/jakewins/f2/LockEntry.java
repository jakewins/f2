package com.jakewins.f2;

import com.jakewins.f2.include.ResourceType;

/** Tracks one client's holding, or attempt of holding, of one lock */
class LockEntry {
    /** The mode this entry holds or wants to hold a lock by (eg. shared/exclusive) */
    LockMode lockMode;

    /** The type of resource this entry holds or wants to hold */
    ResourceType resourceType;

    /** The id of the resource this entry holds or wants to hold */
    long resourceId;

    /**
     * If the lock is held: The next entry holding the same lock
     * If the lock is waited on: The next entry waiting for the same lock
     */
    LockEntry nextHolder = null;
}
