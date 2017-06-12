package com.jakewins.f2;

import com.jakewins.f2.include.ResourceType;

/** Tracks one client's holding, or attempt of holding, of one lock */
class F2ClientEntry {
    /** Owner of this entry */
    F2Client owner;

    /** The mode this entry holds or wants to hold a lock by (eg. shared/exclusive) */
    LockMode lockMode;

    /** The type of resource this entry holds or wants to hold */
    ResourceType resourceType;

    /** The id of the resource this entry holds or wants to hold */
    long resourceId;

    /**
     * If the lock is held: The next entry holding the same lock
     * If the lock is waited on: The next entry waiting for the same lock
     * If the entry is on a freelist: The next free entry after this one
     */
    F2ClientEntry next = null;

    @Override
    public String toString() {
        return "F2ClientEntry{" +
                "owner=" + owner +
                ", lockMode=" + lockMode +
                ", resourceType=" + resourceType +
                ", resourceId=" + resourceId +
                '}';
    }
}
