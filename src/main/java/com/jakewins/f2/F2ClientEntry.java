package com.jakewins.f2;

import org.neo4j.storageengine.api.lock.ResourceType;

/** Tracks one client's holding, or attempt of holding, of one lock */
class F2ClientEntry {
    /** Owner of this entry */
    F2Client owner;

    /** The lock this entry is holding or waiting for, if any */
    F2Lock lock;

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
    F2ClientEntry next;

    /**
     * Used by {@link #owner} without synchronization; count number of re-entrant locks held to avoid globally
     * coordinating when grabbing locks already held.
     */
    int[] heldcount = new int[LockMode.numberOfModes()];

    @Override
    public String toString() {
        return "Entry(" +
                "Client(" + owner + "), "
                + lockMode + " " + lock +
                ")";
    }

    boolean holdsLocks() {
        for(int held : heldcount) {
            if(held > 0) {
                return true;
            }
        }
        return false;
    }
}
