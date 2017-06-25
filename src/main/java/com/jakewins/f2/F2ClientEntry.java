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
     * If owner holds several lock types on the same lock, this will point to the next entry for the same owner
     */
    F2ClientEntry ownerNext;

    /**
     * Used by owner to track reentrancy - how many times has the user asked to acquire this lock?
     */
    int reentrancyCounter = 0;

    F2ClientEntry() {

    }

    F2ClientEntry(F2Client owner, F2Lock lock, LockMode lockMode,
                         ResourceType resourceType, long resourceId, F2ClientEntry next) {
        this.owner = owner;
        this.lock = lock;
        this.lockMode = lockMode;
        this.resourceType = resourceType;
        this.resourceId = resourceId;
        this.next = next;
    }

    @Override
    public String toString() {
        return "Entry(" +
                "Client(" + owner + ") "
                + lockMode + " " + lock +
                ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        F2ClientEntry that = (F2ClientEntry) o;

        if (resourceId != that.resourceId) return false;
        if (owner != null ? !owner.equals(that.owner) : that.owner != null) return false;
        if (lock != null ? !lock.equals(that.lock) : that.lock != null) return false;
        if (lockMode != that.lockMode) return false;
        if (resourceType != null ? !resourceType.equals(that.resourceType) : that.resourceType != null) return false;
        if (next != null ? !next.equals(that.next) : that.next != null) return false;
        return ownerNext != null ? ownerNext.equals(that.ownerNext) : that.ownerNext == null;

    }

    @Override
    public int hashCode() {
        int result = owner != null ? owner.hashCode() : 0;
        result = 31 * result + (lock != null ? lock.hashCode() : 0);
        result = 31 * result + (lockMode != null ? lockMode.hashCode() : 0);
        result = 31 * result + (resourceType != null ? resourceType.hashCode() : 0);
        result = 31 * result + (int) (resourceId ^ (resourceId >>> 32));
        result = 31 * result + (next != null ? next.hashCode() : 0);
        result = 31 * result + (ownerNext != null ? ownerNext.hashCode() : 0);
        return result;
    }
}
