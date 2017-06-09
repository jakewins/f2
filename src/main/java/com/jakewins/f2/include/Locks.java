package com.jakewins.f2.include;

public interface Locks
{
    interface Client
    {
        /**
         * Represents the fact that no lock session is used because no locks are taken.
         */
        int NO_LOCK_SESSION_ID = -1;

        /**
         * Can be grabbed when there are no locks or only share locks on a resource. If the lock cannot be acquired,
         * behavior is specified by the {@link WaitStrategy} for the given {@link ResourceType}.
         *
         * @param resourceType type or resource(s) to lock.
         * @param resourceIds id(s) of resources to lock. Multiple ids should be ordered consistently by all callers
         */
        void acquireShared( ResourceType resourceType, long resourceIds ) throws AcquireLockTimeoutException;

        void acquireExclusive( ResourceType resourceType, long resourceIds ) throws AcquireLockTimeoutException;

        /** Try grabbing exclusive lock, not waiting and returning a boolean indicating if we got the lock. */
        boolean tryExclusiveLock( ResourceType resourceType, long resourceId );

        /** Try grabbing shared lock, not waiting and returning a boolean indicating if we got the lock. */
        boolean trySharedLock( ResourceType resourceType, long resourceId );

        boolean reEnterShared( ResourceType resourceType, long resourceId );

        boolean reEnterExclusive( ResourceType resourceType, long resourceId );

        /** Release a set of shared locks */
        void releaseShared( ResourceType resourceType, long resourceId );

        /** Release a set of exclusive locks */
        void releaseExclusive( ResourceType resourceType, long resourceId );

        /**
         * Stop all active lock waiters and release them. All already held locks remains.
         * All new attempts to acquire any locks will cause exceptions.
         * This client can and should only be {@link #close() closed} afterwards.
         */
        void stop();

        /** Releases all locks, using the client after calling this is undefined. */
        void close();

        /** For slave transactions, this tracks an identifier for the lock session running on the master */
        int getLockSessionId();

        long activeLockCount();
    }

    /**
     * A client is able to grab and release locks, and compete with other clients for them. This can be re-used until
     * you call {@link Locks.Client#close()}.
     *
     * @throws IllegalStateException if this instance has been closed, i.e has had {@link #close()} called.
     */
    Client newClient();

    void close();
}