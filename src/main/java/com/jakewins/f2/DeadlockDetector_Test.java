package com.jakewins.f2;

import org.junit.Test;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;

import static com.jakewins.f2.AcquireMode.BLOCKING;
import static com.jakewins.f2.LockMode.EXCLUSIVE;

public class DeadlockDetector_Test {
    @Test
    public void testPreschoolDeadlock() {
        F2Lock lock1 = F2Lock_Test.newLock(1);
        F2Lock lock2 = F2Lock_Test.newLock(2);
        F2Client clientX = F2Lock_Test.newClient("X");
        F2Client clientY = F2Lock_Test.newClient("Y");

        F2ClientEntry clientXHoldsLockA = F2Lock_Test.newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYHoldsLockB = F2Lock_Test.newEntry(clientY, EXCLUSIVE);
        F2ClientEntry clientXWaitsForLockB = F2Lock_Test.newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYWaitsForLockA = F2Lock_Test.newEntry(clientY, EXCLUSIVE);

        lock1.acquire(BLOCKING, clientXHoldsLockA);
        lock2.acquire(BLOCKING, clientYHoldsLockB);

        lock1.acquire(BLOCKING, clientYWaitsForLockA);
        clientY.waitsFor = clientYWaitsForLockA;

        lock2.acquire(BLOCKING, clientXWaitsForLockB);
        clientX.waitsFor = clientXWaitsForLockB;

        DeadlockDescription expectedDeadlock = new DeadlockDescription(
                clientXWaitsForLockB, clientYWaitsForLockA, clientXWaitsForLockB );
        DeadlockDescription deadlock = new DeadlockDetector().detectDeadlock(clientX);

        assert deadlock.equals(expectedDeadlock) : String.format("Expected %s, found %s", expectedDeadlock, deadlock);
    }

    static ResourceType NODE = new ResourceType() {
        @Override
        public int typeId() {
            return 1;
        }

        @Override
        public WaitStrategy waitStrategy() {
            return LockWaitStrategies.INCREMENTAL_BACKOFF;
        }

        @Override
        public String name() {
            return "Node";
        }
    };

    static ResourceType SCHEMA = new ResourceType() {
        @Override
        public int typeId() {
            return 0;
        }

        @Override
        public WaitStrategy waitStrategy() {
            return LockWaitStrategies.INCREMENTAL_BACKOFF;
        }

        @Override
        public String name() {
            return "Schema";
        }
    };
}
