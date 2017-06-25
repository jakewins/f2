package com.jakewins.f2;

import org.junit.Test;
import org.neo4j.kernel.impl.util.concurrent.LockWaitStrategies;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.neo4j.storageengine.api.lock.WaitStrategy;

import static com.jakewins.f2.AcquireMode.BLOCKING;
import static com.jakewins.f2.F2Lock_Test.newEntry;
import static com.jakewins.f2.LockMode.EXCLUSIVE;

public class DeadlockDetector_Test {
    @Test
    public void testPreschoolDeadlock() {
        F2Lock lock1 = F2Lock_Test.newLock(1);
        F2Lock lock2 = F2Lock_Test.newLock(2);
        F2Client clientX = F2Lock_Test.newClient("X");
        F2Client clientY = F2Lock_Test.newClient("Y");

        F2ClientEntry clientXHoldsLockA = newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYHoldsLockB = newEntry(clientY, EXCLUSIVE);
        F2ClientEntry clientXWaitsForLockB = newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYWaitsForLockA = newEntry(clientY, EXCLUSIVE);

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

    @Test
    public void testUnrelatedDeadlockDoesNotCauseInfiniteLoop() {
        F2Lock lock1 = F2Lock_Test.newLock(1);
        F2Lock lock2 = F2Lock_Test.newLock(2);
        F2Client clientX = F2Lock_Test.newClient("X");
        F2Client clientY = F2Lock_Test.newClient("Y");
        F2Client clientZ = F2Lock_Test.newClient("Z");

        F2ClientEntry clientXHoldsLockA = newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYHoldsLockB = newEntry(clientY, EXCLUSIVE);
        F2ClientEntry clientXWaitsForLockB = newEntry(clientX, EXCLUSIVE);
        F2ClientEntry clientYWaitsForLockA = newEntry(clientY, EXCLUSIVE);
        F2ClientEntry clientZWaitsForLockB = newEntry(clientZ, EXCLUSIVE);

        lock1.acquire(BLOCKING, clientXHoldsLockA);
        lock2.acquire(BLOCKING, clientYHoldsLockB);

        lock1.acquire(BLOCKING, clientYWaitsForLockA);
        clientY.waitsFor = clientYWaitsForLockA;

        lock2.acquire(BLOCKING, clientXWaitsForLockB);
        clientX.waitsFor = clientXWaitsForLockB;

        lock2.acquire(BLOCKING, clientZWaitsForLockB);
        clientZ.waitsFor = clientZWaitsForLockB;

        DeadlockDescription expectedDeadlock = DeadlockDetector.NONE;
        DeadlockDescription deadlock = new DeadlockDetector().detectDeadlock(clientZ);

        assert deadlock.equals(expectedDeadlock) : String.format("Expected %s, found %s", expectedDeadlock, deadlock);
    }

    @Test
    public void testHasHelpfulDeadlockDescription() {
        // Given
        F2Lock lock1 = F2Lock_Test.newLock(1);
        F2Lock lock2 = F2Lock_Test.newLock(2);
        F2Client clientA = F2Lock_Test.newClient("MATCH (n) RETURN n");
        F2Client clientB = F2Lock_Test.newClient("CREATE (n:User)");

        F2ClientEntry clientAWaitsLock1 = newEntry(clientA, EXCLUSIVE, lock1);
        F2ClientEntry clientBHoldsLock1 = newEntry(clientB, EXCLUSIVE, lock1);
        F2ClientEntry clientBWaitsLock2 = newEntry(clientB, EXCLUSIVE, lock2);
        F2ClientEntry clientAHoldsLock2 = newEntry(clientA, EXCLUSIVE, lock2);

        lock1.waitList = clientAWaitsLock1;
        lock1.exclusiveHolder = clientBHoldsLock1;
        lock2.waitList = clientBWaitsLock2;
        lock2.exclusiveHolder = clientAHoldsLock2;

        // When
        DeadlockDescription deadlock = new DeadlockDescription(clientAWaitsLock1, clientBWaitsLock2, clientAHoldsLock2);

        // Then
        String expected = "Deadlock (A)-[:WAITS_FOR]->(EXCLUSIVE Lock(Node[1]))->[:BLOCKED_BY]->(B)-[:WAITS_FOR]->(EXCLUSIVE Lock(Node[2]))->[:BLOCKED_BY]->(MATCH (n) RETURN n)\n" +
                "Where:\n" +
                "  A: MATCH (n) RETURN n\n" +
                "  B: CREATE (n:User)\n";
        assert deadlock.toString().equals(expected) : String.format("Expected '%s', found '%s'", expected, deadlock.toString());
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
