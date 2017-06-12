package com.jakewins.f2;

import com.jakewins.f2.include.ResourceType;
import com.jakewins.f2.include.WaitStrategy;
import org.junit.Test;

import static com.jakewins.f2.AcquireMode.BLOCKING;
import static com.jakewins.f2.LockMode.EXCLUSIVE;

public class DeadlockDetector_Test {
    @Test
    public void testPreschoolDeadlock() {
        F2Lock lock1 = newLock(1);
        F2Lock lock2 = newLock(2);
        F2Client clientX = newClient("X");
        F2Client clientY = newClient("Y");

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
                clientXWaitsForLockB, clientYWaitsForLockA, clientXWaitsForLockB
        );
        DeadlockDescription deadlock = new DeadlockDetector().detectDeadlock(clientX);

        assert deadlock.equals(expectedDeadlock) : String.format("Expected %s, found %s", expectedDeadlock, deadlock);
    }

    private F2Lock newLock(long resourceId) {
        F2Lock lock = new F2Lock();
        lock.resourceType = NODE;
        lock.resourceId = resourceId;
        return lock;
    }

    private F2Client newClient(String name) {
        F2Client client = new F2Client(null, null);
        client.setName(name);
        return client;
    }

    private F2ClientEntry newEntry(F2Client owner, LockMode lockMode) {
        F2ClientEntry entry = new F2ClientEntry();
        entry.owner = owner;
        entry.lockMode = lockMode;
        return entry;
    }

    public static ResourceType NODE = new ResourceType() {
        @Override
        public int typeId() {
            return 0;
        }

        @Override
        public WaitStrategy waitStrategy() {
            return null;
        }

        @Override
        public String name() {
            return "Node";
        }
    };
}
