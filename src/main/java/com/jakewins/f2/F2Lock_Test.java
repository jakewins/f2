package com.jakewins.f2;

import com.jakewins.f2.F2Lock.AcquireOutcome;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.jakewins.f2.AcquireMode.BLOCKING;
import static com.jakewins.f2.AcquireMode.NONBLOCKING;
import static com.jakewins.f2.F2Lock.AcquireOutcome.ACQUIRED;
import static com.jakewins.f2.F2Lock.AcquireOutcome.MUST_WAIT;
import static com.jakewins.f2.F2Lock.AcquireOutcome.NOT_ACQUIRED;
import static com.jakewins.f2.LockMode.EXCLUSIVE;
import static com.jakewins.f2.LockMode.SHARED;
import static com.jakewins.f2.LockMode.UPGRADE;
import static java.util.Arrays.asList;

public class F2Lock_Test {
    @Test
    public void testExclusiveBasics() {
        // Given no holders..
        test(acquireExclusive(BLOCKING), ACQUIRED);
        test(acquireExclusive(NONBLOCKING), ACQUIRED);

        // Given the lock is already held by sharers
        test(given(acquireShared), acquireExclusive(BLOCKING), MUST_WAIT);
        test(given(acquireShared), acquireExclusive(NONBLOCKING), NOT_ACQUIRED);

        // Given the lock is already held exclusively..
        test(given(acquireExclusive), acquireExclusive(BLOCKING), MUST_WAIT);
        test(given(acquireExclusive), acquireExclusive(NONBLOCKING), NOT_ACQUIRED);
    }

    @Test
    public void testSharedBasics() {
        // Given no holders..
        test(when(acquireShared(BLOCKING)), ACQUIRED);
        test(when(acquireShared(NONBLOCKING)), ACQUIRED);

        // Given the lock is already held by other sharers..
        test(given(acquireShared), when(acquireShared(BLOCKING)), ACQUIRED);
        test(given(acquireShared), when(acquireShared(NONBLOCKING)), ACQUIRED);

        // Given the lock is already held exclusively..
        test(given(acquireExclusive), when(acquireShared(BLOCKING)), MUST_WAIT);
        test(given(acquireExclusive), when(acquireShared(NONBLOCKING)), NOT_ACQUIRED);
    }

    @Test
    public void testAcquireAndReleaseShared() {
        F2ClientEntry clientASharedLock = newEntry(newClient("A"), SHARED);
        F2ClientEntry clientBExclusiveLock = newEntry(newClient("B"), EXCLUSIVE);

        test(
            given(
                acquireShared(clientASharedLock),
                acquireExclusive(clientBExclusiveLock)),
            when(
                release(clientASharedLock)),
            then(
                lockIsHeldExclusivelyBy(clientBExclusiveLock),
                noSharedHolders,
                waitListIsEmpty,
                noCurrentHolderIsWaiting));
    }

    @Test
    public void testUncontendedUpgrade() {
        F2Client clientA = newClient("A");
        F2ClientEntry clientASharedLock = newEntry(clientA, SHARED);
        F2ClientEntry clientAUpgradeLock = newEntry(clientA, UPGRADE);

        test(
            given(
                    acquireShared(clientASharedLock)),
            when(
                    acquireUpgrade(clientAUpgradeLock)),
            then(
                    lockIsHeldExclusivelyBy(clientAUpgradeLock),
                    lockIsHeldSharedBy(clientASharedLock),
                    waitListIsEmpty,
                    noCurrentHolderIsWaiting));
    }

    @Test
    public void testUpgradePreemptsExclusiveWaiterToAvoidDeadlock() {
        F2Client clientA = newClient("A");
        F2ClientEntry clientASharedLock = newEntry(clientA, SHARED);
        F2ClientEntry clientAUpgradeLock = newEntry(clientA, UPGRADE);
        F2ClientEntry clientBSharedLock = newEntry(newClient("B"), SHARED);
        F2ClientEntry clientCExclusiveLock = newEntry(newClient("C"), EXCLUSIVE);

        test(
                given(
                        acquireShared(clientASharedLock),
                        acquireShared(clientBSharedLock),
                        acquireExclusive(clientCExclusiveLock)), // Eg. client B is waiting for an exclusive lock
                when(
                        acquireUpgrade(clientAUpgradeLock)),
                then(
                        noExclusiveHolder,
                        lockIsHeldSharedBy(clientBSharedLock, clientASharedLock),
                        waitListIs(clientAUpgradeLock, clientCExclusiveLock)));
    }

    @Test
    public void testReleasingSharedThatBlocksUpgradeGrantsUpgrade() {
        F2Client clientA = newClient("A");
        F2ClientEntry clientASharedLock = newEntry(clientA, SHARED);
        F2ClientEntry clientAUpgradeLock = newEntry(clientA, UPGRADE);
        F2ClientEntry clientBSharedLock = newEntry(newClient("B"), SHARED);

        test(
                given(
                        acquireShared(clientASharedLock),
                        acquireShared(clientBSharedLock),
                        acquireUpgrade(clientAUpgradeLock)), // Which will block on client B's shared lock
                when(
                        release(clientBSharedLock)),
                then(
                        lockIsHeldExclusivelyBy(clientAUpgradeLock),
                        lockIsHeldSharedBy(clientASharedLock),
                        waitListIsEmpty,
                        noCurrentHolderIsWaiting));
    }

    @Test
    public void testReleasingSharedPortionOfUpgradeDoesNotGrantWaitersAccess() {
        F2Client clientA = newClient("A");
        F2ClientEntry clientASharedLock = newEntry(clientA, SHARED);
        F2ClientEntry clientAUpgradeLock = newEntry(clientA, UPGRADE);
        F2ClientEntry clientBSharedLock = newEntry(newClient("B"), SHARED);

        test(
                given(
                        acquireShared(clientASharedLock),
                        acquireUpgrade(clientAUpgradeLock),
                        acquireShared(clientBSharedLock)), // Waiting on upgrade lock
                when(
                        release(clientASharedLock)),
                then(
                        lockIsHeldExclusivelyBy(clientAUpgradeLock),
                        noSharedHolders,
                        waitListIs(clientBSharedLock),
                        noCurrentHolderIsWaiting));
    }

    @Test
    public void testReleasingExclusivePortionOfUpgradeLockGrantsSharedWaitersAccess() {
        F2Client clientA = newClient("A");
        F2ClientEntry clientASharedLock = newEntry(clientA, SHARED);
        F2ClientEntry clientAUpgradeLock = newEntry(clientA, UPGRADE);
        F2ClientEntry clientBSharedLock = newEntry(newClient("B"), SHARED);

        test(
                given(
                        acquireShared(clientASharedLock),
                        acquireUpgrade(clientAUpgradeLock),
                        acquireShared(clientBSharedLock)), // Waiting on upgrade lock
                when(
                        release(clientAUpgradeLock)),
                then(
                        noExclusiveHolder,
                        lockIsHeldSharedBy(clientBSharedLock, clientASharedLock),
                        waitListIsEmpty,
                        noCurrentHolderIsWaiting));
    }

    @Test
    public void testCleanupExclusiveWaitingOnExclusive() {
        F2ClientEntry onWaitList = newEntry(newClient("A"));
        F2ClientEntry currentHolder = newEntry(newClient("B"));
        testErrorCleanup(
                given(acquireExclusive(currentHolder), acquireExclusive(onWaitList)),
                onWaitList,
                then(lockIsHeldExclusivelyBy(currentHolder), waitListIsEmpty, noCurrentHolderIsWaiting));
    }

    @Test
    public void testCleanupExclusiveWaitingOnShared() {
        F2ClientEntry onWaitList = newEntry(newClient("A"));
        F2ClientEntry currentHolder = newEntry(newClient("B"));
        testErrorCleanup(
                given(acquireShared(currentHolder), acquireExclusive(onWaitList)),
                onWaitList,
                then(lockIsHeldSharedBy(currentHolder), waitListIsEmpty, noExclusiveHolder, noCurrentHolderIsWaiting));
    }

    @Test
    public void testCleanupSharedWaitingOnExclusive() {
        F2ClientEntry onWaitList = newEntry(newClient("A"));
        F2ClientEntry currentHolder = newEntry(newClient("B"));
        testErrorCleanup(
                given(acquireExclusive(currentHolder), acquireShared(onWaitList)),
                onWaitList,
                then(lockIsHeldExclusivelyBy(currentHolder), waitListIsEmpty, noCurrentHolderIsWaiting));
    }

    @Test
    public void testCleanupGrantedExclusive() {
        F2ClientEntry currentHolder = newEntry(newClient("A"));
        testErrorCleanup(
                given(acquireExclusive(currentHolder)),
                currentHolder,
                then(noExclusiveHolder));
    }

    @Test
    public void testCleanupGrantedShared() {
        F2ClientEntry currentHolder = newEntry(newClient("A"));
        testErrorCleanup(
                given(acquireShared(currentHolder)),
                currentHolder,
                then(noSharedHolders));
    }

    private void test(Function<F2Lock, AcquireOutcome> actionUnderTest, AcquireOutcome expectedOutcome) {
        test(noSetup, actionUnderTest, expectedOutcome, noAssertions);
    }

    private void test(Consumer<F2Lock> setup, Function<F2Lock, AcquireOutcome> actionUnderTest, AcquireOutcome expectedOutcome) {
        test(setup, actionUnderTest, expectedOutcome, noAssertions);
    }

    private void test(Consumer<F2Lock> setup, Function<F2Lock, AcquireOutcome> actionUnderTest, Consumer<F2Lock> assertions) {
        test(setup, actionUnderTest, null, assertions);
    }

    private void test(Consumer<F2Lock> setup, Function<F2Lock, AcquireOutcome> actionUnderTest, AcquireOutcome expectedOutcome, Consumer<F2Lock> assertions) {
        F2Lock lock = new F2Lock();
        lock.resourceType = DeadlockDetector_Test.NODE;
        lock.resourceId = 0; // RIP, reference node.
        setup.accept(lock);

        AcquireOutcome outcome = actionUnderTest.apply(lock);

        assert expectedOutcome == null || outcome == expectedOutcome : String.format("Expected %s but got %s", expectedOutcome.name(), outcome.name());
        asList(assertions).forEach( a -> a.accept(lock) );
    }

    private void testErrorCleanup(Consumer<F2Lock> setup, F2ClientEntry entryToCleanUp, Consumer<F2Lock> assertions) {
        F2Lock lock = new F2Lock();
        setup.accept(lock);

        lock.errorCleanup(entryToCleanUp);

        assertions.accept(lock);
    }


    // ACQUIRE SHARED
    private static Function<F2Lock, AcquireOutcome> acquireShared(F2ClientEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = LockMode.SHARED;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, AcquireOutcome> acquireShared(AcquireMode mode) {
        return acquireShared(newEntry(newClient()), mode);
    }
    private static Function<F2Lock, AcquireOutcome> acquireShared(F2ClientEntry entry) {
        return acquireShared(entry, BLOCKING);
    }
    private static Function<F2Lock, AcquireOutcome> acquireShared = acquireShared(newEntry(newClient()), BLOCKING);


    // ACQUIRE EXCLUSIVE
    private static Function<F2Lock, AcquireOutcome> acquireExclusive(F2ClientEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = EXCLUSIVE;
            return l.acquire(mode, entry);
        };
    }

    private static Function<F2Lock, AcquireOutcome> acquireExclusive(AcquireMode mode) {
        return acquireExclusive(newEntry(newClient()), mode);
    }
    private static Function<F2Lock, AcquireOutcome> acquireExclusive(F2ClientEntry entry) {
        return acquireExclusive(entry, BLOCKING);
    }
    private static Function<F2Lock, AcquireOutcome> acquireExclusive = acquireExclusive(newEntry(newClient()), BLOCKING);


    // ACQUIRE UPGRADE
    private static Function<F2Lock, AcquireOutcome> acquireUpgrade(F2ClientEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = UPGRADE;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, AcquireOutcome> acquireUpgrade(F2ClientEntry entry) {
        return acquireUpgrade(entry, BLOCKING);
    }

    // OTHER
    private static Function<F2Lock, AcquireOutcome> release(F2ClientEntry entry) {
        return (l) -> {
            l.release(entry);
            return null;
        };
    }

    private static Consumer<F2Lock> given(Function<F2Lock, AcquireOutcome> ... actions) {
        return lock -> asList(actions).forEach(a -> a.apply(lock));
    }
    private static Consumer<F2Lock> noSetup = l -> {};
    private static Consumer<F2Lock> noAssertions = l -> {};

    // Purely to help reading the tests
    private static Function<F2Lock, AcquireOutcome> when(Function<F2Lock, AcquireOutcome> action) {
        return action;
    }

    private static Consumer<F2Lock> then(Consumer<F2Lock> ... assertion) {
        return lock -> asList(assertion).forEach(a -> a.accept(lock));
    }

    private static Consumer<F2Lock> waitListIsEmpty = (lock) -> { assert lock.waitList == null : "There should be no wait list, found " + lock.waitList; };
    private static Consumer<F2Lock> waitListIs(F2ClientEntry ... waiters) {
        return (lock) -> {
            Iterator<F2ClientEntry> nextExpected = asList(waiters).iterator();
            F2ClientEntry current = lock.waitList;
            while(current != null) {
                assert nextExpected.hasNext() : "There are more waiters than expected, first additional: " + current;

                F2ClientEntry expected = nextExpected.next();
                assert current == expected: "Expected " + expected + " to be next waiter, but found " + current;

                current = current.next;
            }
            assert !nextExpected.hasNext() : "Expected " + nextExpected.next() + " to be on wait list.";
        };
    }


    private static Consumer<F2Lock> noExclusiveHolder = (lock) -> { assert lock.exclusiveHolder == null : "There should be no exclusive holder, found " + lock.exclusiveHolder; };
    private static Consumer<F2Lock> noSharedHolders = (lock) -> { assert lock.sharedHolderList == null : "There should be shared holder, found " + lock.sharedHolderList; };
    private static Consumer<F2Lock> lockIsHeldExclusivelyBy(F2ClientEntry holder) {
        return (lock) -> { assert lock.exclusiveHolder == holder : "Lock should be held by " + holder; };
    }
    private static Consumer<F2Lock> lockIsHeldSharedBy(F2ClientEntry ... holders) {
        return (lock) -> {
            Iterator<F2ClientEntry> nextExpected = asList(holders).iterator();
            F2ClientEntry current = lock.sharedHolderList;
            while(current != null) {
                assert nextExpected.hasNext() : "There are more holders of the shared lock than expected, first additional: " + current;

                F2ClientEntry expected = nextExpected.next();
                assert current == expected: "Expected " + expected + " to be next holder of shared lock, but found " + current;

                current = current.next;
            }
            assert !nextExpected.hasNext() : "Expected " + nextExpected.next() + " to hold shared lock.";
        };
    }
    private static Consumer<F2Lock> noCurrentHolderIsWaiting = (lock) -> {
        assert lock.exclusiveHolder == null || lock.exclusiveHolder.owner.waitsFor == null : "Exclusive holder should not be marked as waiting: " + lock.exclusiveHolder.owner + " waitsFor " + lock.exclusiveHolder.owner.waitsFor;
        for(F2ClientEntry shared = lock.sharedHolderList; shared != null; shared = shared.next) {
            assert shared.owner.waitsFor == null : "Shared holder should not be marked as waiting: " + shared.owner + " waitsFor " + shared.owner.waitsFor;
        }
    };

    static F2Lock newLock(long resourceId) {
        F2Lock lock = new F2Lock();
        lock.resourceType = DeadlockDetector_Test.NODE;
        lock.resourceId = resourceId;
        return lock;
    }

    static F2Client newClient() {
        String randomName = new BigInteger(32, new Random()).toString(32);
        return newClient(randomName);
    }

    static F2Client newClient(String name) {
        F2Client client = new F2Client(1, null, null);
        client.setName(name);
        return client;
    }

    static F2ClientEntry newEntry(F2Client owner) {
        return newEntry(owner, EXCLUSIVE);
    }

    static F2ClientEntry newEntry(F2Client owner, LockMode lockMode) {
        F2ClientEntry entry = new F2ClientEntry();
        entry.owner = owner;
        entry.lockMode = lockMode;
        return entry;
    }

}
