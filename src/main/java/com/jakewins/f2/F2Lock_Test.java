package com.jakewins.f2;

import com.jakewins.f2.F2Lock.Outcome;
import org.junit.Test;

import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.jakewins.f2.AcquireMode.BLOCKING;
import static com.jakewins.f2.AcquireMode.NONBLOCKING;
import static com.jakewins.f2.F2Lock.Outcome.ACQUIRED;
import static com.jakewins.f2.F2Lock.Outcome.MUST_WAIT;
import static com.jakewins.f2.F2Lock.Outcome.NOT_ACQUIRED;
import static java.util.Arrays.asList;

public class F2Lock_Test {
    @Test
    public void testExclusiveBasics() {
        // Given no holders..
        testAcquire(noSetup, exclusiveLock(BLOCKING), ACQUIRED);
        testAcquire(noSetup, exclusiveLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held by sharers
        testAcquire(given(sharedLock), exclusiveLock(BLOCKING), MUST_WAIT);
        testAcquire(given(sharedLock), exclusiveLock(NONBLOCKING), NOT_ACQUIRED);

        // Given the lock is already held exclusively..
        testAcquire(given(exclusiveLock), exclusiveLock(BLOCKING), MUST_WAIT);
        testAcquire(given(exclusiveLock), exclusiveLock(NONBLOCKING), NOT_ACQUIRED);
    }

    @Test
    public void testSharedBasics() {
        // Given no holders..
        testAcquire(noSetup, sharedLock(BLOCKING), ACQUIRED);
        testAcquire(noSetup, sharedLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held by other sharers..
        testAcquire(given(sharedLock), sharedLock(BLOCKING), ACQUIRED);
        testAcquire(given(sharedLock), sharedLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held exclusively..
        testAcquire(given(exclusiveLock), sharedLock(BLOCKING), MUST_WAIT);
        testAcquire(given(exclusiveLock), sharedLock(NONBLOCKING), NOT_ACQUIRED);
    }

    @Test
    public void testCleanupExclusiveWaitingOnExclusive() {
        F2ClientEntry onWaitList = new F2ClientEntry();
        F2ClientEntry currentHolder = new F2ClientEntry();
        testErrorCleanup(
                given(exclusiveLock(currentHolder), exclusiveLock(onWaitList)),
                onWaitList,
                then(lockIsHeldExclusivelyBy(currentHolder), waitListIsEmpty));
    }

    @Test
    public void testCleanupExclusiveWaitingOnShared() {
        F2ClientEntry onWaitList = new F2ClientEntry();
        F2ClientEntry currentHolder = new F2ClientEntry();
        testErrorCleanup(
                given(sharedLock(currentHolder), exclusiveLock(onWaitList)),
                onWaitList,
                then(lockIsHeldSharedBy(currentHolder), waitListIsEmpty, noExclusiveHolder));
    }

    private void testAcquire(Consumer<F2Lock> setup, Function<F2Lock, Outcome> actionUnderTest, Outcome expectedOutcome) {
        F2Lock lock = new F2Lock();
        setup.accept(lock);

        Outcome outcome = actionUnderTest.apply(lock);

        assert outcome == expectedOutcome : String.format("Expected %s but got %s", expectedOutcome.name(), outcome.name());
    }

    private void testErrorCleanup(Consumer<F2Lock> setup, F2ClientEntry entryToCleanUp, Consumer<F2Lock> assertions) {
        F2Lock lock = new F2Lock();
        setup.accept(lock);

        lock.errorCleanup(entryToCleanUp);

        assertions.accept(lock);
    }

    private static Function<F2Lock, Outcome> sharedLock(F2ClientEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = LockMode.SHARED;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, Outcome> sharedLock(AcquireMode mode) {
        return sharedLock(new F2ClientEntry(), mode);
    }
    private static Function<F2Lock, Outcome> sharedLock(F2ClientEntry entry) {
        return sharedLock(entry, BLOCKING);
    }
    private static Function<F2Lock, Outcome> sharedLock = sharedLock(new F2ClientEntry(), BLOCKING);

    private static Function<F2Lock, Outcome> exclusiveLock(F2ClientEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = LockMode.EXCLUSIVE;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, Outcome> exclusiveLock(AcquireMode mode) {
        return exclusiveLock(new F2ClientEntry(), mode);
    }
    private static Function<F2Lock, Outcome> exclusiveLock(F2ClientEntry entry) {
        return exclusiveLock(entry, BLOCKING);
    }
    private static Function<F2Lock, Outcome> exclusiveLock = exclusiveLock(new F2ClientEntry(), BLOCKING);

    private static Consumer<F2Lock> given(Function<F2Lock, Outcome> ... actions) {
        return lock -> asList(actions).forEach(a -> a.apply(lock));
    }
    private static Consumer<F2Lock> noSetup = l -> {};

    private static Consumer<F2Lock> then(Consumer<F2Lock> ... assertion) {
        return lock -> asList(assertion).forEach(a -> a.accept(lock));
    }

    private static Consumer<F2Lock> waitListIsEmpty = (lock) -> { assert lock.waitList == null : "Wait list should be empty"; };
    private static Consumer<F2Lock> noExclusiveHolder = (lock) -> { assert lock.exclusiveHolder == null : "Wait list should be empty"; };
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
}
