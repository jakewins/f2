package com.jakewins.f2;

import com.jakewins.f2.F2Lock.Outcome;
import org.junit.Test;

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
        test(noSetup, exclusiveLock(BLOCKING), ACQUIRED);
        test(noSetup, exclusiveLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held by sharers
        test(setup(sharedLock), exclusiveLock(BLOCKING), MUST_WAIT);
        test(setup(sharedLock), exclusiveLock(NONBLOCKING), NOT_ACQUIRED);

        // Given the lock is already held exclusively..
        test(setup(exclusiveLock), exclusiveLock(BLOCKING), MUST_WAIT);
        test(setup(exclusiveLock), exclusiveLock(NONBLOCKING), NOT_ACQUIRED);
    }

    @Test
    public void testSharedBasics() {
        // Given no holders..
        test(noSetup, sharedLock(BLOCKING), ACQUIRED);
        test(noSetup, sharedLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held by other sharers..
        test(setup(sharedLock), sharedLock(BLOCKING), ACQUIRED);
        test(setup(sharedLock), sharedLock(NONBLOCKING), ACQUIRED);

        // Given the lock is already held exclusively..
        test(setup(exclusiveLock), sharedLock(BLOCKING), MUST_WAIT);
        test(setup(exclusiveLock), sharedLock(NONBLOCKING), NOT_ACQUIRED);
    }

    private void test(Consumer<F2Lock> setup, Function<F2Lock, Outcome> actionUnderTest, Outcome expectedOutcome) {
        F2Lock lock = new F2Lock();
        setup.accept(lock);

        Outcome outcome = actionUnderTest.apply(lock);

        assert outcome == expectedOutcome : String.format("Expected %s but got %s", expectedOutcome.name(), outcome.name());
    }


    private static Function<F2Lock, Outcome> sharedLock(LockEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = LockMode.SHARED;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, Outcome> sharedLock(AcquireMode mode) {
        return sharedLock(new LockEntry(), mode);
    }
    private static Function<F2Lock, Outcome> sharedLock = sharedLock(new LockEntry(), BLOCKING);

    private static Function<F2Lock, Outcome> exclusiveLock(LockEntry entry, AcquireMode mode) {
        return (l) -> {
            entry.lockMode = LockMode.EXCLUSIVE;
            return l.acquire(mode, entry);
        };
    }
    private static Function<F2Lock, Outcome> exclusiveLock(AcquireMode mode) {
        return exclusiveLock(new LockEntry(), mode);
    }
    private static Function<F2Lock, Outcome> exclusiveLock = exclusiveLock(new LockEntry(), BLOCKING);


    private static Consumer<F2Lock> setup(Function<F2Lock, Outcome> ... actions) {
        return lock -> asList(actions).forEach(a -> a.apply(lock));
    }
    private static Consumer<F2Lock> noSetup = l -> {};
}
