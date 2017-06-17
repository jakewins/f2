package com.jakewins.f2;

import org.junit.Test;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.storageengine.api.lock.ResourceType;
import sun.misc.Signal;
import sun.misc.SignalHandler;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.function.*;

import static com.jakewins.f2.DeadlockDetector_Test.NODE;
import static com.jakewins.f2.F2Client_Test.Call.call;

public class F2Client_Test {
    @Test
    public void testReentrancy() {
        StubF2Partitions partitions = new StubF2Partitions();
        F2Client client = new F2Client(8, partitions, null);

        // Expect
        partitions.partition.lock.expect(
            call("acquire", (args) -> {
                assert args[0] == AcquireMode.BLOCKING;
                F2ClientEntry expected = new F2ClientEntry(client, null, LockMode.EXCLUSIVE, NODE, 0, new int[]{0,0}, null);
                assert args[1].equals(expected) : String.format("Expected %s got %s", expected, args[1]);
                return F2Lock.AcquireOutcome.ACQUIRED;
            })
        );

        // When I acquire the same lock multiple times, the global lock only gets grabbed once
        client.acquireExclusive(LockTracer.NONE, NODE, 0);
        client.acquireExclusive(LockTracer.NONE, NODE, 0);
        client.acquireExclusive(LockTracer.NONE, NODE, 0);
    }

    @Test
    public void testAcquireReentrant__release__acquire() {
        StubF2Partitions partitions = new StubF2Partitions();
        F2Client client = new F2Client(8, partitions, null);

        // Expect one request to acquire
        partitions.partition.lock.expect(
                call("acquire", (args) -> {
                    assert args[0] == AcquireMode.BLOCKING;

                    F2ClientEntry expected = new F2ClientEntry(client, null, LockMode.EXCLUSIVE, NODE, 0, new int[]{0,0}, null);
                    assert args[1].equals(expected) : String.format("Expected %s got %s", expected, args[1]);

                    ((F2ClientEntry)args[1]).lock = partitions.partition.lock;
                    return F2Lock.AcquireOutcome.ACQUIRED;
                })
        );


        // When I acquire twice and release once on the client
        client.acquireExclusive(LockTracer.NONE, NODE, 0);
        client.acquireExclusive(LockTracer.NONE, NODE, 0);
        client.releaseExclusive(NODE, 0);

        // Expect one request to release
        partitions.partition.lock.expect(
                call("release", (args) -> {
                    F2ClientEntry expected = new F2ClientEntry(client, partitions.partition.lock, LockMode.EXCLUSIVE, NODE, 0, new int[]{0,0}, null);
                    assert args[0].equals(expected) : String.format("Expected %s got %s", expected, args[0]);
                    return F2Lock.ReleaseOutcome.LOCK_IDLE;
                })
        );

        // When I release the second time
        client.releaseExclusive(NODE, 0);
    }

    static class StubF2Partitions extends F2Partitions {
        StubF2Partition partition = new StubF2Partition();

        StubF2Partitions() {
            super(8, 1);
        }

        @Override
        F2Partition getPartition(long resourceId) {
            return partition;
        }
    }

    static class StubF2Partition extends F2Partition {

        StubF2Lock lock = new StubF2Lock();

        StubF2Partition() {
            super(5, 1);
        }

        @Override
        F2Lock getOrCreateLock(ResourceType resourceType, long resourceId) {
            return lock;
        }

        @Override
        void removeLock(ResourceType resourceType, long resourceId) {

        }
    }

    static class StubF2Lock extends F2Lock {

        private final LinkedList<Call> expectedCalls = new LinkedList<>();

        @Override
        AcquireOutcome acquire(AcquireMode acquireMode, F2ClientEntry entry) {
            return this.nextCall("acquire", acquireMode, entry);
        }

        @Override
        ReleaseOutcome release(F2ClientEntry entry) {
            return this.nextCall("release", entry);
        }

        @Override
        ReleaseOutcome errorCleanup(F2ClientEntry entry) {
            return this.nextCall("errorCleanup", entry);
        }

        @Override
        public String toString() {
            return "StubLock()";
        }

        <T> T nextCall(String method, Object ... args) {
            if(expectedCalls.size() == 0) {
                throw new AssertionError(String.format("Expected no more calls, but %s#%s(%s) was just called.", this, method, Arrays.toString(args)));
            }
            Call next = expectedCalls.pop();
            if(!next.method.equals(method)) {
                throw new AssertionError(String.format("Expected %s%s, but %s#%s(%s) was just called.", this, next.method, this, method, Arrays.toString(args)));
            }

            return (T)next.behavior.apply(args);
        }

        void expect(Call ... calls) {
            expectedCalls.addAll(Arrays.asList(calls));
        }
    }

    static class Call {
        static Call call(String method, Function<Object[], Object> behavior) {
            Call call = new Call();
            call.method = method;
            call.behavior = behavior;
            return call;
        }

        String method;
        Function<Object[], Object> behavior;
    }
}
