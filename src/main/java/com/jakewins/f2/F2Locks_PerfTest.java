package com.jakewins.f2;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.lock.forseti.ForsetiLockManager;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.ResourceType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.time.Clock;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.jakewins.f2.DeadlockDetector_Test.NODE;
import static com.jakewins.f2.DeadlockDetector_Test.SCHEMA;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class F2Locks_PerfTest {

    @State(Scope.Benchmark)
    public static class SharedState {
        private Locks forseti;
        private Locks f2;

        @Setup
        public void setup() {
            this.forseti = new ForsetiLockManager(Config.defaults(), Clock.systemUTC(), NODE, SCHEMA);
            this.f2 = new F2Locks(new ResourceType[]{NODE, SCHEMA}, 64);
        }
    }

    private Locks.Client forsetiClient;
    private Locks.Client f2Client;
    private Random random;

    @Setup
    public void setup(SharedState shared) {
        this.forsetiClient = shared.forseti.newClient();
        this.f2Client = shared.f2.newClient();
        this.random = new Random();
        Thread.currentThread().setName("CLIENT " + this.f2Client);
    }

//    @Benchmark
//    public void f2AcquireContendedShared() throws AcquireLockTimeoutException {
//        f2Client.acquireShared(LockTracer.NONE, NODE, 0);
//        f2Client.releaseShared(NODE, 0);
//    }
//
//    @Benchmark
//    public void forsetiAcquireContendedShared() throws AcquireLockTimeoutException {
//        forsetiClient.acquireShared(LockTracer.NONE, NODE, 0);
//        forsetiClient.releaseShared(NODE, 0);
//    }
//
//    @Benchmark
//    public void f2AcquireContendedExclusive() throws AcquireLockTimeoutException {
//        f2Client.acquireExclusive(LockTracer.NONE, NODE, 0);
//        f2Client.releaseExclusive(NODE, 0);
//    }
//
//    @Benchmark
//    public void forsetiAcquireContendedExclusive() throws AcquireLockTimeoutException {
//        for(;;) {
//            try {
//                forsetiClient.acquireExclusive(LockTracer.NONE, NODE, 0);
//                forsetiClient.releaseExclusive(NODE, 0);
//                return;
//            } catch (DeadlockDetectedException e) {
//                // This use case can't deadlock, but Forseti finds false positives.
//                // Correct for this by forcing Forseti to keep going until it actually acquires and releases the
//                // lock once.
//            }
//        }
//    }

    @Benchmark
    public void f2AcquireCombination() throws AcquireLockTimeoutException {
        for(;;) {
            try {
                // Always acquire the schema lock
                f2Client.acquireShared(LockTracer.NONE, SCHEMA, 0);

                // Then acquire 5 random locks
                for (int i = 0; i < 5; i++) {
                    f2Client.acquireExclusive(LockTracer.NONE, NODE, random.nextInt(100));
                }
                return;
            } catch (DeadlockDetectedException e) {
                // Retry
            } finally {
                f2Client.close(); // technically not allowed to reuse after this, but current impl allows this
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(F2Locks_PerfTest.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(5)
                .threads(Runtime.getRuntime().availableProcessors()*4)
                .forks(1)
                .syncIterations(true)
                .build();

        new Runner(opt).run();
    }

}