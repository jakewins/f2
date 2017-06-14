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
import java.util.concurrent.TimeUnit;

import static com.jakewins.f2.DeadlockDetector_Test.NODE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class F2Locks_PerfTest {

    @State(Scope.Benchmark)
    public static class SharedState {
        private Locks forseti;
        private Locks f2;

        @Setup
        public void setup() {
            this.forseti = new ForsetiLockManager(Config.defaults(), Clock.systemUTC(), NODE);
            this.f2 = new F2Locks(new ResourceType[]{NODE}, 64);
        }
    }

    @State(Scope.Thread)
    public static class LocalState {
        private Locks.Client forsetiClient;
        private Locks.Client f2Client;

        @Setup
        public void setup(SharedState shared) {
            this.forsetiClient = shared.forseti.newClient();
            this.f2Client = shared.f2.newClient();
        }
    }

    @Benchmark
    public void f2AcquireContendedShared(LocalState state) throws AcquireLockTimeoutException {
        state.f2Client.acquireShared(LockTracer.NONE, NODE, 0);
        state.f2Client.releaseShared(NODE, 0);
    }

    @Benchmark
    public void forsetiAcquireContendedShared(LocalState state) throws AcquireLockTimeoutException {
        state.forsetiClient.acquireShared(LockTracer.NONE, NODE, 0);
        state.forsetiClient.releaseShared(NODE, 0);
    }

    @Benchmark
    public void f2AcquireContendedExclusive(LocalState state) throws AcquireLockTimeoutException {
        state.f2Client.acquireExclusive(LockTracer.NONE, NODE, 0);
        state.f2Client.releaseExclusive(NODE, 0);
    }

    @Benchmark
    public void forsetiAcquireContendedExclusive(LocalState state) throws AcquireLockTimeoutException {
        for(;;) {
            try {
                state.forsetiClient.acquireExclusive(LockTracer.NONE, NODE, 0);
                state.forsetiClient.releaseExclusive(NODE, 0);
                return;
            } catch (DeadlockDetectedException e) {
                // This use case can't deadlock, but Forseti finds false positives.
                // Correct for this by forcing Forseti to keep going until it actually acquires and releases the
                // lock once.
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