package com.jakewins.f2;

import com.jakewins.f2.include.AcquireLockTimeoutException;
import com.jakewins.f2.include.Locks;
import com.jakewins.f2.include.ResourceType;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

import static com.jakewins.f2.DeadlockDetector_Test.NODE;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class F2Locks_PerfTest {

    @State(Scope.Benchmark)
    public static class SharedState {
        private Locks locks;

        @Setup
        public void setup() {
            this.locks = new F2Locks(new ResourceType[]{NODE}, 64);
        }
    }

    @State(Scope.Thread)
    public static class LocalState {
        private Locks.Client client;

        @Setup
        public void setup(SharedState shared) {
            this.client = shared.locks.newClient();
        }
    }

    @Benchmark
    public void testAcquireContendedShared(LocalState state) throws AcquireLockTimeoutException {
        state.client.acquireShared(NODE, 0);
        state.client.releaseShared(NODE, 0);
    }
//
//    @Benchmark
//    public void testAcquireContendedExclusive(LocalState state) throws AcquireLockTimeoutException {
//        state.client.acquireExclusive(NODE, 0);
//        state.client.acquireExclusive(NODE, 0);
//    }

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