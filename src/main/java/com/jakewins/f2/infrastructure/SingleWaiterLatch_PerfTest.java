package com.jakewins.f2.infrastructure;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

@State(Scope.Group)
public class SingleWaiterLatch_PerfTest {

    private SingleWaiterLatch latchA;
    private SingleWaiterLatch latchB;

    private Semaphore semaphoreA;
    private Semaphore semaphoreB;

    @Setup(Level.Iteration)
    public void up() {
        latchA = new SingleWaiterLatch();
        latchA.release();
        latchB = new SingleWaiterLatch();
        latchB.release();
        semaphoreA = new Semaphore(1);
        semaphoreB = new Semaphore(1);
    }

    @Benchmark
    @Group("latch")
    @GroupThreads
    public void latchWorker1() throws InterruptedException {
        latchA.tryAcquire(1, TimeUnit.SECONDS);
        latchB.release();
    }

    @Benchmark
    @Group("latch")
    @GroupThreads
    public void latchWorker2() throws InterruptedException {
        latchB.tryAcquire(1, TimeUnit.SECONDS);
        latchA.release();
    }

    @Benchmark
    @Group("semaphore")
    @GroupThreads
    public void semaphoreWorker1() throws InterruptedException {
        semaphoreA.tryAcquire(1, TimeUnit.SECONDS);
        semaphoreB.release();
    }

    @Benchmark
    @Group("semaphore")
    @GroupThreads
    public void semaphoreWorker2() throws InterruptedException {
        semaphoreB.tryAcquire(1, TimeUnit.SECONDS);
        semaphoreA.release();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SingleWaiterLatch_PerfTest.class.getSimpleName())
                .warmupIterations(1)
                .measurementIterations(5)
                .threads(2)
                .forks(1)
                .syncIterations(true)
                .build();

        new Runner(opt).run();
    }
}
