package com.jakewins.f2.infrastructure;

import org.junit.Test;
import org.neo4j.function.ThrowingAction;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class SingleWaiterLatch_Test {
    private List<Thread> runningThreads = new LinkedList<>();

    @Test
    public void shouldReleaseWaitingThread() throws InterruptedException {
        // Given
        AtomicBoolean didAcquire = new AtomicBoolean();
        SingleWaiterLatch latch = new SingleWaiterLatch();
        run(() -> didAcquire.set(latch.tryAcquire(5, TimeUnit.SECONDS)));

        // When
        run(latch::release);

        // Then
        allThreadsTerminate();
        assert didAcquire.get() : "Expected waiting thread to get latch.";
    }

    private void allThreadsTerminate() throws InterruptedException {
        for (Thread thread : runningThreads) {
            thread.join();
        }
    }

    void run(ThrowingAction<Exception> action) {
        Thread thread = new Thread(() -> {
            try {
                action.apply();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        runningThreads.add(thread);
        thread.start();
    }
}
