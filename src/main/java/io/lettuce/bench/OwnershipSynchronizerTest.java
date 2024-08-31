package io.lettuce.bench;

import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.DefaultAutoBatchFlushEndpoint;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import io.netty.util.concurrent.EventExecutor;

public class OwnershipSynchronizerTest {

    private static final int THREADS = 30;

    private static final int ITERATIONS = 10000;

    private static final int RUNS = 40;

    private static final int EXPECT_RESULT = THREADS * ITERATIONS * RUNS;

    private static final int NUM_PREEMPTS = 1000;

    public static void main(String[] args) {
        final ClientResources clientResources = DefaultClientResources.builder().build();
        for (int i = 0; i < 1000; i++) {
            test(clientResources);
        }
        clientResources.shutdown();
    }

    public static void test(ClientResources clientResources) {
        final DefaultAutoBatchFlushEndpoint.OwnershipSynchronizer ownershipSynchronizer = new DefaultAutoBatchFlushEndpoint.OwnershipSynchronizer(
                clientResources.eventExecutorGroup().next(), Thread.currentThread().getName(), true);

        final IntWrapper counter = new IntWrapper(0);
        Thread[] threads = new Thread[THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < ITERATIONS; j++) {
                    ownershipSynchronizer.execute(() -> {
                        for (int k = 0; k < RUNS; k++) {
                            counter.increment();
                        }
                    });
                }
            });
            threads[i].start();
        }

        for (int i = 0; i < NUM_PREEMPTS; i++) {
            final EventExecutor eventExecutor = clientResources.eventExecutorGroup().next();
            eventExecutor.execute(() -> {
                ownershipSynchronizer.preempt(eventExecutor, Thread.currentThread().getName(), false);
                eventExecutor.schedule(() -> ownershipSynchronizer.done(1), 3, java.util.concurrent.TimeUnit.MILLISECONDS);
            });
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final EventExecutor eventExecutor = clientResources.eventExecutorGroup().next();
            try {
                eventExecutor.submit(() -> {
                }).get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("counter: " + counter.getValue());
        System.out.println("expected: " + EXPECT_RESULT);
        LettuceAssert.assertState(counter.getValue() == EXPECT_RESULT, "failed");
    }

    public static class IntWrapper {

        private int value;

        public IntWrapper(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public void increment() {
            value++;
        }

    }

}
