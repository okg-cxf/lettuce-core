package io.lettuce.bench.base;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLogger;

import static io.lettuce.bench.base.Common.DIGIT_NUM;
import static io.lettuce.bench.base.Common.KEY_FORMATTER;
import static io.lettuce.bench.base.Common.VALUE_FORMATTER;

/**
 * @author chenxiaofan
 */
public abstract class AbstractMultiThreadSync<T> {

    protected static InternalLogger logger;

    private void test(Args args) {
        final int loopNum = args.loopNum;
        final int threadCount = args.threadCount;
        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(loopNum).length() + 1, "digit num is not large enough");

        try (RedisClusterClient redisClient = RedisClusterClient.create(
                RedisURI.Builder.redis(args.host).withSsl(args.useSsl).build())) {
            final ClusterClientOptions.Builder optsBuilder = ClusterClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build())
                    .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder().enablePeriodicRefresh(true)
                            .enableAllAdaptiveRefreshTriggers().refreshPeriod(Duration.ofSeconds(6)).build());
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisClusterConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final Thread[] threads = new Thread[threadCount];
            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < loopNum; j++) {
                        final byte[] keyBytes = usesSameKey() ? "key".getBytes() : genKey(j);
                        final byte[] valueBytes = genValue(j);
                        final long cmdStart = System.nanoTime();
                        try {
                            final T result = doSync(connection.sync(), keyBytes, valueBytes);
                            totalLatency.addAndGet((System.nanoTime() - cmdStart) / 1000);
                            assertResult(keyBytes, valueBytes, result);
                        } catch (Exception e) {
                            logger.warn("doSync failed for key {}: {}", new String(keyBytes), e.getMessage());
                        }
                        totalCount.incrementAndGet();
                    }
                });
            }
            final long start = System.nanoTime();
            Arrays.asList(threads).forEach(Thread::start);
            Arrays.asList(threads).forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(e);
                }
            });
            double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
            logger.info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
            logger.info("Total time: {}s", costInSeconds);
            logger.info("Avg latency: {}us", totalLatency.get() / (double) totalCount.get());
            logger.info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
        }
    }

    protected boolean usesSameKey() {
        return false;
    }

    protected abstract T doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes);

    protected abstract void assertResult(byte[] keyBytes, byte[] expValueBytes, T result);

    private byte[] genKey(int j) {
        return String.format(KEY_FORMATTER, j).getBytes();
    }

    private byte[] genValue(int j) {
        return String.format(VALUE_FORMATTER, j).getBytes();
    }

    private static class Args {

        final int threadCount;

        final int loopNum;

        final String host;

        final boolean useSsl;

        public Args(int threadCount, int loopNum, String host, boolean useSsl) {
            this.threadCount = threadCount;
            this.loopNum = loopNum;
            this.host = host;
            this.useSsl = useSsl;
        }

    }

    private static Args parseArgs(String[] args) {
        int threadCount = 0;
        int loopNum = 0;
        String host = "127.0.0.1";
        int i = 0;
        boolean useSsl = false;
        while (i < args.length) {
            switch (args[i]) {
                case "-t":
                    i++;
                    threadCount = Integer.parseInt(args[i]);
                    break;
                case "-n":
                    i++;
                    loopNum = Integer.parseInt(args[i]);
                    break;
                case "-h":
                    i++;
                    host = args[i];
                    break;
                case "--ssl":
                    useSsl = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + args[i]);
            }
            i++;
        }
        if (threadCount == 0) {
            throw new IllegalArgumentException("thread count must be specified");
        }
        if (loopNum == 0) {
            throw new IllegalArgumentException("loop num must be specified");
        }
        logger.info("thread count: {}", threadCount);
        logger.info("loop num: {}", loopNum);
        logger.info("use ssl: {}", useSsl);
        return new Args(threadCount, loopNum, host, useSsl);
    }

    protected static <T> void runTest(AbstractMultiThreadSync<T> abstractMultiThreadSync, String[] parameters) {
        final Args args = parseArgs(parameters);
        logger.info("=====================================");
        abstractMultiThreadSync.test(args);
        logger.info("=====================================");
    }

}
