package io.lettuce.bench.base;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.bench.utils.BenchUtils;
import io.lettuce.core.AutoBatchFlushOptions;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.DefaultAutoBatchFlushEndpoint;
import io.netty.util.internal.logging.InternalLogger;

import static io.lettuce.bench.base.Common.DIGIT_NUM;
import static io.lettuce.bench.base.Common.KEY_FORMATTER;
import static io.lettuce.bench.base.Common.VALUE_FORMATTER;

/**
 * @author chenxiaofan
 */
public abstract class AbstractMultiThreadSync<T> {

    protected static InternalLogger logger;

    private void test(boolean autoBatchFlush, Args args) {
        final int loopNum = args.loopNum;
        final int threadCount = args.threadCount;
        final int batchSize = args.batchSize;
        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(loopNum).length() + 1, "digit num is not large enough");

        DefaultAutoBatchFlushEndpoint.FLUSHED_COMMAND_COUNT.set(0L);
        DefaultAutoBatchFlushEndpoint.FLUSHED_BATCH_COUNT.set(0L);
        try (RedisClient redisClient = RedisClient
                .create(RedisURI.Builder.redis(args.host, args.port).withSsl(args.useSsl).build())) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            if (autoBatchFlush) {
                optsBuilder.autoBatchFlushOptions(AutoBatchFlushOptions.builder().enableAutoBatchFlush(true)
                        .batchSize(batchSize).writeSpinCount(Integer.MAX_VALUE).build());
            }
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final Thread[] threads = new Thread[threadCount];
            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatencyInMicros = new AtomicLong();
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < loopNum; j++) {
                        final byte[] keyBytes = usesSameKey() ? "key".getBytes() : genKey(j);
                        final byte[] valueBytes = genValue(j);
                        final long cmdStart = System.nanoTime();
                        final T result = doSync(connection.sync(), keyBytes, valueBytes);
                        totalLatencyInMicros.addAndGet((System.nanoTime() - cmdStart) / 1000);
                        assertResult(keyBytes, valueBytes, result);
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
            logger.info("Avg latency: {}ms", totalLatencyInMicros.get() / (double) totalCount.get() / 1000.0);
            logger.info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
            BenchUtils.logEnterRatioIfNeeded(logger);
            BenchUtils.logAvgBatchCount(logger);
        }
    }

    protected boolean usesSameKey() {
        return false;
    }

    protected abstract T doSync(RedisCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes);

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

        final int batchSize;

        final String host;

        final int port;

        final boolean useSsl;

        public Args(int threadCount, int loopNum, int batchSize, String host, int port, boolean useSsl) {
            this.threadCount = threadCount;
            this.loopNum = loopNum;
            this.batchSize = batchSize;
            this.host = host;
            this.port = port;
            this.useSsl = useSsl;
        }

    }

    private static Args parseArgs(String[] args) {
        int threadCount = 0;
        int loopNum = 0;
        int batchSize = 0;
        String host = "127.0.0.1";
        int port = 6379;
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
                case "-b":
                    i++;
                    batchSize = Integer.parseInt(args[i]);
                    break;
                case "-h":
                    i++;
                    host = args[i];
                    break;
                case "--ssl":
                    useSsl = true;
                    break;
                case "-p":
                    i++;
                    port = Integer.parseInt(args[i]);
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
        if (batchSize == 0) {
            throw new IllegalArgumentException("batch size must be specified");
        }
        logger.info("thread count: {}", threadCount);
        logger.info("loop num: {}", loopNum);
        logger.info("batch size: {}", batchSize);
        logger.info("use ssl: {}", useSsl);
        return new Args(threadCount, loopNum, batchSize, host, port, useSsl);
    }

    protected static <T> void runTest(AbstractMultiThreadSync<T> abstractClusterMultiThreadSync, String[] arguments) {
        final Args args = parseArgs(arguments);
        for (boolean autoBatchFlush : new boolean[] { true, false }) {
            logger.info("=====================================");
            logger.info("autoBatchFlush: {}", autoBatchFlush);
            abstractClusterMultiThreadSync.test(autoBatchFlush, args);
        }
        logger.info("=====================================");
    }

}
