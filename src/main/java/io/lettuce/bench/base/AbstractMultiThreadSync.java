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

    private void test(Common.TestParameter para, Args args, boolean printResult) {
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
            if (para.useAutoBatch) {
                optsBuilder.autoBatchFlushOptions(AutoBatchFlushOptions.builder().enableAutoBatchFlush(true)
                        .batchSize(batchSize).useConsolidateFlush(para.useConsolidateFlush)
                        .consolidateFlushWhenNoReadInProgress(para.consolidateFlushWhenNoReadInProgress)
                        .writeSpinCount(args.writeSpinCount).build());
            }
            redisClient.setOptions(optsBuilder.build());
            @SuppressWarnings("unchecked")
            final StatefulRedisConnection<byte[], byte[]>[] connections = new StatefulRedisConnection[threadCount];
            if (args.multiConnection) {
                for (int i = 0; i < connections.length; i++) {
                    connections[i] = redisClient.connect(ByteArrayCodec.INSTANCE);
                }
            } else {
                final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);
                Arrays.fill(connections, connection);
            }

            final Thread[] threads = new Thread[threadCount];
            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatencyInMicros = new AtomicLong();
            for (int i = 0; i < threadCount; i++) {
                final int finalI = i;
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < loopNum; j++) {
                        final byte[] keyBytes = usesSameKey() ? "key".getBytes() : genKey(j);
                        final byte[] valueBytes = genValue(j);
                        final long cmdStart = System.nanoTime();
                        final T result = doSync(connections[finalI].sync(), keyBytes, valueBytes);
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
            if (printResult) {
                double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
                logger.info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
                logger.info("Total time: {}s", costInSeconds);
                logger.info("Avg latency: {}ms", totalLatencyInMicros.get() / (double) totalCount.get() / 1000.0);
                logger.info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
                BenchUtils.logEnterRatioIfNeeded(logger);
                BenchUtils.logAvgBatchCount(logger);
            }
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

        final int writeSpinCount;

        final String host;

        final int port;

        final boolean useSsl;

        final boolean multiConnection;

        public Args(int threadCount, int loopNum, int batchSize, int writeSpinCount, String host, int port, boolean useSsl,
                boolean multiConnection) {
            this.threadCount = threadCount;
            this.loopNum = loopNum;
            this.batchSize = batchSize;
            this.writeSpinCount = writeSpinCount;
            this.host = host;
            this.port = port;
            this.useSsl = useSsl;
            this.multiConnection = multiConnection;
        }

    }

    private static Args parseArgs(String[] args) {
        int threadCount = 0;
        int loopNum = 0;
        int batchSize = AutoBatchFlushOptions.DEFAULT_BATCH_SIZE;
        int writeSpinCount = AutoBatchFlushOptions.DEFAULT_WRITE_SPIN_COUNT;
        String host = "127.0.0.1";
        int port = 6379;
        int i = 0;
        boolean useSsl = false;
        boolean multiConnection = false;
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
                case "-s":
                    i++;
                    writeSpinCount = Integer.parseInt(args[i]);
                    break;
                case "--multi-connection":
                    multiConnection = true;
                    break;
                default:
                    throw new IllegalArgumentException("unknown option: " + args[i]);
            }
            i++;
        }
        if (threadCount <= 0) {
            throw new IllegalArgumentException("thread count must be specified");
        }
        if (loopNum <= 0) {
            throw new IllegalArgumentException("loop num must be specified");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batch size must be specified");
        }
        logger.info("thread count: {}", threadCount);
        logger.info("loop num: {}", loopNum);
        logger.info("batch size: {}", batchSize);
        logger.info("write spin count: {}", writeSpinCount);
        logger.info("use ssl: {}", useSsl);
        if (multiConnection) {
            logger.info("multi connection: enabled");
        }
        return new Args(threadCount, loopNum, batchSize, writeSpinCount, host, port, useSsl, multiConnection);
    }

    protected static <T> void runTest(AbstractMultiThreadSync<T> abstractClusterMultiThreadSync, String[] arguments) {
        final Args args = parseArgs(arguments);
        Common.TestParameter[] array = Arrays.copyOf(Common.TestParameter.values(), Common.TestParameter.values().length);
        // Arrays.sort(array, (o1, o2) -> -(o1.ordinal() - o2.ordinal()));
        for (Common.TestParameter para : array) {
            logger.info("=====================================");
            logger.info("useAutoBatch: {}, useConsolidateFlush: {}, consolidateFlushWhenNoReadInProgress: {}",
                    para.useAutoBatch, para.useConsolidateFlush, para.consolidateFlushWhenNoReadInProgress);
            abstractClusterMultiThreadSync.test(para, args, false); // warm up
            abstractClusterMultiThreadSync.test(para, args, true);
        }
        logger.info("=====================================");
    }

}
