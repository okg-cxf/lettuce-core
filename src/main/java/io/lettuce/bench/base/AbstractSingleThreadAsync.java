package io.lettuce.bench.base;

import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.AsyncCommand;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.util.internal.logging.InternalLogger;

import static io.lettuce.bench.base.Common.DIGIT_NUM;
import static io.lettuce.bench.base.Common.KEY_FORMATTER;
import static io.lettuce.bench.base.Common.VALUE_FORMATTER;

/**
 * @author chenxiaofan
 */
@SuppressWarnings({ "BusyWait" })
public abstract class AbstractSingleThreadAsync<T> {

    protected static InternalLogger logger;

    private static InternalLogger getLogger() {
        return logger;
    }

    String prevKey = "";

    private byte[] genKey(int j) {
        return String.format(KEY_FORMATTER, j).getBytes();
    }

    private byte[] genValue(int j) {
        return String.format(VALUE_FORMATTER, j).getBytes();
    }

    protected RedisFuture<T> doAsyncCommand(StatefulRedisConnection<byte[], byte[]> async, byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    private Collection<RedisCommand<byte[], byte[], ?>> doBatchAsync(StatefulRedisConnection<byte[], byte[]> conn,
            Collection<byte[][]> keyValues) {

        List<RedisCommand<byte[], byte[], ?>> asyncCommands = new ArrayList<>();
        for (byte[][] kv : keyValues) {
            RedisCommand<byte[], byte[], ?> command = buildCommand(kv[0], kv[1]);
            RedisCommand<byte[], byte[], ?> asyncCommand = new AsyncCommand<>(command);
            asyncCommands.add(asyncCommand);
        }
        return conn.dispatch(asyncCommands);
    }

    protected RedisCommand<byte[], byte[], ?> buildCommand(byte[] key, byte[] value) {
        throw new UnsupportedOperationException();
    }

    protected abstract void assertResult(byte[] key, byte[] value, T result);

    private void test(Args args) {
        final int loopNum = args.loopNum;
        final String host = args.host;

        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(loopNum).length() + 1, "digit num is not large enough");
        try (RedisClient redisClient = RedisClient.create(RedisURI.Builder.redis(host).withSsl(args.useSsl).build())) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();

            final long start = System.nanoTime();
            for (int j = 0; j < loopNum; j++) {
                final byte[] keyBytes = genKey(j);
                final byte[] valueBytes = genValue(j);
                final long cmdStart = System.nanoTime();
                final RedisFuture<T> resultFut = doAsyncCommand(connection, keyBytes, valueBytes);
                resultFut.whenComplete((result, throwable) -> onComplete(totalCount, totalLatency, cmdStart, throwable));
            }
            while (totalCount.get() != loopNum) {
                Thread.sleep(1);
            }
            double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
            getLogger().info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
            getLogger().info("Total time: {}s", costInSeconds);
            getLogger().info("Avg latency: {}s", totalLatency.get() / (double) totalCount.get() / 1000.0 / 1000.0);
            getLogger().info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void testBatch(Args args) {
        final int loopNum = args.loopNum;
        final int batchSize = args.batchSize;
        final String host = args.host;

        LettuceAssert.assertState(DIGIT_NUM >= String.valueOf(loopNum).length() + 1, "digit num is not large enough");
        try (RedisClient redisClient = RedisClient.create(RedisURI.Builder.redis(host).withSsl(args.useSsl).build())) {
            final ClientOptions.Builder optsBuilder = ClientOptions.builder()
                    .timeoutOptions(TimeoutOptions.builder().fixedTimeout(Duration.ofSeconds(7200)).build());
            redisClient.setOptions(optsBuilder.build());
            final StatefulRedisConnection<byte[], byte[]> connection = redisClient.connect(ByteArrayCodec.INSTANCE);

            final AtomicLong totalCount = new AtomicLong();
            final AtomicLong totalLatency = new AtomicLong();

            final long start = System.nanoTime();
            int j = 0;
            final List<byte[][]> batch = new ArrayList<>();
            while (j < loopNum) {
                for (int i = 0; i < batchSize && j < loopNum; i++, j++) {
                    batch.add(new byte[][] { genKey(j), genValue(j) });
                }
                final long cmdStart = System.nanoTime();
                final Collection<RedisCommand<byte[], byte[], ?>> commands = doBatchAsync(connection, batch);
                commands.forEach(command -> {
                    final AsyncCommand<byte[], byte[], ?> asyncCommand = (AsyncCommand<byte[], byte[], ?>) command;
                    asyncCommand.whenComplete((result, throwable) -> onComplete(totalCount, totalLatency, cmdStart, throwable));
                });
                batch.clear();
            }
            while (totalCount.get() != loopNum) {
                Thread.sleep(1);
            }
            double costInSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
            getLogger().info("Total commands: {}", NumberFormat.getInstance().format(totalCount.get()));
            getLogger().info("Total time: {}s", costInSeconds);
            getLogger().info("Avg latency: {}s", totalLatency.get() / (double) totalCount.get() / 1000.0 / 1000.0);
            getLogger().info("Avg QPS: {}/s", totalCount.get() / costInSeconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    private void onComplete(AtomicLong totalCount, AtomicLong totalLatency, long cmdStart, Throwable throwable) {
        try {
            if (throwable != null) {
                getLogger().error("async#get failed: err: {}", throwable.getMessage(), throwable);
            }
            // assertResult(keyBytes, valueBytes, result);
            totalCount.incrementAndGet();
            totalLatency.addAndGet((System.nanoTime() - cmdStart) / 1000);

            // LettuceAssert.assertState(key.compareTo(prevKey) > 0,
            // String.format("not in order, prevKey: %s, key: %s", prevKey, key));
            // prevKey = key;
        } catch (Exception e) {
            getLogger().error("async#get failed: err: {}", e.getMessage(), e);
        }
    }

    private static class Args {

        final int loopNum;

        final int batchSize;

        final String host;

        final boolean useSsl;

        public Args(int loopNum, int batchSize, String host, boolean useSsl) {
            this.loopNum = loopNum;
            this.batchSize = batchSize;
            this.host = host;
            this.useSsl = useSsl;
        }

    }

    private static Args parseArgs(String[] args) {
        int loopNum = 0;
        int batchSize = 0;
        String host = "127.0.0.1";
        boolean useSsl = false;

        int i = 0;
        while (i < args.length) {
            switch (args[i]) {
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
                default:
                    throw new IllegalArgumentException("unknown option: " + args[i]);
            }
            i++;
        }
        if (loopNum == 0) {
            throw new IllegalArgumentException("loop num must be specified");
        }
        if (batchSize == 0) {
            throw new IllegalArgumentException("batch size must be specified");
        }
        logger.info("loop num: {}", loopNum);
        logger.info("batch size: {}", batchSize);
        return new Args(loopNum, batchSize, host, useSsl);
    }

    protected static <T> void runTest(AbstractSingleThreadAsync<T> async, String[] arguments) {
        final Args args = parseArgs(arguments);
        logger.info("=====================================");
        async.test(args);
        logger.info("=====================================");
    }

    protected static <T> void runBatchTest(AbstractSingleThreadAsync<T> async, String[] arguments) {
        final Args args = parseArgs(arguments);
        logger.info("=====================================");
        async.testBatch(args);
        logger.info("=====================================");
    }

}
