package io.lettuce.bench;

import io.lettuce.bench.base.AbstractSingleThreadAsync;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncExists extends AbstractSingleThreadAsync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncExists.class);
    }

    @Override
    protected RedisFuture<Long> doAsyncCommand(StatefulRedisConnection<byte[], byte[]> conn, byte[] key, byte[] ignoredValue) {
        return conn.async().exists(key);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, Long result) {
        // do nothing
    }

    public static void main(String[] args) {
        runTest(new SingleThreadAsyncExists(), args);
    }

}
