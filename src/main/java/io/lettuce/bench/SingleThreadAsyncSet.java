package io.lettuce.bench;

import io.lettuce.bench.base.AbstractSingleThreadAsync;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncSet extends AbstractSingleThreadAsync<String> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncSet.class);
    }

    @Override
    protected RedisFuture<String> doAsyncCommand(StatefulRedisConnection<byte[], byte[]> conn, byte[] key, byte[] value) {
        return conn.async().set(key, value);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, String result) {
        // do nothing
    }

    public static void main(String[] args) {
        runTest(new SingleThreadAsyncSet(), args);
    }

}
