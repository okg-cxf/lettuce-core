package io.lettuce.bench;

import io.lettuce.bench.base.AbstractMultiThreadSync;
import io.lettuce.core.api.sync.RedisCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncSet extends AbstractMultiThreadSync<String> {

    static {
        logger = InternalLoggerFactory.getInstance(MultiThreadSyncSet.class);
    }

    @Override
    protected String doSync(RedisCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes) {
        return sync.set(keyBytes, valueBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, String result) {
        // no need
    }

    public static void main(String[] args) {
        runTest(new MultiThreadSyncSet(), args);
    }

}
