package io.lettuce.bench;

import io.lettuce.bench.base.AbstractMultiThreadSync;
import io.lettuce.core.api.sync.RedisCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncIncr extends AbstractMultiThreadSync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(MultiThreadSyncIncr.class);
    }

    @Override
    protected Long doSync(RedisCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] ignoredValueBytes) {
        return sync.incr(keyBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, Long result) {
        // do nothing
    }

    @Override
    protected boolean usesSameKey() {
        return true;
    }

    public static void main(String[] args) {
        runTest(new MultiThreadSyncIncr(), args);
    }

}
