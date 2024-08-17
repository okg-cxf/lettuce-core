package io.lettuce.bench;

import io.lettuce.bench.base.AbstractMultiThreadSync;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncIncr extends AbstractMultiThreadSync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(MultiThreadSyncIncr.class);
    }

    @Override
    protected Long doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] ignoredValueBytes) {
        return sync.incr(keyBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, Long result) {
        LettuceAssert.assertState(expValueBytes != null, "expValueBytes is null");
    }

    @Override
    protected boolean usesSameKey() {
        return false;
    }

    public static void main(String[] args) {
        runTest(new MultiThreadSyncIncr(), args);
    }

}
