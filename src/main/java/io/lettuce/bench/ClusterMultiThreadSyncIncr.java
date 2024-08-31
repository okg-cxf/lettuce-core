package io.lettuce.bench;

import io.lettuce.bench.base.AbstractClusterMultiThreadSync;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class ClusterMultiThreadSyncIncr extends AbstractClusterMultiThreadSync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(ClusterMultiThreadSyncIncr.class);
    }

    @Override
    protected Long doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] ignoredValueBytes) {
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
        runTest(new ClusterMultiThreadSyncIncr(), args);
    }

}
