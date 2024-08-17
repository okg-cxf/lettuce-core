package io.lettuce.bench;

import io.lettuce.bench.base.AbstractClusterMultiThreadSync;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class ClusterMultiThreadSyncSet extends AbstractClusterMultiThreadSync<String> {

    static {
        logger = InternalLoggerFactory.getInstance(ClusterMultiThreadSyncSet.class);
    }

    @Override
    protected String doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes) {
        return sync.set(keyBytes, valueBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, String result) {
        // no need
    }

    public static void main(String[] args) {
        runTest(new ClusterMultiThreadSyncSet(), args);
    }

}
