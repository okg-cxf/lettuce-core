package io.lettuce.bench;

import io.lettuce.bench.base.AbstractClusterMultiThreadSync;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class ClusterMultiThreadSyncExists extends AbstractClusterMultiThreadSync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(ClusterMultiThreadSyncExists.class);
    }

    @Override
    protected Long doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes) {
        return sync.exists(keyBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, Long result) {
        LettuceAssert.assertState(result == 1L, "key not exists");
    }

    public static void main(String[] args) {
        runTest(new ClusterMultiThreadSyncExists(), args);
    }

}
