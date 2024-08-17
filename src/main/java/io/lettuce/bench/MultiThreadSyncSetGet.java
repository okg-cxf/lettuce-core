package io.lettuce.bench;

import java.util.Arrays;

import io.lettuce.bench.base.AbstractMultiThreadSync;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncSetGet extends AbstractMultiThreadSync<byte[]> {

    static {
        logger = InternalLoggerFactory.getInstance(MultiThreadSyncSetGet.class);
    }

    @Override
    protected byte[] doSync(RedisAdvancedClusterCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] valueBytes) {
        sync.set(keyBytes, valueBytes);
        return sync.get(keyBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, byte[] result) {
        LettuceAssert.assertState(Arrays.equals(expValueBytes, result),
                String.format("value not equals, exp: %s got: %s", new String(expValueBytes), new String(result)));
    }

    public static void main(String[] args) {
        runTest(new MultiThreadSyncSetGet(), args);
    }

}
