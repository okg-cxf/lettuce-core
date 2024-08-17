package io.lettuce.bench;

import java.util.Arrays;

import io.lettuce.bench.base.AbstractMultiThreadSync;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.internal.LettuceAssert;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class MultiThreadSyncGet extends AbstractMultiThreadSync<byte[]> {

    static {
        logger = InternalLoggerFactory.getInstance(MultiThreadSyncGet.class);
    }

    @Override
    protected byte[] doSync(RedisCommands<byte[], byte[]> sync, byte[] keyBytes, byte[] ignoredValueBytes) {
        return sync.get(keyBytes);
    }

    @Override
    protected void assertResult(byte[] keyBytes, byte[] expValueBytes, byte[] result) {
        LettuceAssert.assertState(Arrays.equals(expValueBytes, result),
                String.format("value not equals, exp: %s got: %s", new String(expValueBytes), new String(result)));
    }

    public static void main(String[] args) {
        runTest(new MultiThreadSyncGet(), args);
    }

}
