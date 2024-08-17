package io.lettuce.bench;

import java.util.Arrays;

import io.lettuce.bench.base.AbstractSingleThreadAsync;
import io.lettuce.core.RedisCommandBuilder;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncBatchGet extends AbstractSingleThreadAsync<byte[]> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncBatchGet.class);
    }

    private final RedisCommandBuilder<byte[], byte[]> builder = new RedisCommandBuilder<>(ByteArrayCodec.INSTANCE);

    @Override
    protected RedisCommand<byte[], byte[], ?> buildCommand(byte[] key, byte[] value) {
        return builder.get(key);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, byte[] result) {
        LettuceAssert.assertState(Arrays.equals(value, result), String.format("value not match, exp: '%s', got: '%s'",
                new String(value), result == null ? "null" : new String(result)));
    }

    public static void main(String[] args) {
        runBatchTest(new SingleThreadAsyncBatchGet(), args);
    }

}
