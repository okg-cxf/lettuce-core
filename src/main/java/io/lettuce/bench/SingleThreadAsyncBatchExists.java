package io.lettuce.bench;

import io.lettuce.bench.base.AbstractSingleThreadAsync;
import io.lettuce.core.RedisCommandBuilder;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.RedisCommand;
import io.netty.util.internal.logging.InternalLoggerFactory;

/**
 * @author chenxiaofan
 */
public class SingleThreadAsyncBatchExists extends AbstractSingleThreadAsync<Long> {

    static {
        logger = InternalLoggerFactory.getInstance(SingleThreadAsyncBatchExists.class);
    }

    private final RedisCommandBuilder<byte[], byte[]> builder = new RedisCommandBuilder<>(ByteArrayCodec.INSTANCE);

    @Override
    protected RedisCommand<byte[], byte[], ?> buildCommand(byte[] key, byte[] value) {
        return builder.exists(key);
    }

    @Override
    protected void assertResult(byte[] key, byte[] value, Long result) {
        LettuceAssert.assertState(result == 1L, "result not 1: " + result);
    }

    public static void main(String[] args) {
        runBatchTest(new SingleThreadAsyncBatchExists(), args);
    }

}
