package io.lettuce.bench.utils;

import io.lettuce.core.context.AutoBatchFlushEndPointContext;
import io.lettuce.core.protocol.DefaultAutoBatchFlushEndpoint;
import io.netty.util.internal.logging.InternalLogger;

/**
 * @author chenxiaofan
 */
public class BenchUtils {

    private BenchUtils() {
    }

    public static void logEnterRatioIfNeeded(InternalLogger logger) {
        final long total = AutoBatchFlushEndPointContext.HasOngoingSendLoop.ENTERED_FREQUENCY.get()
                + AutoBatchFlushEndPointContext.HasOngoingSendLoop.ENTER_FAILED_FREQUENCY.get();
        if (total == 0L) {
            return;
        }
        logger.info("enter ratio: {}%oo",
                AutoBatchFlushEndPointContext.HasOngoingSendLoop.ENTERED_FREQUENCY.get() * 10000 / (double) total);

    }

    public static void logAvgBatchCount(InternalLogger logger) {
        final long flushedCommandCount = DefaultAutoBatchFlushEndpoint.FLUSHED_COMMAND_COUNT.get();
        final long flushedBatchCount = DefaultAutoBatchFlushEndpoint.FLUSHED_BATCH_COUNT.get();
        if (flushedBatchCount == 0L) {
            logger.info("no batch flushed");
            return;
        }
        logger.info("avg batch count: {}", flushedCommandCount / (double) flushedBatchCount);
    }

}
