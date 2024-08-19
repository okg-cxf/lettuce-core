package io.lettuce.bench.base;

class Common {

    private Common() {
    }

    static final int DIGIT_NUM = 9;

    static final String KEY_FORMATTER = String.format("key-%%0%dd", DIGIT_NUM);

    static final String VALUE_FORMATTER = String.format("value-%%0%dd", DIGIT_NUM);

    public enum TestParameter {

        AUTO_BATCH_WITHOUT_CONSOLIDATE_FLUSH(true, false, false), AUTO_BATCH_WITH_CONSOLIDATE_FLUSH_ALWAYS(true, true,
                true), AUTO_BATCH_WITH_CONSOLIDATE_FLUSH_ONLY_WHEN_READ_IN_PROGRESS(true, true, false),

        NO_AUTO_BATCH(false, false, false),;

        final boolean useAutoBatch;

        final boolean useConsolidateFlush;

        final boolean consolidateFlushWhenNoReadInProgress;

        TestParameter(boolean useAutoBatch, boolean useConsolidateFlush, boolean consolidateFlushWhenNoReadInProgress) {
            this.useAutoBatch = useAutoBatch;
            this.useConsolidateFlush = useConsolidateFlush;
            this.consolidateFlushWhenNoReadInProgress = consolidateFlushWhenNoReadInProgress;
        }

    }

}
