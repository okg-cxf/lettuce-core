package io.lettuce.bench.base;

class Common {

    private Common() {
    }

    static final int DIGIT_NUM = 9;

    static final String KEY_FORMATTER = String.format("key-%%0%dd", DIGIT_NUM);

    static final String VALUE_FORMATTER = String.format("value-%%0%dd", DIGIT_NUM);

}
