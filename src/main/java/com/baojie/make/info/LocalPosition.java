package com.baojie.make.info;

public enum  LocalPosition {

    INDEX_SIZE(32),

    READ_NUM_OFFSET(8),
    READ_POS_OFFSET(12),
    READ_CNT_OFFSET(16),

    WRITE_NUM_OFFSET(20),
    WRITE_POS_OFFSET(24),
    WRITE_CNT_OFFSET(28);

    private final int value;

    LocalPosition(final int value){
        this.value=value;
    }

    public final int value(){
        return value;
    }

    @Override
    public String toString() {
        return "LocalPosition{" +
                "value=" + value +
                '}';
    }

}
