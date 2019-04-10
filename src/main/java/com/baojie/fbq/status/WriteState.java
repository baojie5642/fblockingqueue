package com.baojie.fbq.status;

public enum WriteState {

    SUC((byte) 1),

    FAIL((byte) 2),

    FULL((byte) 3);

    private final byte value;

    WriteState(final byte value) {
        this.value = value;
    }

    public final byte value() {
        return value;
    }

    @Override
    public final String toString() {
        return "WriteState{" +
                "value=" + value +
                '}';
    }

}
