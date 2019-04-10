package com.baojie.fbq.status;

public enum Position {

    START(20),

    INDEX_LENGTH(32);

    private final int value;

    Position(final int value) {
        this.value = value;
    }

    public final int value() {
        return value;
    }

    @Override
    public String toString() {
        return "Position{" +
                "value=" + value +
                '}';
    }

}
