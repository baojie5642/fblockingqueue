package com.baojie.make.info;

public enum RafState {

    O_RDONLY("r"),

    O_RDWR("rw"),

    O_SYNC("rws"),

    O_DSYNC("rwd");

    private final String value;

    RafState(final String value) {
        this.value = value;
    }

    public final String value() {
        return value;
    }

    @Override
    public final String toString() {
        return "RafState{" +
                "value='" + value + '\'' +
                '}';
    }

}
