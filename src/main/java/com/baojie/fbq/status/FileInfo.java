package com.baojie.fbq.status;

public enum FileInfo {

    MAGIC("FQueuefs"),

    INDEX_NAME("fq.idx"),

    PREFIX("fq_"),

    SUFFIX(".db");

    private final String value;

    FileInfo(final String value) {
        this.value = value;
    }

    public final String value() {
        return value;
    }

    @Override
    public final String toString() {
        return "FileInfo{" +
                "value='" + value + '\'' +
                '}';
    }

}
