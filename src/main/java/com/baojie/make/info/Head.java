package com.baojie.make.info;

public enum  Head {

    MAGIC("Baojie00"),

    INDEX_NAME("baojie.idx");

    private final String value;

    Head(final String value){
        this.value=value;
    }

    public final String value(){
        return value;
    }

    @Override
    public String toString() {
        return "Head{" +
                "value='" + value + '\'' +
                '}';
    }

}
