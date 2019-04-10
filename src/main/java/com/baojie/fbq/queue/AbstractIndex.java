package com.baojie.fbq.queue;


import com.baojie.fbq.status.FileInfo;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AbstractIndex {

    protected volatile String magic;
    protected volatile int version = -1;
    protected volatile int readerPosition = -1;
    protected volatile int writerPosition = -1;
    protected volatile int readerIndex = -1;
    protected volatile int writerIndex = -1;

    protected final AtomicInteger size = new AtomicInteger();

    protected AbstractIndex(){

    }

    protected final String check(final String path) {
        if (null == path) {
            throw new NullPointerException();
        }
        final String tmp = path.trim();
        if (tmp.length() <= 0) {
            throw new IllegalArgumentException();
        }
        return tmp;
    }

    protected final File info(String path) {
        return new File(path, FileInfo.INDEX_NAME.value());
    }


    protected final RandomAccessFile raf(final File info, final String mode) throws FileNotFoundException {
        return new RandomAccessFile(info, mode);
    }
    protected int size() {
        return size.get();
    }


















    public String getMagic() {
        return magic;
    }

    public void setMagic(String magic) {
        this.magic = magic;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getReaderPosition() {
        return readerPosition;
    }

    public void setReaderPosition(int readerPosition) {
        this.readerPosition = readerPosition;
    }

    public int getWriterPosition() {
        return writerPosition;
    }

    public void setWriterPosition(int writerPosition) {
        this.writerPosition = writerPosition;
    }

    public int getReaderIndex() {
        return readerIndex;
    }

    public void setReaderIndex(int readerIndex) {
        this.readerIndex = readerIndex;
    }

    public int getWriterIndex() {
        return writerIndex;
    }

    public void setWriterIndex(int writerIndex) {
        this.writerIndex = writerIndex;
    }


    protected String headerInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append(" magicString:");
        sb.append(magic);
        sb.append(" version:");
        sb.append(version);
        sb.append(" readerPosition:");
        sb.append(readerPosition);
        sb.append(" writerPosition:");
        sb.append(writerPosition);
        sb.append(" size:");
        sb.append(size);
        sb.append(" readerIndex:");
        sb.append(readerIndex);
        sb.append(" writerIndex:");
        sb.append(writerIndex);
        return sb.toString();
    }

}
