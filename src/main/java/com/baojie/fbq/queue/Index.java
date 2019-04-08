/*
 *  Copyright 2011 sunli [sunli1223@gmail.com][weibo.com@sunli1223]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.baojie.fbq.queue;


import com.baojie.fbq.clean.LocalCleaner;
import com.baojie.fbq.exception.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据索引文件
 */
public class Index {
    private static final Logger log = LoggerFactory.getLogger(Index.class);
    private static final int INDEX_LIMIT_LENGTH = 32;
    private static final String INDEX_FILE_NAME = "fq.idx";

    private RandomAccessFile dbRandFile;
    private FileChannel fc;
    private MappedByteBuffer mappedByteBuffer;

    /**
     * 文件操作位置信息
     */
    private volatile String magicString = null;
    private volatile int version = -1;
    private volatile int readerPosition = -1;
    private volatile int writerPosition = -1;
    private volatile int readerIndex = -1;
    private volatile int writerIndex = -1;
    private final AtomicInteger size = new AtomicInteger();

    protected Index(String path) throws IOException, FileFormat {
        File dbFile = new File(path, INDEX_FILE_NAME);

        // 文件不存在，创建文件
        if (dbFile.exists() == false) {
            dbFile.createNewFile();
            dbRandFile = new RandomAccessFile(dbFile, "rwd");
            initIdxFile();
        } else {
            dbRandFile = new RandomAccessFile(dbFile, "rwd");
            if (dbRandFile.length() < INDEX_LIMIT_LENGTH) {
                throw new FileFormat("file format error.");
            }
            byte[] bytes = new byte[INDEX_LIMIT_LENGTH];
            dbRandFile.read(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            bytes = new byte[Entity.MAGIC.getBytes().length];
            buffer.get(bytes);
            magicString = new String(bytes);
            version = buffer.getInt();
            readerPosition = buffer.getInt();
            writerPosition = buffer.getInt();
            readerIndex = buffer.getInt();
            writerIndex = buffer.getInt();
            int sz = buffer.getInt();
            if (readerPosition == writerPosition && readerIndex == writerIndex && sz <= 0) {
                initIdxFile();
            } else {
                size.set(sz);
            }
        }
        fc = dbRandFile.getChannel();
        mappedByteBuffer = fc.map(MapMode.READ_WRITE, 0, INDEX_LIMIT_LENGTH);
    }
    // 记录文件的信息以及读取指针的位置还有版本,，总共32字节
    private void initIdxFile() throws IOException {
        magicString = Entity.MAGIC;
        version = 1;
        readerPosition = Entity.MESSAGE_START_POSITION;// 先将头部信息设置好，然后下面再设置头部信息
        writerPosition = Entity.MESSAGE_START_POSITION;
        readerIndex = 1;
        writerIndex = 1;
        dbRandFile.setLength(32);
        dbRandFile.seek(0); // 设置起始读取位置
        dbRandFile.write(magicString.getBytes());// magic
        dbRandFile.writeInt(version);// 8 version
        dbRandFile.writeInt(readerPosition);// 12 reader position
        dbRandFile.writeInt(writerPosition);// 16 write position
        dbRandFile.writeInt(readerIndex);// 20 reader index
        dbRandFile.writeInt(writerIndex);// 24 writer index
        dbRandFile.writeInt(0);// 28 size
    }

    protected void clear() throws IOException {
        mappedByteBuffer.clear();
        mappedByteBuffer.force();
        initIdxFile();
    }

    /**
     * 记录写位置
     *
     * @param pos
     */
    protected void putWriterPosition(int pos) {
        mappedByteBuffer.position(16);
        mappedByteBuffer.putInt(pos);
        this.writerPosition = pos;
    }

    /**
     * 记录读取的位置
     *
     * @param pos
     */
    protected void putReaderPosition(int pos) {
        mappedByteBuffer.position(12);
        mappedByteBuffer.putInt(pos);
        this.readerPosition = pos;
    }

    /**
     * 记录写文件索引
     *
     * @param index
     */
    protected void putWriterIndex(int index) {
        mappedByteBuffer.position(24);
        mappedByteBuffer.putInt(index);
        this.writerIndex = index;
    }

    /**
     * 记录读取文件索引
     *
     * @param index
     */
    protected void putReaderIndex(int index) {
        mappedByteBuffer.position(20);
        mappedByteBuffer.putInt(index);
        this.readerIndex = index;
    }

    protected void incrementSize() {
        int num = size.incrementAndGet();
        mappedByteBuffer.position(28);
        mappedByteBuffer.putInt(num);
    }

    protected void decrementSize() {
        int num = size.decrementAndGet();
        mappedByteBuffer.position(28);
        mappedByteBuffer.putInt(num);
    }

    protected String getMagicString() {
        return magicString;
    }

    protected int getVersion() {
        return version;
    }

    protected int getReaderPosition() {
        return readerPosition;
    }

    protected int getWriterPosition() {
        return writerPosition;
    }

    protected int getReaderIndex() {
        return readerIndex;
    }

    protected int getWriterIndex() {
        return writerIndex;
    }

    protected int size() {
        return size.get();
    }

    /**
     * 关闭索引文件
     */
    protected void close() throws IOException {
        mappedByteBuffer.force();
        LocalCleaner.clean(mappedByteBuffer);
        fc.close();
        dbRandFile.close();
        mappedByteBuffer = null;
        fc = null;
        dbRandFile = null;
    }

    protected String headerInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append(" magicString:");
        sb.append(magicString);
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
