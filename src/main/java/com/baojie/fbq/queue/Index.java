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
import com.baojie.fbq.status.FileInfo;
import com.baojie.fbq.status.Position;
import com.baojie.make.info.RafState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

/**
 * 数据索引文件
 */
public class Index extends AbstractIndex {

    private static final Logger log = LoggerFactory.getLogger(Index.class);

    private final FileChannel fc;
    private final RandomAccessFile raf;
    private final MappedByteBuffer mbf;

    /**
     * 文件操作位置信息
     */

    protected Index(String path) throws IOException, FileFormat {
        final String real = check(path);
        // 创建持久化的队列信息的文件
        final File info = info(real);
        // 文件不存在，创建文件
        if (info.exists() == false) {
            info.createNewFile();
            raf = raf(info, RafState.O_DSYNC.value());
            index();
        } else {
            raf = raf(info, RafState.O_DSYNC.value());
            if (raf.length() < Position.INDEX_LENGTH.value()) {
                throw new FileFormat("index length error");
            }
            byte[] bytes = new byte[Position.INDEX_LENGTH.value()];
            raf.read(bytes);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            bytes = new byte[Entity.MAGIC.getBytes().length];
            buffer.get(bytes);
            magic = new String(bytes);
            version = buffer.getInt();
            readerPosition = buffer.getInt();
            writerPosition = buffer.getInt();
            readerIndex = buffer.getInt();
            writerIndex = buffer.getInt();
            int sz = buffer.getInt();
            if (readerPosition == writerPosition && readerIndex == writerIndex && sz <= 0) {
                index();
            } else {
                size.set(sz);
            }
        }
        fc = raf.getChannel();
        mbf = fc.map(MapMode.READ_WRITE, 0, Position.INDEX_LENGTH.value());
    }



    // 记录文件的信息以及读取指针的位置还有版本,，总共32字节
    private final void index() throws IOException {
        magic = FileInfo.MAGIC.value();
        version = 1;
        readerPosition = Position.START.value();// 先将头部信息设置好，然后下面再设置头部信息
        writerPosition = Position.START.value();
        readerIndex = 1;
        writerIndex = 1;
        raf.setLength(32);
        raf.seek(0); // 设置起始读取位置
        raf.write(magic.getBytes());// magic
        raf.writeInt(version);// 8 version
        raf.writeInt(readerPosition);// 12 reader position
        raf.writeInt(writerPosition);// 16 write position
        raf.writeInt(readerIndex);// 20 reader index
        raf.writeInt(writerIndex);// 24 writer index
        raf.writeInt(0);// 28 size
    }

    protected void clear() throws IOException {
        mbf.clear();
        mbf.force();
        index();
    }

    /**
     * 记录写位置
     *
     * @param pos
     */
    protected void putWriterPosition(int pos) {
        mbf.position(16);
        mbf.putInt(pos);
        this.writerPosition = pos;
    }

    /**
     * 记录读取的位置
     *
     * @param pos
     */
    protected void putReaderPosition(int pos) {
        mbf.position(12);
        mbf.putInt(pos);
        this.readerPosition = pos;
    }

    /**
     * 记录写文件索引
     *
     * @param index
     */
    protected void putWriterIndex(int index) {
        mbf.position(24);
        mbf.putInt(index);
        this.writerIndex = index;
    }

    /**
     * 记录读取文件索引
     *
     * @param index
     */
    protected void putReaderIndex(int index) {
        mbf.position(20);
        mbf.putInt(index);
        this.readerIndex = index;
    }

    protected void incrementSize() {
        int num = size.incrementAndGet();
        mbf.position(28);
        mbf.putInt(num);
    }

    protected void decrementSize() {
        int num = size.decrementAndGet();
        mbf.position(28);
        mbf.putInt(num);
    }

    /**
     * 关闭索引文件
     */
    protected final void close() throws IOException {
        try {
            closeBuffer();
        } finally {
            closeChannel();
        }
    }

    private final void closeBuffer() {
        try {
            force();
        } finally {
            LocalCleaner.clean(mbf);
        }
    }

    private final void closeChannel() throws IOException {
        try {
            if (null != fc) {
                fc.close();
            }
        } finally {
            if (null != raf) {
                raf.close();
            }
        }
    }


    private final void force() {
        final MappedByteBuffer copy = this.mbf;
        if (null != copy) {
            copy.force();
        }
    }


}
