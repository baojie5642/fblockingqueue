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

import com.baojie.fbq.exception.FileEOF;
import com.baojie.fbq.exception.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * 完成基于文件的先进先出的读写功能
 */
public class FSQueue {

    private static final Logger log = LoggerFactory.getLogger(FSQueue.class);
    private int entityLimitLength;
    private String path = null;
    /**
     * 文件操作实例
     */
    private Index idx = null;
    private Entity writerHandle = null;
    private Entity readerHandle = null;
    /**
     * 文件操作位置信息
     */
    private int readerIndex = -1;
    private int writerIndex = -1;

    protected FSQueue(String dir) throws IOException, FileFormat {
        this(new File(dir));
    }

    /**
     * 在指定的目录中，以fileLimitLength为单个数据文件的最大大小限制初始化队列存储
     *
     * @param dir               队列数据存储的路径
     * @param entityLimitLength 单个数据文件的大小，不能超过2G
     * @throws IOException
     * @throws FileFormat
     */
    protected FSQueue(String dir, int entityLimitLength) throws IOException, FileFormat {
        this(new File(dir), entityLimitLength);
    }

    protected FSQueue(File dir) throws IOException, FileFormat {
        this(dir, 1024 * 1024 * 2);
    }

    /**
     * 在指定的目录中，以fileLimitLength为单个数据文件的最大大小限制初始化队列存储
     *
     * @param dir               队列数据存储的目录
     * @param entityLimitLength 单个数据文件的大小，不能超过2G
     * @throws IOException
     * @throws FileFormat
     */
    protected FSQueue(File dir, int entityLimitLength) throws IOException, FileFormat {
        if (dir.exists() == false && dir.isDirectory() == false) {
            if (dir.mkdirs() == false) {
                throw new IOException("create dir error");
            }
        }
        if (entityLimitLength < FQueue.pageSize()) {
            throw new IllegalArgumentException("error limit length, pageSize=" + FQueue.pageSize());
        }
        this.entityLimitLength = entityLimitLength;
        path = dir.getAbsolutePath();
        // 打开索引文件
        idx = new Index(path);
        initHandle();
    }

    private void initHandle() throws IOException, FileFormat {
        writerIndex = idx.getWriterIndex();
        readerIndex = idx.getReaderIndex();
        writerHandle = new Entity(path, writerIndex, entityLimitLength, idx);
        if (readerIndex == writerIndex) {
            readerHandle = writerHandle;
        } else {
            readerHandle = new Entity(path, readerIndex, entityLimitLength, idx);
        }
    }

    /**
     * 一个文件的数据写入达到fileLimitLength的时候，滚动到下一个文件实例
     *
     * @throws IOException
     * @throws FileFormat
     */
    private void rotateNextLogWriter() throws IOException, FileFormat {
        writerIndex = writerIndex + 1;
        writerHandle.putNextFileNumber(writerIndex);
        if (readerHandle != writerHandle) {// 如果这里关闭了，那么readhandler也关闭了
            writerHandle.close();
        }
        idx.putWriterIndex(writerIndex);
        writerHandle = new Entity(path, writerIndex, entityLimitLength, idx, true);
    }

    /**
     * 向队列存储添加一个byte数组
     *
     * @param e
     * @throws IOException
     * @throws FileFormat
     */
    public void add(byte[] e) throws IOException, FileFormat {
        if (null == e) {
            throw new NullPointerException();
        }
        short status = writerHandle.write(e);
        if (status == Entity.WRITEFULL) {
            rotateNextLogWriter();
            status = writerHandle.write(e);
        }
        if (status == Entity.WRITESUCCESS) {
            idx.incrementSize();
        }
    }

    private byte[] read(boolean commit) throws IOException, FileFormat {
        byte[] bytes;
        try {
            bytes = readerHandle.read(commit);
        } catch (FileEOF e) {
            int nextFileNumber = readerHandle.getNextFileNumber();
            readerHandle.reset();
            File deleteFile = readerHandle.getFile();
            readerHandle.close();
            deleteFile.delete();
            // 更新下一次读取的位置和索引
            idx.putReaderPosition(Entity.MESSAGE_START_POSITION);
            idx.putReaderIndex(nextFileNumber);
            if (writerHandle.getCurrentFileNumber() == nextFileNumber) {
                readerHandle = writerHandle;
            } else {
                readerHandle = new Entity(path, nextFileNumber, entityLimitLength, idx);
            }
            try {
                bytes = readerHandle.read(commit);
            } catch (FileEOF e1) {
                throw new FileFormat(e1);
            } catch (Throwable t) {
                throw new IOException(t.getCause());
            }
        }
        if (commit && bytes != null) {
            idx.decrementSize();
        }
        return bytes;
    }

    /**
     * 读取队列头的数据，但不移除。
     *
     * @return
     * @throws IOException
     * @throws FileFormat
     */
    public byte[] readNext() throws IOException, FileFormat {
        return read(false);
    }

    /**
     * 从队列存储中取出最先入队的数据，并移除它
     *
     * @return
     * @throws IOException
     * @throws FileFormat
     */
    public byte[] readNextAndRemove() throws IOException, FileFormat {
        return read(true);
    }

    public void clear() throws IOException, FileFormat {
        idx.clear();
        initHandle();
    }

    public void close() throws IOException {
        readerHandle.close();
        writerHandle.close();
        idx.close();
    }

    public int size() {
        return idx.size();
    }
}
