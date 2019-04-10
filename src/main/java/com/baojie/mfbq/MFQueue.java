package com.baojie.mfbq;

import org.apache.commons.lang3.ArrayUtils;

import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class MFQueue  extends AbstractQueue<byte[]> {

    private String queueName;
    private String fileBackupDir;
    private MFQueueIndex index;
    private MFQueueBlock readBlock;
    private MFQueueBlock writeBlock;
    private ReentrantLock readLock;
    private ReentrantLock writeLock;
    private AtomicInteger size;

    public MFQueue(String queueName, String fileBackupDir) {
        this.queueName = queueName;
        this.fileBackupDir = fileBackupDir;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();
        this.index = new MFQueueIndex(MFQueueIndex.formatIndexFilePath(queueName, fileBackupDir));
        this.size = new AtomicInteger(index.getWriteCounter() - index.getReadCounter());
        this.writeBlock = new MFQueueBlock(index, MFQueueBlock.formatBlockFilePath(queueName,
                index.getWriteNum(), fileBackupDir));
        if (index.getReadNum() == index.getWriteNum()) {
            this.readBlock = this.writeBlock.duplicate();
        } else {
            this.readBlock = new MFQueueBlock(index, MFQueueBlock.formatBlockFilePath(queueName,
                    index.getReadNum(), fileBackupDir));
        }
    }

    @Override
    public Iterator<byte[]> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        return this.size.get();
    }

    private void rotateNextWriteBlock() {
        int nextWriteBlockNum = index.getWriteNum() + 1;
        nextWriteBlockNum = (nextWriteBlockNum < 0) ? 0 : nextWriteBlockNum;
        writeBlock.putEOF();
        if (index.getReadNum() == index.getWriteNum()) {
            writeBlock.sync();
        } else {
            writeBlock.close();
        }
        writeBlock = new MFQueueBlock(index, MFQueueBlock.formatBlockFilePath(queueName,
                nextWriteBlockNum, fileBackupDir));
        index.putWriteNum(nextWriteBlockNum);
        index.putWritePosition(0);
    }

    @Override
    public boolean offer(byte[] bytes) {
        if (ArrayUtils.isEmpty(bytes)) {
            return true;
        }
        writeLock.lock();
        try {
            if (!writeBlock.isSpaceAvailable(bytes.length)) {
                rotateNextWriteBlock();
            }
            writeBlock.write(bytes);
            size.incrementAndGet();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    private void rotateNextReadBlock() {
        if (index.getReadNum() == index.getWriteNum()) {
            // 读缓存块的滑动必须发生在写缓存块滑动之后
            return;
        }
        int nextReadBlockNum = index.getReadNum() + 1;
        nextReadBlockNum = (nextReadBlockNum < 0) ? 0 : nextReadBlockNum;
        readBlock.close();
        String blockPath = readBlock.getBlockFilePath();
        if (nextReadBlockNum == index.getWriteNum()) {
            readBlock = writeBlock.duplicate();
        } else {
            readBlock = new MFQueueBlock(index, MFQueueBlock.formatBlockFilePath(queueName,
                    nextReadBlockNum, fileBackupDir));
        }
        index.putReadNum(nextReadBlockNum);
        index.putReadPosition(0);
        MFQueuePool.toClear(blockPath);
    }

    @Override
    public byte[] poll() {
        readLock.lock();
        try {
            if (readBlock.eof()) {
                rotateNextReadBlock();
            }
            byte[] bytes = readBlock.read();
            if (bytes != null) {
                size.decrementAndGet();
            }
            return bytes;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public byte[] peek() {
        throw new UnsupportedOperationException();
    }

    public void sync() {
        index.sync();
        // read block只读，不用同步
        writeBlock.sync();
    }

    public void close() {
        writeBlock.close();
        if (index.getReadNum() != index.getWriteNum()) {
            readBlock.close();
        }
        index.reset();
        index.close();
    }


}
