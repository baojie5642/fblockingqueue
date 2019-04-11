package com.baojie.make.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

// 这个index借鉴别人代码
// 1.设计上面不错,但是还没测试性能
// 2.因为使用读写分离,会出现index的可见性问题,那怎么办?暂时使用读写锁解决
// 3.因为外部queue为同步的,所以这里可以不用锁,也可以不使用lock,但是为了达到设计上的优雅,使用锁
// 4.使用读写锁也没有问题,问题不在与锁,而在于锁竞争
// 5.使用两个index map,同时子类使用两把锁来达到分别控制同一块磁盘存储的不同区域
// 6.因为这里实现的是阻塞队列所以也可以不用锁,因为外部已经有锁了,先这样写着后续再改
@sun.misc.Contended
public final class BaojieIndex extends Index {

    private static final Logger log = LoggerFactory.getLogger(BaojieIndex.class);

    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();

    private BaojieIndex(String path) throws IOException {
        super(path);
    }

    public static final BaojieIndex create(String path) throws IOException {
        return new BaojieIndex(path);
    }

    // 操作的同一个磁盘区域的不同起始位置与偏移量
    @Override
    public final int readNum() {
        return getReadInfo(0);
    }

    @Override
    public final int readPosition() {
        return getReadInfo(1);
    }

    @Override
    public final int readCount() {
        return getReadInfo(2);
    }

    private final int getReadInfo(final int flag) {
        final ReentrantLock read = readLock;
        read.lock();
        try {
            if (0 == flag) {
                return getReadNum();
            } else if (1 == flag) {
                return getReadPosition();
            } else {
                return getReadCount();
            }
        } finally {
            read.unlock();
        }
    }

    // 操作的同一个磁盘区域的不同起始位置与偏移量
    @Override
    public final int writeNum() {
        return getWriteInfo(0);
    }

    @Override
    public final int writePosition() {
        return getWriteInfo(1);
    }

    @Override
    public final int writeCount() {
        return getWriteInfo(2);
    }

    private final int getWriteInfo(final int flag) {
        final ReentrantLock write = writeLock;
        write.lock();
        try {
            if (0 == flag) {
                return getWriteNum();
            } else if (1 == flag) {
                return getWritePosition();
            } else {
                return getWriteCount();
            }
        } finally {
            write.unlock();
        }
    }

    @Override
    public final void putWN(int writeNum) {
        putWrite(writeCount, 0);
    }

    @Override
    public final void putWP(int writePosition) {
        putWrite(writeCount, 1);
    }

    @Override
    public final void putWC(int writeCount) {
        putWrite(writeCount, 2);
    }

    private final void putWrite(int value, int which) {
        final ReentrantLock read = readLock;
        read.lock();
        try {
            if (0 == which) {
                putWriteNum(value);
            } else if (1 == which) {
                putWritePosition(value);
            } else {
                putWriteCount(value);
            }
        } finally {
            read.unlock();
        }
    }

    @Override
    public final void putRN(int readNum) {
        putRead(readNum, 0);
    }

    @Override
    public final void putRP(int readPosition) {
        putRead(readPosition, 1);
    }

    @Override
    public final void putRC(int readCount) {
        putRead(readCount, 2);
    }

    private final void putRead(int value, int which) {
        final ReentrantLock read = readLock;
        read.lock();
        try {
            if (0 == which) {
                putReadNum(value);
            } else if (1 == which) {
                putReadPosition(value);
            } else {
                putReadCount(value);
            }
        } finally {
            read.unlock();
        }
    }


    public final void reset() {
        fullyLock();
        try {
            doReset();
        } finally {
            fullyUnlock();
        }
    }

    // 注意锁的获取与释放关系
    private final void fullyLock() {
        writeLock.lock();
        readLock.lock();
    }

    private final void fullyUnlock() {
        readLock.unlock();
        writeLock.unlock();
    }

}
