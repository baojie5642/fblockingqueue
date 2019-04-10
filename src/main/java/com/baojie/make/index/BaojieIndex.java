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
@sun.misc.Contended
public final class BaojieIndex extends AbstractIndex {

    private static final Logger log = LoggerFactory.getLogger(BaojieIndex.class);

    private final ReentrantLock readLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock();


    public BaojieIndex(String path) throws IOException {
        super(path);
    }

    public final int getReadNum() {
        return this.readNum;
    }

    public final int getReadPosition() {
        return this.readPosition;
    }

    public final int getReadCount() {
        return this.readCount;
    }

    public final int getWriteNum() {
        return this.writeNum;
    }

    public final int getWritePosition() {
        return this.writePosition;
    }

    public final int getWriteCount() {
        return this.writeCount;
    }

    public final void reset() {
        int size = writeCount - readCount;
        putReadCount(0);
        putWriteCount(size);
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
        }
    }

}
