package com.baojie.make.queue;


import com.baojie.fbq.util.LocalSystem;
import com.baojie.make.block.BaojieBlock;
import com.baojie.make.block.Block;
import com.baojie.make.index.BaojieIndex;
import com.baojie.make.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class BaojieDuraQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, java.io.Serializable {

    private static final Logger log = LoggerFactory.getLogger(BaojieDuraQueue.class);

    private static final AtomicInteger blockNum = new AtomicInteger(0);

    private static final long serialVersionUID = -5960741434564940154L;

    // 队列的名称
    private String queueName;
    // 队列数据存储的文件夹路径
    private String path;
    // 队列文件指针
    private Index index;
    // 读取数据的数据块
    private Block readBlock;
    // 写入数据的数据块
    private Block writeBlock;
    private final ReentrantLock readLock;
    private final ReentrantLock writeLock;
    // 队列数据的大小
    private AtomicInteger size;

    public BaojieDuraQueue(String queueName, String path) throws IOException {
        this.path = path;
        this.queueName = queueName;
        this.readLock = new ReentrantLock();
        this.writeLock = new ReentrantLock();
        this.index = BaojieIndex.create("");
        this.size = new AtomicInteger(index.writeCount() - index.readCount());
        this.writeBlock = BaojieBlock.create(index, "", LocalSystem.blockSize());
        if (index.readNum() == index.writeNum()) {
            this.readBlock = this.writeBlock.duplicate();
        } else {
            this.readBlock = BaojieBlock.create(index, "", LocalSystem.blockSize());
        }
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("iterator Unsupported now");
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public void put(E e) throws InterruptedException {

    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return 0;
    }

    @Override
    public boolean offer(E e) {
        return false;
    }

    @Override
    public E poll() {
        return null;
    }

    @Override
    public E peek() {
        return null;
    }
}
