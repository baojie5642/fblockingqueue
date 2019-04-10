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

import com.baojie.fbq.bytes.LocalBytes;
import com.baojie.fbq.exception.FileFormat;
import com.baojie.fbq.unsafe.LocalUnsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 基于文件系统的持久化队列
 */
public class FQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, java.io.Serializable {

    private static final Logger log = LoggerFactory.getLogger(FQueue.class);

    private static final long serialVersionUID = -5960741434564940154L;
    private static final int pageSize;

    static {
        try {
            pageSize = LocalUnsafe.getUnsafe().pageSize();
        } catch (Throwable t) {
            log.error(t.toString(), t);
            throw new Error(t.getCause());
        }
    }

    public static final int pageSize() {
        return pageSize;
    }

    static class Node<E> {

        E item;

        Node(E x) {
            item = x;
        }

    }


    private final ReentrantLock mainLock = new ReentrantLock();

    private final Condition mainContion = mainLock.newCondition();


    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.mainLock;
        takeLock.lock();
        try {
            mainContion.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.mainLock;
        putLock.lock();
        try {
            mainContion.signal();
        } finally {
            putLock.unlock();
        }
    }

    private final int capacity;

    private final FSQueue fsQueue;

    public FQueue(String path, int capacity) throws IOException, FileFormat {
        this.fsQueue = new FSQueue(path);
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
    }

    /**
     * 创建一个持久化队列
     *
     * @param path              文件的存储路径
     * @param entityLimitLength 存储数据的单个文件的大小
     * @throws IOException
     * @throws FileFormat
     */
    public FQueue(String path, int entityLimitLength, int capacity) throws IOException, FileFormat {
        if (entityLimitLength < pageSize()) {
            throw new IllegalArgumentException("error limit length, pageSize=" + pageSize());
        } else {
            this.fsQueue = new FSQueue(path, entityLimitLength);
            if (capacity <= 0) throw new IllegalArgumentException();
            this.capacity = capacity;
        }
    }

    public FQueue(File dir, int capacity) throws IOException, FileFormat {
        this.fsQueue = new FSQueue(dir);
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
    }

    /**
     * 创建一个持久化队列
     *
     * @param dir               文件的存储目录
     * @param entityLimitLength 存储数据的单个文件的大小
     * @throws IOException
     * @throws FileFormat
     */
    public FQueue(File dir, int entityLimitLength, int capacity) throws IOException, FileFormat {
        fsQueue = new FSQueue(dir, entityLimitLength);
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
    }

    @Override
    public Iterator<E> iterator() {
        throw new UnsupportedOperationException("iterator Unsupported now");
    }

    @Override
    public int size() {
        return fsQueue.size();
    }

    @Override
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();
        if (size() == capacity) {
            return false;
        }
        int c = -1;
        Node<E> node = new Node<E>(e);
        final ReentrantLock putLock = this.mainLock;
        putLock.lock();
        try {
            if (size() < capacity) {
                localAdd(node);
                c = size();
                if (c + 1 < capacity) {
                    mainContion.signal();
                }
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return c >= 0;
    }

    private boolean localAdd(Node<E> node) {
        try {
            fsQueue.add(LocalBytes.seria(node));
            return true;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (FileFormat ex) {
        }
        return false;
    }

    @Override
    public void put(E e) throws InterruptedException {
        if (null == e) throw new NullPointerException();
        int c = -1;
        final ReentrantLock putLock = this.mainLock;
        putLock.lockInterruptibly();
        try {
            while (size() == capacity) {
                mainContion.await();
            }
            localAdd(new Node<E>(e));
            c = size();
            if (c + 1 < capacity) {
                mainContion.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (e == null) throw new NullPointerException();
        long nanos = unit.toNanos(timeout);
        int c = -1;
        final ReentrantLock putLock = this.mainLock;
        putLock.lockInterruptibly();
        try {
            while (size() >= capacity) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = mainContion.awaitNanos(nanos);
            }
            localAdd(new Node<E>(e));
            c = size();
            if (c >= 1 && c + 1 < capacity) {
                mainContion.signal();
            }
        } finally {
            putLock.unlock();
        }
        if (c == 0) {
            signalNotEmpty();
        }
        return true;
    }

    @Override
    public E take() throws InterruptedException {
        E x;
        int c = -1;
        final ReentrantLock takeLock = this.mainLock;
        takeLock.lockInterruptibly();
        try {
            while (size() <= 0) {
                mainContion.await();
            }
            x = localRead(true);
            c = size();
            if (c >= 1) {
                mainContion.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E x = null;
        int c = -1;
        long nanos = unit.toNanos(timeout);
        final ReentrantLock takeLock = this.mainLock;
        takeLock.lockInterruptibly();
        try {
            while (size() == 0) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = mainContion.awaitNanos(nanos);
            }
            x = localRead(true);
            c = size();
            if (c > 1) {
                mainContion.signal();
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
    }

    @Override
    public E poll() {
        if (size() == 0) {
            return null;
        }
        E x = null;
        int c = -1;
        final ReentrantLock takeLock = this.mainLock;
        takeLock.lock();
        try {
            if (size() > 0) {
                x = localRead(true);
                c = size();
                if (c > 1) {
                    mainContion.signal();
                }
            }
        } finally {
            takeLock.unlock();
        }
        if (c == capacity) {
            signalNotFull();
        }
        return x;
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
    public E peek() {
        if (size() == 0) {
            return null;
        }
        final ReentrantLock takeLock = this.mainLock;
        takeLock.lock();
        try {
            return localRead(false);
        } finally {
            takeLock.unlock();
        }
    }

    private final E localRead(boolean commit) {
        byte[] data = null;
        try {
            if (commit) {
                data = fsQueue.readNextAndRemove();
            } else {
                data = fsQueue.readNext();
            }
        } catch (IOException ex) {

        } catch (FileFormat ex) {

        } catch (Throwable t) {

        }
        if (null == data || data.length <= 0) {
            return null;
        } else {
            Node<E> node = LocalBytes.deseria(data, Node.class);
            return node.item;
        }
    }

    @Override
    public void clear() {
        fullyLock();
        try {
            fsQueue.clear();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } catch (FileFormat e) {
            // ignore
        } finally {
            fullyUnlock();
        }
    }

    void fullyLock() {
        mainLock.lock();
    }

    void fullyUnlock() {
        mainLock.unlock();
    }

    /**
     * 关闭文件队列
     *
     * @throws IOException
     * @throws FileFormat
     */
    public void close() throws IOException {
        fullyLock();
        try {
            if (fsQueue != null) {
                fsQueue.close();
            }
        } finally {
            fullyUnlock();
        }
    }
}
