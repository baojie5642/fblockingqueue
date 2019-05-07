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
package com.baojie.fbq;

import com.baojie.fbq.exception.FileFormatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 基于文件系统的持久化队列
 */
public class FQueue extends AbstractQueue<byte[]> implements BlockingQueue<byte[]>, java.io.Serializable {
	private static final long serialVersionUID = -5960741434564940154L;
	private FSQueue fsQueue = null;
	private static final Logger log = LoggerFactory.getLogger(FQueue.class);
	private Lock lock = new ReentrantReadWriteLock().writeLock();

	public FQueue(String path) throws Exception {
		fsQueue = new FSQueue(path, 1024 * 1024 * 300);
	}

	public FQueue(String path, int logsize) throws Exception {
		fsQueue = new FSQueue(path, logsize);
	}

	@Override
	public Iterator<byte[]> iterator() {
		throw new UnsupportedOperationException("iterator Unsupported now");
	}

	@Override
	public int size() {
		return fsQueue.getQueuSize();
	}

	@Override
	public boolean offer(byte[] e) {
		try {
			lock.lock();
			fsQueue.add(e);
			return true;
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (FileFormatException e1) {
			e1.printStackTrace();
		} finally {
			lock.unlock();
		}
		return false;
	}

	@Override
	public void put(byte[] bytes) throws InterruptedException {

	}

	@Override
	public boolean offer(byte[] bytes, long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}

	@Override
	public byte[] take() throws InterruptedException {
		return new byte[0];
	}

	@Override
	public byte[] poll(long timeout, TimeUnit unit) throws InterruptedException {
		return new byte[0];
	}

	@Override
	public int remainingCapacity() {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super byte[]> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super byte[]> c, int maxElements) {
		return 0;
	}

	@Override
	public byte[] peek() {
		throw new UnsupportedOperationException("peek Unsupported now");
	}

	@Override
	public byte[] poll() {
		try {
			lock.lock();
			return fsQueue.readNextAndRemove();
		} catch (IOException e) {
			log.error(e.getMessage(), e);
			return null;
		} catch (FileFormatException e) {
			log.error(e.getMessage(), e);
			return null;
		} finally {
			lock.unlock();
		}
	}

	public void close() {
		if (fsQueue != null) {
			fsQueue.close();
		}
	}
}