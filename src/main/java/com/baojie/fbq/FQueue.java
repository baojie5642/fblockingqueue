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

import com.baojie.fbq.exception.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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

	public FQueue(String path) throws IOException, FileFormat {
		fsQueue = new FSQueue(path);
	}

	/**
	 * 创建一个持久化队列
	 *
	 * @param path              文件的存储路径
	 * @param entityLimitLength 存储数据的单个文件的大小
	 * @throws IOException
	 * @throws FileFormat
	 */
	public FQueue(String path, int entityLimitLength) throws IOException, FileFormat {
		fsQueue = new FSQueue(path, entityLimitLength);
	}

	public FQueue(File dir) throws IOException, FileFormat {
		fsQueue = new FSQueue(dir);
	}

	/**
	 * 创建一个持久化队列
	 *
	 * @param dir               文件的存储目录
	 * @param entityLimitLength 存储数据的单个文件的大小
	 * @throws IOException
	 * @throws FileFormat
	 */
	public FQueue(File dir, int entityLimitLength) throws IOException, FileFormat {
		fsQueue = new FSQueue(dir, entityLimitLength);
	}

	@Override
	public Iterator<byte[]> iterator() {
		throw new UnsupportedOperationException("iterator Unsupported now");
	}

	@Override
	public int size() {
		return fsQueue.getQueueSize();
	}

	@Override
	public boolean offer(byte[] e) {
		try {
			lock.lock();
			fsQueue.add(e);
			return true;
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} catch (FileFormat ex) {
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
		try {
			lock.lock();
			return fsQueue.readNext();
		} catch (IOException ex) {
			return null;
		} catch (FileFormat ex) {
			return null;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public byte[] poll() {
		try {
			lock.lock();
			return fsQueue.readNextAndRemove();
		} catch (IOException ex) {
			return null;
		} catch (FileFormat ex) {
			return null;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void clear() {
		try {
			lock.lock();
			fsQueue.clear();
		} catch (IOException ex) {
			throw new RuntimeException(ex);
		} catch (FileFormat e) {
			// ignore
		} finally {
			lock.unlock();
		}
	}

	/**
	 * 关闭文件队列
	 *
	 * @throws IOException
	 * @throws FileFormat
	 */
	public void close() throws IOException, FileFormat {
		if (fsQueue != null) {
			fsQueue.close();
		}
	}
}
