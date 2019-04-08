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
import com.baojie.fbq.exception.FileEOF;
import com.baojie.fbq.exception.FileFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Entity {
	private static final Logger log = LoggerFactory.getLogger(Entity.class);

	private static final ExecutorService SERVICE= Executors.newFixedThreadPool(32);

	public static final byte WRITESUCCESS = 1;
	public static final byte WRITEFAILURE = 2;
	public static final byte WRITEFULL = 3;
	public static final String MAGIC = "FQueuefs";
	public static int MESSAGE_START_POSITION = 20;
	private static final String DB_FILE_PREFIX = "fq_";
	private static final String DB_FILE_SUFFIX = ".db";
	private File file;
	private RandomAccessFile raFile;
	private FileChannel fc;
	private MappedByteBuffer mappedByteBuffer;
	private Index idx = null;
	private int fileLimitLength;
	/**
	 * 文件操作位置信息
	 */
	private String magicString = null;
	private int version = -1;
	private int readerPosition = -1;
	private int writerPosition = -1;
	private int endPosition = -1;
	private int currentFileNumber = -1;
	private int nextFileNumber = -1;

	protected Entity(String path, int fileNumber, int fileLimitLength, Index db)
			throws IOException, FileFormat {
		this(path, fileNumber, fileLimitLength, db, false);
	}

	protected Entity(String path, int fileNumber, int fileLimitLength,
			Index idx, boolean create) throws IOException, FileFormat {
		this.currentFileNumber = fileNumber;
		this.fileLimitLength = fileLimitLength;
		this.idx = idx;
		this.file = getIdbFile(path, fileNumber);	// 创建真正的数据存储的文件
		// 文件不存在，创建文件
		if (!file.exists() || create) {
			createLogEntity();
		} else {
			raFile = new RandomAccessFile(file, "rwd");
			int fileLength = (int) raFile.length();
			if (fileLength < MESSAGE_START_POSITION) {
				throw new FileFormat("file format error");
			}
			// 如果原文件大小比定义的长度要大，文件大小以实际大小为准，目的是防止有数据未被读取时截断数据。
			if (fileLimitLength < fileLength) {
				fileLimitLength = fileLength;
				this.fileLimitLength = fileLength;
			}
			fc = raFile.getChannel();
			mappedByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileLimitLength);
			// magicString
			byte[] bytes = new byte[8];
			mappedByteBuffer.get(bytes);
			magicString = new String(bytes);
			if (magicString.equals(MAGIC) == false) {
				throw new FileFormat("file format error");
			}
			// version
			version = mappedByteBuffer.getInt();
			// nextFileNumber
			nextFileNumber = mappedByteBuffer.getInt();
			endPosition = mappedByteBuffer.getInt();
			if (endPosition == -1) { // 未写满
				writerPosition = idx.getWriterPosition();
			} else { // 已写满
				writerPosition = endPosition;
			}
			if (idx.getReaderIndex() == currentFileNumber) {
				readerPosition = idx.getReaderPosition();
			} else {
				readerPosition = MESSAGE_START_POSITION;
			}
		}
		SERVICE.submit(new Sync());
	}

	protected int getCurrentFileNumber() {
		return currentFileNumber;
	}

	protected int getNextFileNumber() {
		return nextFileNumber;
	}
	// 在创建真正的数据文件时，将20字节头部写入文件
	private boolean createLogEntity() throws IOException {
		raFile = new RandomAccessFile(file, "rwd");
		raFile.setLength(0);
		fc = raFile.getChannel();
		mappedByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, fileLimitLength);
		mappedByteBuffer.put(MAGIC.getBytes());
		mappedByteBuffer.putInt(version);// 8 version
		mappedByteBuffer.putInt(nextFileNumber);// 12 next fileindex
		mappedByteBuffer.putInt(endPosition);// 16
		magicString = MAGIC;
		writerPosition = MESSAGE_START_POSITION;
		readerPosition = MESSAGE_START_POSITION;
		idx.putWriterPosition(writerPosition);
		return true;
	}

	protected void reset() throws IOException {
		version = -1;
		endPosition = -1;
		currentFileNumber = -1;
		nextFileNumber = -1;
		mappedByteBuffer.position(0);
		mappedByteBuffer.put(MAGIC.getBytes());
		mappedByteBuffer.putInt(version);// 8 version
		mappedByteBuffer.putInt(nextFileNumber);// 12 next fileindex
		mappedByteBuffer.putInt(endPosition);// 16
		mappedByteBuffer.force();
		magicString = MAGIC;
		writerPosition = MESSAGE_START_POSITION;
		readerPosition = MESSAGE_START_POSITION;
	}

	/**
	 * write next File number id.
	 *
	 * @param number
	 */
	protected void putNextFileNumber(int number) throws IOException {
		mappedByteBuffer.position(12);
		mappedByteBuffer.putInt(number);
		nextFileNumber = number;
	}

	protected boolean isFull(int increment) {
		// confirm if the file is full
		if (fileLimitLength < writerPosition + increment) {
			return true;
		}
		return false;
	}

	protected byte write(byte[] bytes) throws IOException {
		int increment = bytes.length + 4;
		if (isFull(increment)) {
			mappedByteBuffer.position(16);
			mappedByteBuffer.putInt(writerPosition);
			endPosition = writerPosition;
			return WRITEFULL;
		}
		mappedByteBuffer.position(writerPosition);
		mappedByteBuffer.putInt(bytes.length);
		mappedByteBuffer.put(bytes);
		writerPosition += increment;
		idx.putWriterPosition(writerPosition);
		return WRITESUCCESS;
	}

	/**
	 * @param commit 如果为 false，则只读取数据，不移动读取指针。
	 * @return
	 * @throws IOException
	 * @throws FileFormat
	 */
	protected byte[] read(boolean commit) throws IOException, FileEOF {
		if (endPosition != -1 && readerPosition >= endPosition) {
			throw new FileEOF("file eof");
		}
		if (readerPosition >= writerPosition) {
			return null;
		}
		mappedByteBuffer.position(readerPosition);
		int length = mappedByteBuffer.getInt();
		byte[] bytes = new byte[length];
		mappedByteBuffer.get(bytes);
		if (commit) {
			readerPosition += length + 4;
			idx.putReaderPosition(readerPosition);
		}
		return bytes;
	}

	protected byte[] read() throws IOException, FileEOF {
		return read(true);
	}

	protected File getFile() {
		return file;
	}

	protected void close() throws IOException {
		if (mappedByteBuffer == null) {
			return;
		}
		mappedByteBuffer.force();
		LocalCleaner.clean(mappedByteBuffer);
		mappedByteBuffer = null;
		fc.close();
		raFile.close();// 不能删除.db文件，因为下次启动可能会读取文件
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
		sb.append(" nextFileNumber:");
		sb.append(nextFileNumber);
		sb.append(" endPosition:");
		sb.append(endPosition);
		sb.append(" currentFileNumber:");
		sb.append(currentFileNumber);
		return sb.toString();
	}

	protected static File getIdbFile(String path, int fileNumber) {
		return new File(path, DB_FILE_PREFIX + fileNumber + DB_FILE_SUFFIX);
	}

	private class Sync implements Runnable {
		@Override
		public void run() {
			while (true) {
				if (mappedByteBuffer != null) {
					try {
						mappedByteBuffer.force();
					} catch (Exception e) {
						break;
					}
					try {
						Thread.sleep(10);
					} catch (InterruptedException e) {
						break;
					}
				} else {
					break;
				}
			}
		}
	}

}
