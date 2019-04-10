package com.baojie.mfbq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;

public class MFQueueIndex {
    private static final Logger log = LoggerFactory.getLogger(MFQueueIndex.class);
    private static final String MAGIC = "v.1.0000";
    private static final String INDEX_FILE_SUFFIX = ".idx";
    private static final int INDEX_SIZE = 32;

    private static final int READ_NUM_OFFSET = 8;
    private static final int READ_POS_OFFSET = 12;
    private static final int READ_CNT_OFFSET = 16;
    private static final int WRITE_NUM_OFFSET = 20;
    private static final int WRITE_POS_OFFSET = 24;
    private static final int WRITE_CNT_OFFSET = 28;

    private int p11, p12, p13, p14, p15, p16, p17, p18; // 缓存行填充 32B
    private volatile int readPosition;   // 12   读索引位置
    private volatile int readNum;        // 8   读索引文件号
    private volatile int readCounter;    // 16   总读取数量
    private int p21, p22, p23, p24, p25, p26, p27, p28; // 缓存行填充 32B
    private volatile int writePosition;  // 24  写索引位置
    private volatile int writeNum;       // 20  写索引文件号
    private volatile int writeCounter;   // 28 总写入数量
    private int p31, p32, p33, p34, p35, p36, p37, p38; // 缓存行填充 32B

    private RandomAccessFile indexFile;
    private FileChannel fileChannel;
    // 读写分离
    private MappedByteBuffer writeIndex;
    private MappedByteBuffer readIndex;

    public MFQueueIndex(String indexFilePath) {
        File file = new File(indexFilePath);
        try {
            if (file.exists()) {
                this.indexFile = new RandomAccessFile(file, "rw");
                byte[] bytes = new byte[8];
                this.indexFile.read(bytes, 0, 8);
                if (!MAGIC.equals(new String(bytes))) {
                    throw new IllegalArgumentException("version mismatch");
                }
                this.readNum = indexFile.readInt();
                this.readPosition = indexFile.readInt();
                this.readCounter = indexFile.readInt();
                this.writeNum = indexFile.readInt();
                this.writePosition = indexFile.readInt();
                this.writeCounter = indexFile.readInt();
                this.fileChannel = indexFile.getChannel();
                this.writeIndex = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.writeIndex = writeIndex.load();
                this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
            } else {
                this.indexFile = new RandomAccessFile(file, "rw");
                this.fileChannel = indexFile.getChannel();
                this.writeIndex = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, INDEX_SIZE);
                this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
                putMagic();
                putReadNum(0);
                putReadPosition(0);
                putReadCounter(0);
                putWriteNum(0);
                putWritePosition(0);
                putWriteCounter(0);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static boolean isIndexFile(String fileName) {
        return fileName.endsWith(INDEX_FILE_SUFFIX);
    }

    public static String parseQueueName(String indexFileName) {
        String fileName = indexFileName.substring(0, indexFileName.lastIndexOf('.'));
        return fileName.split("_")[1];
    }

    public static String formatIndexFilePath(String queueName, String fileBackupDir) {
        return fileBackupDir + File.separator + String.format("findex_%s%s", queueName, INDEX_FILE_SUFFIX);
    }

    public int getReadNum() {
        return this.readNum;
    }

    public int getReadPosition() {
        return this.readPosition;
    }

    public int getReadCounter() {
        return this.readCounter;
    }

    public int getWriteNum() {
        return this.writeNum;
    }

    public int getWritePosition() {
        return this.writePosition;
    }

    public int getWriteCounter() {
        return this.writeCounter;
    }

    public void putMagic() {
        this.writeIndex.position(0);
        this.writeIndex.put(MAGIC.getBytes());
    }

    public void putWritePosition(int writePosition) {
        this.writeIndex.position(WRITE_POS_OFFSET);
        this.writeIndex.putInt(writePosition);
        this.writePosition = writePosition;
    }

    public void putWriteNum(int writeNum) {
        this.writeIndex.position(WRITE_NUM_OFFSET);
        this.writeIndex.putInt(writeNum);
        this.writeNum = writeNum;
    }

    public void putWriteCounter(int writeCounter) {
        this.writeIndex.position(WRITE_CNT_OFFSET);
        this.writeIndex.putInt(writeCounter);
        this.writeCounter = writeCounter;
    }

    public void putReadNum(int readNum) {
        this.readIndex.position(READ_NUM_OFFSET);
        this.readIndex.putInt(readNum);
        this.readNum = readNum;
    }

    public void putReadPosition(int readPosition) {
        this.readIndex.position(READ_POS_OFFSET);
        this.readIndex.putInt(readPosition);
        this.readPosition = readPosition;
    }

    public void putReadCounter(int readCounter) {
        this.readIndex.position(READ_CNT_OFFSET);
        this.readIndex.putInt(readCounter);
        this.readCounter = readCounter;
    }

    public void reset() {
        int size = writeCounter - readCounter;
        putReadCounter(0);
        putWriteCounter(size);
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
        }
    }

    public void sync() {
        if (writeIndex != null) {
            writeIndex.force();
        }
    }

    public void close() {
        try {
            if (writeIndex == null) {
                return;
            }
            sync();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    try {
                        Method getCleanerMethod = writeIndex.getClass().getMethod("cleaner");
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner = (sun.misc.Cleaner) getCleanerMethod.invoke(writeIndex);
                        cleaner.clean();
                    } catch (Exception e) {
                        log.error("close fqueue index file failed", e);
                    }
                    return null;
                }
            });
            writeIndex = null;
            readIndex = null;
            fileChannel.close();
            indexFile.close();
        } catch (IOException e) {
            log.error("close fqueue index file failed", e);
        }
    }
}

