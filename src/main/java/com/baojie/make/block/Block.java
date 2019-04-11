package com.baojie.make.block;

import com.baojie.fbq.clean.LocalCleaner;
import com.baojie.fbq.util.LocalSystem;
import com.baojie.make.index.Index;
import com.baojie.make.info.RafState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public abstract class Block {

    private static final Logger log = LoggerFactory.getLogger(Block.class);

    private static final String BLOCK_FILE_SUFFIX = ".blk"; // 数据文件
    private static final int BLOCK_SIZE = 32 * 1024 * 1024; // 32MB

    private final int EOF = -1;

    protected final String path;
    protected final int blockSize;
    protected final Index index;
    protected final RandomAccessFile raf;
    protected final FileChannel channel;
    protected final MappedByteBuffer real;
    protected final MappedByteBuffer mapped;

    protected Block(String path, Index index, RandomAccessFile raf, FileChannel channel,
            MappedByteBuffer real, MappedByteBuffer mapped, int blockSize) {
        this.raf = raf;
        this.path = path;
        this.real = real;
        this.index = index;
        this.mapped = mapped;
        this.channel = channel;
        this.blockSize = blockSize;
    }

    protected Block(Index index, String path, int blockSize) throws IOException {
        if (null == index || null == path) {
            throw new NullPointerException();
        }
        if (path.trim().length() <= 0) {
            throw new IllegalArgumentException();
        }
        if (blockSize <= LocalSystem.pageSize()) {
            throw new IllegalArgumentException();
        }
        this.path = path;
        this.index = index;
        this.blockSize = blockSize;
        File file = new File(path);
        this.raf = new RandomAccessFile(file, RafState.O_DSYNC.value());
        this.channel = raf.getChannel();
        this.mapped = channel.map(FileChannel.MapMode.READ_WRITE, 0, blockSize);
        this.real = mapped.load();
    }

    public abstract Block duplicate();

    public String getPath() {
        return path;
    }

    public final void putEOF() {
        this.real.position(index.writePosition());
        this.real.putInt(EOF);
    }

    public final boolean isSpaceAvailable(int len) {
        int increment = len + 4;
        int writePosition = index.writePosition();
        return BLOCK_SIZE >= increment + writePosition + 4; // 保证最后有4字节的空间可以写入EOF
    }

    public final boolean eof() {
        int readPosition = index.readPosition();
        return readPosition > 0 && real.getInt(readPosition) == EOF;
    }

    public final int write(byte[] bytes) {
        int len = bytes.length;
        int increment = len + 4;
        int writePosition = index.writePosition();
        real.position(writePosition);
        real.putInt(len);
        real.put(bytes);
        index.putWP(increment + writePosition);
        index.putWC(index.writeCount() + 1);
        return increment;
    }

    public final byte[] read() {
        byte[] bytes;
        int readNum = index.readNum();
        int readPosition = index.readPosition();
        int writeNum = index.writeNum();
        int writePosition = index.writePosition();
        if (readNum == writeNum && readPosition >= writePosition) {
            return null;
        }
        real.position(readPosition);
        int dataLength = real.getInt();
        if (dataLength <= 0) {
            return null;
        }
        bytes = new byte[dataLength];
        real.get(bytes);
        index.putRP(readPosition + bytes.length + 4);
        index.putRC(index.readCount() + 1);
        return bytes;
    }

    public void sync() {
        if (mapped != null) {
            mapped.force();
        }
    }

    public void close() {
        try {
            if (mapped == null) {
                return;
            }
            sync();
            LocalCleaner.clean(mapped);
            channel.close();
            raf.close();
        } catch (IOException e) {
            log.error("close fqueue block file failed", e);
        }
    }

}
