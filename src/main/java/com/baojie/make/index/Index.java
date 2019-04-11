package com.baojie.make.index;

import com.baojie.fbq.clean.LocalCleaner;
import com.baojie.make.info.Head;
import com.baojie.make.info.LocalPosition;
import com.baojie.make.info.RafState;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReentrantLock;

// 这个index借鉴别人代码
// 1.设计上面不错,但是还没测试性能
// 2.因为使用读写分离,会出现index的可见性问题,那怎么办?暂时使用读写锁解决
// 3.因为外部queue为同步的,所以这里可以不用锁,也可以不使用lock,但是为了达到设计上的优雅,使用锁
// 4.使用读写锁也没有问题,问题不在与锁,而在于锁竞争
// 5.使用两个index map,同时子类使用两把锁来达到分别控制同一块磁盘存储的不同区域
@sun.misc.Contended
public abstract class Index {

    protected volatile int readNum;        // 8    读索引文件号
    protected volatile int readPosition;   // 12   读索引位置
    protected volatile int readCount;      // 16   总读取数量

    protected volatile int writeNum;       // 20  写索引文件号
    protected volatile int writePosition;  // 24  写索引位置
    protected volatile int writeCount;     // 28  总写入数量

    private final RandomAccessFile raf;
    private final FileChannel channel;

    // 读写分离
    private final MappedByteBuffer writeIndex;
    private final MappedByteBuffer readIndex;

    public Index(String path) throws IOException {
        final String real = check(path);
        final File file = file(real);
        if (file.exists()) {
            if (!file.isFile()) {
                final String name = file.getAbsolutePath();
                throw new IllegalArgumentException("illegal file=" + name);
            }
            this.raf = raf(file, rafMode());
            // 版本信息不一致抛出异常
            if (!version(raf)) {
                throw new IllegalArgumentException("version mismatch");
            }
            // 按顺序读出
            this.readNum = raf.readInt();
            this.readPosition = raf.readInt();
            this.readCount = raf.readInt();
            this.writeNum = raf.readInt();
            this.writePosition = raf.readInt();
            this.writeCount = raf.readInt();
            this.channel = raf.getChannel();
            this.writeIndex = channel.map(channelMode(), 0, indexSize()).load();
            this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
        } else {
            if (!file.createNewFile()) {
                final String name = file.getAbsolutePath();
                throw new IllegalStateException("create fail, file=" + name);
            }
            this.raf = raf(file, rafMode());
            this.channel = raf.getChannel();
            this.writeIndex = channel.map(channelMode(), 0, indexSize());
            this.readIndex = (MappedByteBuffer) writeIndex.duplicate();
            putMagic();
            putReadNum(0);
            putReadPosition(0);
            putReadCount(0);
            putWriteNum(0);
            putWritePosition(0);
            putWriteCount(0);
        }
    }

    protected final String check(final String path) {
        if (null == path) {
            throw new NullPointerException();
        }
        final String tmp = path.trim();
        if (tmp.length() <= 0) {
            throw new IllegalArgumentException();
        }
        return tmp;
    }

    protected final File file(String path) {
        return new File(path, Head.INDEX_NAME.value());
    }

    protected final int indexSize() {
        return LocalPosition.INDEX_SIZE.value();
    }

    protected final String rafMode() {
        return RafState.O_DSYNC.value();
    }

    protected final FileChannel.MapMode channelMode() {
        return FileChannel.MapMode.READ_WRITE;
    }

    protected final String magic() {
        return Head.MAGIC.value();
    }

    protected final boolean version(final RandomAccessFile raf) throws IOException {
        byte[] bytes = new byte[8];
        raf.read(bytes, 0, 8);
        if (magic().equals(new String(bytes))) {
            return true;
        } else {
            return false;
        }
    }

    protected final RandomAccessFile raf(final File info, final String mode) throws FileNotFoundException {
        return new RandomAccessFile(info, mode);
    }

    private final int rn() {
        return LocalPosition.READ_NUM_OFFSET.value();
    }

    private final int rp() {
        return LocalPosition.READ_POS_OFFSET.value();
    }

    private final int rc() {
        return LocalPosition.READ_CNT_OFFSET.value();
    }

    private final int wn() {
        return LocalPosition.WRITE_NUM_OFFSET.value();
    }

    private final int wp() {
        return LocalPosition.WRITE_POS_OFFSET.value();
    }

    private final int wc() {
        return LocalPosition.WRITE_CNT_OFFSET.value();
    }

    protected final void putMagic() {
        this.writeIndex.position(0);
        this.writeIndex.put(magic().getBytes());
    }

    protected final void putWritePosition(int writePosition) {
        this.writeIndex.position(wp());
        this.writeIndex.putInt(writePosition);
        this.writePosition = writePosition;
    }

    protected final void putWriteNum(int writeNum) {
        this.writeIndex.position(wn());
        this.writeIndex.putInt(writeNum);
        this.writeNum = writeNum;
    }

    protected final void putWriteCount(int writeCount) {
        this.writeIndex.position(wc());
        this.writeIndex.putInt(writeCount);
        this.writeCount = writeCount;
    }

    protected final void putReadNum(int readNum) {
        this.readIndex.position(rn());
        this.readIndex.putInt(readNum);
        this.readNum = readNum;
    }

    protected final void putReadPosition(int readPosition) {
        this.readIndex.position(rp());
        this.readIndex.putInt(readPosition);
        this.readPosition = readPosition;
    }

    protected final void putReadCount(int readCount) {
        this.readIndex.position(rc());
        this.readIndex.putInt(readCount);
        this.readCount = readCount;
    }

    protected final int getReadNum() {
        return this.readNum;
    }

    protected final int getReadPosition() {
        return this.readPosition;
    }

    protected final int getReadCount() {
        return this.readCount;
    }

    protected final int getWriteNum() {
        return this.writeNum;
    }

    protected final int getWritePosition() {
        return this.writePosition;
    }

    protected final int getWriteCount() {
        return this.writeCount;
    }


    protected final void doSync() {
        if (writeIndex != null) {
            writeIndex.force();
        }
    }

    protected final void doReset() {
        // 计算剩余已写总数
        int size = writeCount - readCount;
        // 设置读取总数为0
        putReadCount(0);
        // 设置已写的数量
        putWriteCount(size);
        // 这时说明没有数据在磁盘中
        if (size == 0 && readNum == writeNum) {
            putReadPosition(0);
            putWritePosition(0);
        }
    }

    protected final void doClose() throws IOException {
        doSync();
        LocalCleaner.clean(writeIndex);
        LocalCleaner.clean(readIndex);
        channel.close();
        raf.close();
    }

    public abstract int readNum();

    public abstract int readPosition();

    public abstract int readCount();

    public abstract int writeNum();

    public abstract int writePosition();

    public abstract int writeCount();

    public abstract void putWN(int writeNum);

    public abstract void putWP(int writePosition);

    public abstract void putWC(int writeCount);

    public abstract void putRN(int readNum);

    public abstract void putRP(int readPosition);

    public abstract void putRC(int readCount);

}
