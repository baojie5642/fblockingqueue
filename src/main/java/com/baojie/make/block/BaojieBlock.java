package com.baojie.make.block;


import com.baojie.make.index.Index;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class BaojieBlock extends Block {

    private BaojieBlock(Index index, String path, int blockSize) throws IOException {
        super(index, path, blockSize);
    }

    private BaojieBlock(String path, Index index, RandomAccessFile raf, FileChannel channel,
            MappedByteBuffer real, MappedByteBuffer mapped, int blockSize) {
        super(path, index, raf, channel, real, mapped, blockSize);
    }

    public static final BaojieBlock create(Index index, String path, int blockSize) throws IOException {
        return new BaojieBlock(index, path, blockSize);
    }

    @Override
    public final Block duplicate() {
        return new BaojieBlock(path, index, raf, channel, real, mapped, blockSize);
    }


}
