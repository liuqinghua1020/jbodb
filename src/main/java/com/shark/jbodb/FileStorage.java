package com.shark.jbodb;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * file
 */
public class FileStorage implements Storage {

    private File file;

    private File fileLock;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private FileStorage(File file){
        this.file = file;
    }

    private void init(){
    }

    public ByteBuffer byteBuffer(){
        return null;
    }

}
