package com.shark.jbodb;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * file
 */
public class DBStorage {

    private File file;

    private File fileLock;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;


    private DBStorage(File file){
        this.file = file;
    }

}
