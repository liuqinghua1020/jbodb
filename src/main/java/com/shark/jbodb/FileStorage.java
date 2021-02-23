package com.shark.jbodb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/**
 * file
 */
public class FileStorage implements Storage {

    public static final int TOTAL_SIZE = 1024 * 1024 * 1024 ; //1G

    private String dataDir;

    private File file;

    private FileChannel fileChannel;




    private FileLock fileLock;

    private MappedByteBuffer mappedByteBuffer;

    private ByteBuf wrapperMappedByteBuf;

    private boolean newCreate;


    public FileStorage(String dataDir){
       this.dataDir = dataDir;
    }

    public void init()  {
        File file = new File(dataDir);

        try {
            /**
             * 第一次初始化，没有数据文件
             */
            if(!file.exists()) {
                file.createNewFile();
                newCreate = true;
            }
            fileChannel = new RandomAccessFile(file, "rw").getChannel();
            mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, TOTAL_SIZE);
            wrapperMappedByteBuf = Unpooled.wrappedBuffer(mappedByteBuffer);
        }catch (Exception e){

        }

    }

    public boolean isNewCreate() {
        return newCreate;
    }

    public void remapped(){
        //TODO
    }

    /**
     * @link  public Page getPage(int pgid)
     * @param position
     * @param lenght
     * @return
     */
    @Deprecated
    public ByteBuf byteBuf(int position, int lenght){
        return wrapperMappedByteBuf.slice(position, lenght);
    }

    /**
     * 返回指定页面
     * @param pgid >=0
     * @return
     */
    public Page getPage(int pgid){
        //TODO
        return null;
    }

    public void sync(){
        mappedByteBuffer.force();
    }

    public ByteBuf slice(int position, int length){
        return wrapperMappedByteBuf.slice(position, length);
    }

    public Page page(long pgid){
        return new Page(wrapperMappedByteBuf.slice((int)pgid*DB.PAGE_SIZE, DB.PAGE_SIZE));
    }

}
