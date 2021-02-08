package com.shark.jbodb;

import io.netty.buffer.ByteBuf;
import lombok.Builder;
import lombok.Getter;

/**
 *
 *  |         page header             |
 *   pageid | flag | count | overflow |  ele1   |  ele2   | k1|v1 | k2|v2 |
 *                                    |                   |
 */
@Builder
public class Page {

    public static final int CONTENT_OFFSET = 8 /** pageid **/
                                             + 2 /** flag **/
                                             + 2 /** count **/
                                             + 4 ; /** overflow **/

    //---------- 磁盘存储属性 -----------------

    //page编号, 8个字节
    @Getter
    private long pgid;

    //page的类型，参考 @linker PageFlag 2个字节
    @Getter
    private short flag;

    //本页面包含的 page count 2个字节
    @Getter
    private short count;

    // 这是一个字段，表明后续还有多少page和它一起组成一个连续的页,，表明记录是否跨页 4个字节
    @Getter
    private int overflow;


    //---------- 磁盘存储属性 -----------------

    //---------- 内存信息 -----------------

    private ByteBuf byteBuf;

    public Page(ByteBuf byteBuf){
        this.byteBuf = byteBuf;
    }




    public void setPgid(long pgid){
        this.pgid = pgid;
        this.byteBuf.writeLong(pgid);
    }

    public void setFlag(short flag){
        this.flag = flag;
        this.byteBuf.writeShort(flag);
    }

    public void setCount(short count){
        this.count = count;
        this.byteBuf.writeShort(count);
    }

    public void setOverflow(int overflow){
        this.overflow = overflow;
        this.byteBuf.writeInt(overflow);
    }

    public ByteBuf slice(int offset, int length){
        return byteBuf.slice(offset, length);
    }

    public void writeInt(int intNum){
        //TODO
    }

    public void writeLong(long longNum){
        //TODO
    }

}
