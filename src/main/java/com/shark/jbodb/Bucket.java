package com.shark.jbodb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 在 jbodb概念中，
 * bucket是一个子树，里面包含完整的 k/v 结构
 * 如果bucket的内容很小，可以将整个bucket作为一个结构，放到上层的一个bucket的k/v的v中，k为bucket的name
 * 如果bucket比较大， 则只是将bucket的root pgid 作为一个k放到 上层bucket的 k/v的v中。
 *
 * 整个jodb就是一个完整的匿名的bucket，每一个构建都会将根作为一个匿名的bucket进行处理
 */
@Getter
@Builder
public class Bucket {

    public static final int BucketHeaderSize = 8 /** rootPgid **/ + 8 /** sequence **/;

    public static final float DefaultFillPercent = 0.5f;

    @Setter
    private long rootPgid;

    @Setter
    private long sequence;

    //对应的事务
    private Tx tx;

    /**
     * 子subBucket
     */
    private Map<byte[], Bucket> subBucket = new HashMap<>();

    /**
     * inline page reference
     */
    @Setter
    private Page page;

    @Setter
    private Node rootNode;

    private Map<Long, Node> nodes;

    private float fillPercent;


    public Bucket(Tx tx){
        this.rootPgid = tx.getMeta().getRoot().getRootPgid();
        this.sequence = tx.getMeta().getRoot().getSequence();
    }

    public Bucket(){}

    public static Bucket createEmptyBucket(){
        Bucket bucket = new Bucket();
        bucket.rootPgid = 0;
        bucket.sequence = 0;

        bucket.rootNode = Node.builder().leaf(true).build();
        bucket.fillPercent = DefaultFillPercent;
        return bucket;
    }



    public Bucket createBucket(byte[] key) {

        //check
        assert this.getTx().getDb() != null;
        assert this.getTx().isWriteable();
        assert key != null && key.length > 0;

        Cursor c = this.cursor();

        Cursor.CursorResult cursorResult = c.seek(key);

        /**
         * 如果找到对应的key,则抛出异常
         */
        if(Arrays.equals(key, cursorResult.getKey())){
            if((cursorResult.getFlags() & PageFlag.bucketLeafFlag) == 1){
                throw new RuntimeException("bucket is aleady exist");
            }
            throw new RuntimeException("incompatible value");
        }


        //根据 key 创建一个新的bucket
        Bucket bucket = Bucket.createEmptyBucket();
        byte[] value = bucket.write();

        c.node().put(key, key, value, 0, PageFlag.bucketLeafFlag);

        return this.findBucket(key);

    }

    private Bucket findBucket(byte[] key) {
        //如果之前创建过，则直接返回
        if(!this.subBucket.isEmpty()){
            Bucket bucket = subBucket.get(key);
            if(bucket != null){
                return bucket;
            }
        }

        Cursor c = this.cursor();

        Cursor.CursorResult cursorResult = c.seek(key);

        if(!Arrays.equals(key, cursorResult.getKey()) || (cursorResult.getFlags()&PageFlag.bucketLeafFlag) == 0){
            return null;
        }

        Bucket child = this.openBucket(cursorResult.getValue());
        this.subBucket.put(key, child);
        return child;

    }

    /**
     *  根据value值，序列化成bucket
     * @param value
     * @return
     */
    private Bucket openBucket(byte[] value) {

        //TODO check heap byte buf?
        ByteBuf byteBuf = Unpooled.copiedBuffer(value);

        Bucket child = Bucket.builder().tx(this.tx).fillPercent(fillPercent).build();
        if(this.getTx().isWriteable()){
            child.subBucket = new HashMap<>();
            child.nodes = new HashMap<>();
        }

        /**
         * TODO
         *  If this is a writable transaction then we need to copy the bucket entry.
         *  Read-only transactions can point directly at the mmap entry.
         */
        if(this.tx.isWriteable()){
            child.copyFromValue(byteBuf);
        }else{
            child.copyFromValue(byteBuf);
        }

        //inline bucket
        if(child.getRootPgid() == 0){
            child.setPage(new Page(byteBuf.slice(BucketHeaderSize, byteBuf.capacity()/** TODO ?**/)));
        }
        return child;
    }

    private void copyFromValue(ByteBuf value) {
        this.rootPgid = value.readLong();
        this.sequence = value.readLong();
    }

    /**
     *
     * @return
     */
    private Cursor cursor() {
        return new Cursor(this);
    }

    /**
     * 将此bucket，序列化成 byte字节数组
     * @return
     */
    private byte[] write() {

        Node node = this.rootNode;

        int size = node.size();

        ByteBuf value = Unpooled.buffer(BucketHeaderSize + size);
        Page page = new Page(value);
        /**
         * TODO 设置bucket的内容到page中
         *      1. 设置 rootpgid，设置 seq
         *      2. 设置 bucket的 inode内容进去
         */
        page.setPgid(0);
        page.setFlag((short)0);
        page.setCount((short) 0);
        page.setOverflow(0);

        page.writeLong(this.rootPgid);
        page.writeLong(this.sequence);

        node.write(page);

        return page.toBytebuf();
    }

    public void put(byte[] key, byte[] value){

    }
}
