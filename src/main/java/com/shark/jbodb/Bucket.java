package com.shark.jbodb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static com.shark.jbodb.DB.PAGE_SIZE;
import static com.shark.jbodb.LeafPageElement.leafPageElementSize;
import static com.shark.jbodb.Page.PAGEHEADERSIZE;
import static com.shark.jbodb.PageFlag.bucketLeafFlag;

/**
 * 在 jbodb概念中，
 * bucket是一个子树，里面包含完整的 k/v 结构
 * 如果bucket的内容很小，可以将整个bucket作为一个结构，放到上层的一个bucket的k/v的v中，k为bucket的name
 * 如果bucket比较大， 则只是将bucket的root pgid 作为一个k放到 上层bucket的 k/v的v中。
 *
 * 整个jodb就是一个完整的匿名的bucket，每一个构建都会将根作为一个匿名的bucket进行处理
 */
@Getter
public class Bucket {

    public static final int BucketHeaderSize = 8 /** rootPgid **/ + 8 /** sequence **/;

    public static final float DefaultFillPercent = 0.5f;

    @Setter
    private long rootPgid;

    @Setter
    private long sequence;

    /**
     *  内存中表示的这个bucket，是属于哪一个tx的
     *  每一个tx都从文件中加载组件自己的内存bucket
     */
    private Tx tx;

    /**
     * 子subBucket
     * 从事务加载起来的 bucket，以事务作为区分
     * 从持久层中加载了之后，内存中就是每一个事务都不一样的了，可以做到mvcc
     *
     */
    private Map<byte[], Bucket> subBucket = new HashMap<>();

    /**
     * inline page reference
     * 这个只有本bucket是内联bucket才有效，否则就是空
     */
    @Setter
    private Page page;

    /**
     * 此bucket对应的root node
     */
    @Setter
    private Node rootNode;

    /**
     * 从事务加载起来的 bucket，以事务作为区分
     * 每一个事务从 持久层加载之后，内存中就是每一个事务都不一样的了，可以做到mvcc
     */
    private Map<Long, Node> nodes;

    private float fillPercent;

    private Bucket(){}

    public static Bucket newBucket(Tx tx){
        assert tx != null;
        Bucket bucket = new Bucket();
        bucket.tx = tx;
        bucket.fillPercent = DefaultFillPercent;

        if(tx.isWriteable()){
            bucket.subBucket = new HashMap<>();
            bucket.nodes = new HashMap<>();
        }

        return bucket;
    }

    public static Bucket newEmptyBucket(){
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
            if((cursorResult.getFlags() & bucketLeafFlag) == 1){
                throw new RuntimeException("bucket is aleady exist");
            }
            throw new RuntimeException("incompatible value");
        }


        //根据 key 创建一个新的bucket
        Bucket bucket = Bucket.newEmptyBucket();
        byte[] value = bucket.write();

        c.node().put(key, key, value, 0, bucketLeafFlag);

        return this.findBucket(key);

    }



    /**
     *
     * @return
     */
    private Cursor cursor() {
        return new Cursor(this);
    }



    /**
     * Delete removes a key from the bucket.
     * @param key
     */
    public void delete(byte[] key){
        //TODO
    }

    public void put(byte[] key, byte[] value){
        //TODO
        Cursor cursor = this.cursor();
        Cursor.CursorResult cursorResult = cursor.seek(key);

        if(Arrays.equals(cursorResult.key, key) && (cursorResult.getFlags() & bucketLeafFlag) != 0){
            throw new RuntimeException("ErrIncompatibleValue");
        }

        cursor.node().put(key, key, value, 0, 0);
    }

    public byte[] get(byte[] key){
        Cursor c = this.cursor();
        Cursor.CursorResult cursorResult = c.seek(key);

        if((cursorResult.getFlags() & bucketLeafFlag) != 0){
            throw new RuntimeException("ErrIncompatibleValue");
        }

        if(!Arrays.equals(cursorResult.key, key)){
            throw new RuntimeException("ErrIncompatibleValue");
        }

        return cursorResult.value;

    }

    // node creates a node from a page and associates it with a given parent.
    public Node createNode(long pgid, Node parent){

        // Retrieve node if it's already been created.
        Node n = this.nodes.get(pgid);
        if(n != null){
            return n;
        }

        // Otherwise create a node and cache it.
        Node node = Node.builder().bucket(this).parent(parent).build();
        if(parent == null){
            this.rootNode = node;
        }else{
            parent.getChildren().add(node);
        }

        // Use the inline page if this is an inline bucket.
        Page p = this.page;
        if(page == null){
            p = this.getTx().getPage(pgid);
        }

        // Read the page into the node and cache it.
        node.read(p);
        this.nodes.put(pgid, node);

        return node;
    }

    //再平衡
    public void rebalance() {
        for(Node node:this.nodes.values()){
            node.rebalance();
        }

        for(Bucket child:this.subBucket.values()){
            child.rebalance();
        }
    }

    /**
     *  节点分裂
     *  自底向上进行分裂操作
     */
    public void spill() {

        for(Map.Entry<byte[],Bucket> entry:this.getSubBucket().entrySet()){

            byte[] key = entry.getKey();
            Bucket child = entry.getValue();

            byte[] value;
            /**
             * 如果一个bucket足够小，小到可以将一整个bucket的内容放到一个page里面，并且没有任何子bucket
             * 则考虑直接将其放到一个page里面
             */
            if(child.inLineable()){
                child.free();
                value = child.write();
            }else{
                child.spill();
                /**
                 * 构造足够多的bucket序列化的内容
                 * 主要是两个字段：
                 *  rootpgid long 8个字节
                 *  seq      long 8个字节
                 */
                value = new byte[8+8];
                SerializeUtil.setBytes(this.getRootPgid(), value, 0);
                SerializeUtil.setBytes(this.getRootPgid(), value, 8);
            }


            /**
             * 如果此bucket没有rootnode，则直接continue
             * 感觉像是防御式编程
             */
            if(child.getRootNode() == null){
                continue;
            }


            /**
             * spill之后，重新将新的bucket root pgid 插入到当前的之中。
             */
            Cursor cursor = this.cursor();
            Cursor.CursorResult cursorResult = cursor.seek(key);


            if(!Arrays.equals(cursorResult.key, key)){
                throw new RuntimeException("ErrIncompatibleValue");
            }

            if((cursorResult.getFlags() & bucketLeafFlag) == 0){
                throw new RuntimeException("ErrIncompatibleValue");
            }
            cursor.node().put(key, key, value, 0, bucketLeafFlag);
        }


        //到达最后一层的bucket要处理的逻辑
        //防御式编程
        if(this.getRootNode() == null){
            return ;
        }

        //本bucket的rootnode进行分裂处理
        this.getRootNode().spill();
        this.setRootNode(this.getRootNode().root());

        // Update the root node for this bucket.
        if(this.getRootNode().getPgid() >= this.getTx().getMeta().getPgid()){
            throw new RuntimeException(String.format("pgid (%d) above high water mark (%d)",
                    this.getRootNode().getPgid(), this.getTx().getMeta().getPgid()));
        }
        this.setRootPgid(this.getRootNode().getPgid());
    }


    /**
     * 一个内存中的bucket的 和一个具体的事务绑定，
     * 事务生命周期终止，此事务对应的bucket也消亡
     * @param key
     * @return
     */
    // Bucket retrieves a nested bucket by name.
    // The bucket instance is only valid for the lifetime of the transaction.
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

        //查到了一个相同key的，但是类型不是bucket的，直接返回
        if(!Arrays.equals(key, cursorResult.getKey()) || (cursorResult.getFlags()& bucketLeafFlag) == 0){
            return null;
        }

        Bucket child = this.openBucket(cursorResult.getValue());

        //放入缓存，以后在此tx中操作次bucket，就是直接操作内存里面的了。
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

        Bucket child = newBucket(this.tx);
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
            //TODO 这一个可以随意修改
            child.copyFromValue(byteBuf);
        }else{
            //不能修改
            child.copyFromValue(byteBuf);
        }

        //inline bucket
        if(child.getRootPgid() == 0){
            child.setPage(Page.createPageFromByteBuf(byteBuf.slice(BucketHeaderSize, byteBuf.capacity()/** TODO ?**/)));
        }
        return child;
    }

    private void copyFromValue(ByteBuf value) {
        this.rootPgid = value.readLong();
        this.sequence = value.readLong();
    }


    /**
     * 将此bucket，序列化成 byte字节数组
     * @return
     */
    private byte[] write() {

        Node node = this.rootNode;

        int size = node.size();


        Page page = Page.createNewPage(BucketHeaderSize + size);
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

    /**
     * 释放资源的操作
     * 主要是将其page放到 freelist里面
     *
     *
     * free recursively frees all pages in the bucket.
     */
    private void free() {

        if(this.rootPgid == 0){
            return ;
        }

        Tx tx = this.getTx();

        //inline bucket的情况
        if(this.page != null){

            tx.getDb().getFreeList().free(tx.getMeta().getTxid(), page);

            return;
        }

        //TODO 从 bucket的 root 开始，往下释放

        this.rootPgid = 0;

    }

    /**
     * 是否可以将此bucket放到一个page
     * @return
     */
    private boolean inLineable() {
        Node node = this.getRootNode();

        //此node已经不是叶子了，可以直接返回false了
        if(node == null || !node.isLeaf()){
            return false;
        }

        int size = PAGEHEADERSIZE; /** page header sise */

        for(Entry entry:node.getEntries()){
            size += leafPageElementSize + entry.getKey().length + entry.getValue().length;

            if((entry.getFlags() & bucketLeafFlag) != 0){
                return false;
            }else if(size > this.maxInlineBucketSize()){
                return false;
            }
        }
        return true;
    }

    private int maxInlineBucketSize() {
        return PAGE_SIZE / 4;
    }
}



















