package com.shark.jbodb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DB {

    public static final int PAGE_SIZE = 1024 * 1024 * 4; //4K;

    //TODO 是否需要换成 字节数组，直接写入字节数组
    public static final int MAGIC = 0xED0CDAED;

    public static final int VERSION = 2;

    private String dataDir;

    private Storage storage;

    private Meta meta0;

    private Meta meta1;

    @Getter
    private volatile boolean opened;

    @Getter
    private FreeList freeList;

    private Tx rwtx;

    private List<Tx> txs;

    private Config config;

    private ReentrantReadWriteLock rwLock ;

    private volatile ReentrantReadWriteLock.WriteLock writeLock ;

    private volatile ReentrantReadWriteLock.ReadLock readLock;

    private ReentrantLock metaLock;

    private DB(Config config){
        this.config = config;
        this.dataDir = config.getDataDir();
        this.rwLock = new ReentrantReadWriteLock();
        this.writeLock = rwLock.writeLock();
        this.readLock = rwLock.readLock();
        this.metaLock = new ReentrantLock();

        txs = new CopyOnWriteArrayList<>();
    }

    public void open(){
        //TODO 加锁
        if(opened){
            return;
        }
        storage = new FileStorage(this.dataDir);
        storage.init();

        /**
         * 创建或加载数据库
         */
        if(storage.isNewCreate()){
            this.init();
        }else{
            this.load();
        }

        //从Storage中加载meta0和meta1
        this.meta0 = Meta.loadFromPage(storage.page(0));
        this.meta1 = Meta.loadFromPage(storage.page(1));

        //从Storage中加载freelist
        this.freeList = FreeList.loadFromPage(storage.page(this.meta().getPgid()));
        opened = true;
    }

    /**
     *  新的数据库，完成功能：
     *    1. 获取 os pageSize
     *    2. 初始化 2个 meta page
     *    3. 初始化 freelist page
     *    4. 强制刷磁盘
     */
    private void init() {
        int pageSize = PAGE_SIZE;

        //初始化 4k 的 buffer
        //ByteBuf buffer = Unpooled.buffer(4* pageSize);
        ByteBuf buffer = storage.slice(0, 4*pageSize);
        //初始化两个meta
        for(int i=0;i<2;i++){
            Page page = new Page(buffer.slice(i*pageSize, pageSize));
            page.setPgid(i);
            page.setFlag(PageFlag.MetaPageFlag);
            page.setCount((short)0);
            page.setOverflow(0);

            Meta meta = Meta.toMeta(page);
            meta.setMagic(DB.MAGIC);
            meta.setVersion(DB.VERSION);
            meta.setPageSize(pageSize);
            meta.setFlags(0);

            meta.setRoot(Bucket.builder().rootPgid(3).sequence(0).build());
            meta.setFreeListPgid(2);

            meta.setPgid(4);
            meta.setTxid(i);
            meta.computeCheckSum();

        }


        //初始化 freelist，初始化的时候是一个空的 freelist
        Page freeListPage = new Page(buffer.slice(2*pageSize, pageSize));
        freeListPage.setPgid(2);
        freeListPage.setFlag(PageFlag.FreelistPageFlag);
        freeListPage.setCount((short)0);
        freeListPage.setOverflow(0);

        Page emptyLeafPage = new Page(buffer.slice(3*pageSize, pageSize));
        emptyLeafPage.setPgid(3);
        emptyLeafPage.setFlag(PageFlag.LeafPageFlag);
        emptyLeafPage.setCount((short)0);
        emptyLeafPage.setOverflow(0);

        //强制刷磁盘
        storage.sync();
    }

    /**
     * 加载已经存在的数据库，完成如下功能：
     * 加载开始四页
     */
    private void load(){
        //TODO
    }


    /**
     * 一个更新业务
     * @param dbHandle
     */
    public void update(DBHandle dbHandle){
        Tx tx = this.begin(true);

        try {

            tx.setManaged(true);

            dbHandle.handle(tx);

            tx.setManaged(false);

            tx.commit();
        } catch (Exception e) {
            e.printStackTrace();
            tx.rollback();
        }
    }


    private Tx begin(boolean writeable) {
        if(writeable){
            return beginRWTx();
        }

        return beginTx();
    }

    private Tx beginRWTx() {

        //获取写锁
        writeLock.lock();
        try {
            //获取 meta 锁
            metaLock.lock();

            if(!opened){ //数据库没有打开成功
                writeLock.unlock();
                throw new RuntimeException("db not opened");
            }

            Tx tx = new Tx();
            tx.init(this, true);


            this.rwtx = tx;

            long miniTxid = Long.MAX_VALUE;
            for(Tx t : this.txs){
                if(t.getMeta().getTxid() < miniTxid){
                    miniTxid = t.getMeta().getTxid();
                }
            }

            if(miniTxid > 0){
                this.freeList.release(miniTxid - 1);
            }

            return tx;
        }finally {
            //释放meta锁
            metaLock.unlock();
        }


    }

    private void addTx(Tx tx) {
        this.txs.add(tx);
    }

    private Tx beginTx() {
        //获取写锁
        writeLock.lock();
        try {
            //获取 meta 锁
            metaLock.lock();

            if(!opened){ //数据库没有打开成功
                writeLock.unlock();
                throw new RuntimeException("db not opened");
            }

            Tx tx = new Tx();
            tx.init(this, true);


            this.addTx(tx);


            return tx;

        }finally {
            //释放meta锁
            metaLock.unlock();
        }
    }


    /**
     * 一个查询业务
     * @param dbHandle
     */
    public void view(DBHandle dbHandle){

    }

    public Meta meta(){
        Meta metaA = meta0;
        Meta metaB = meta1;
        if(meta1.getTxid() > meta0.getTxid()){
            metaA = meta1;
            metaB = meta0;
        }

        try{
            metaA.validate();
            return metaA;
        }catch (Exception e){
        }

        try{
            metaB.validate();
            return metaB;
        }catch (Exception e){
        }

        //TODO throw Exception ,system exit
        return null;
    }

    /**
     * 根据pageid获取
     * @param pgid
     * @return
     */
    public Page page(long pgid) {
        return null;
    }

    /**
     * 数据文件扩容
     * @param size
     */
    public void grow(long size) {
    }
}
