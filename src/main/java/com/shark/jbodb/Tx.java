package com.shark.jbodb;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

import static com.shark.jbodb.DB.PAGE_SIZE;

@Getter
public class Tx {

    private boolean writeable;

    @Setter
    private boolean managed;

    private DB db;

    private Meta meta;

    private Bucket root;

    private Map<Long, Page> pages;


    /**
     * 提交事务
     */
    public void commit(){

        assert this.getDb() !=  null;
        assert this.isWriteable();
        assert this.getDb().isOpened();

        //bucket rebalance
        this.getRoot().rebalance();

        //node spill
        this.getRoot().spill();

        /**
         * Free the old root bucket.
         * spill 之后由于mvcc的策略，可能会构造新的根节点，需要将其复制给 meta
         */

        this.getMeta().getRoot().setRootPgid(this.getRoot().getRootPgid());
        this.getMeta().getRoot().setSequence(this.getRoot().getSequence());


        long oldPgid = this.getMeta().getPgid();

        /**
         * 释放原来的 freelist 的page？
         * 申请指定数量的page，用于写入freelist的内容到新的page里面
         *
         * why ？
         */
        this.getDb().getFreeList().free(this.getMeta().getTxid(), this.getDb().page(this.getMeta().getFreeListPgid()));
        Page freeListPage = this.allocate(this.getDb().getFreeList().size()/PAGE_SIZE);
        this.getDb().getFreeList().write(freeListPage);
        this.getMeta().setFreeListPgid(freeListPage.getPgid());

        if(this.getMeta().getPgid() > oldPgid){
            /**
             * DB 数据文件扩容
             */
            this.getDb().grow(this.getMeta().getPgid() + 1);
        }

        /**
         * 将事务中的脏数据写入文件
         */
        this.write();

        /**
         * 检查一致性
         */
        this.check();

        /**
         * 写入元数据
         */
        this.writeMeta();


        /**
         * 事务关闭
         */
        this.close();
    }

    private void close() {
    }

    private void writeMeta() {
    }

    private void check() {
    }

    private void write() {
    }

    public Page allocate(int n) {
        return null;
    }

    /**
     * 回滚事务
     */
    public void rollback(){

    }


    public Bucket createBucket(byte[] name){
        return this.root.createBucket(name);
    }

    /**
     * 初始化事务
     * @param db
     */
    public void init(DB db, boolean writeable) {
        this.writeable = writeable;
        this.db = db;

        this.meta = db.meta().copy();

        this.root = new Bucket(this);

        if(writeable){
            this.pages = new HashMap<>();
            this.meta.setTxid(meta.getTxid() + 1);
        }
    }

    //TODO
    public Page getPage(long pgid) {
        return null;
    }
}
