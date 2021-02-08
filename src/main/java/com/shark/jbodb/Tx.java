package com.shark.jbodb;

import java.util.HashMap;
import java.util.Map;

public class Tx {
    private boolean writeable;

    private boolean managed;

    private DB db;

    private Meta meta;

    private Bucket root;

    private Map<Long, Page> pages;


    /**
     * 提交事务
     */
    public void commit(){

    }

    /**
     * 回滚事务
     */
    public void rollback(){

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
}
