package com.shark.jbodb;

import java.util.concurrent.atomic.AtomicBoolean;

public class DB {

    private String dataDir;

    private Storage storage;

    private Meta meta0;

    private Meta meta1;

    private volatile AtomicBoolean opened;

    private FreeList freeList;

    private Tx rwtx;

    private Tx[] txs;

    public DB(String dataDir){
        this.dataDir = dataDir;
    }

    public void open(){

    }

}
