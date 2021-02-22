package com.shark.jbodb;

import lombok.Getter;

public class Meta {

    private int magic;

    private int version;

    private int pageSize;

    private int flags;

    // 各个子 bucket 根所组成的树
    @Getter
    private Bucket root;

    @Getter
    //指向最新的 freeList page
    private long freeListPgid;

    // 当前用到的最大 page id，mvcc的界限
    @Getter
    private long pgid;

    @Getter
    private long txid;

    private long checksum;

    private Page page;

    public Meta(Page page){
        this.page = page;
    }


    public void setMagic(int magic){
        this.magic = magic;
        page.writeInt(magic);
    }

    public void setVersion(int version){
        this.version = version;
        page.writeInt(version);
    }

    public void setPageSize(int pageSize){
        this.pageSize = pageSize;
        page.writeInt(pageSize);
    }

    public void setFreeListPgid(long freeListPgid){
        this.freeListPgid = freeListPgid;
        page.writeLong(freeListPgid);
    }

    public void setFlags(int flags){
        this.flags = flags;
        page.writeInt(flags);
    }

    public void setPgid(long pgid){
        this.pgid = pgid;
        page.writeLong(pgid);
    }

    public void setTxid(long txid){
        this.txid = txid;
        page.writeLong(txid);
    }

    public void computeCheckSum(){
        //TODO
        long sum = 0;
        page.writeLong(sum);
    }

    public void setRoot(Bucket bucket){
        this.root = bucket;
        page.writeLong(bucket.getRootPgid());
        page.writeLong(bucket.getSequence());
    }

    public static Meta toMeta(Page page){
        return new Meta(page);
    }

    public static Meta loadFromPage(Page page){
        //TODO
        return null;
    }

    public void validate() throws Exception{
        //验证 mete 是否完整
    }

    /**
     * copy 内容到 一个新的meta中，copy的内容包括
     * 1
     * @return
     */
    public Meta copy() {
        return null;
    }
}
