package com.shark.jbodb;

import io.netty.buffer.ByteBuf;

import java.util.*;

import static com.shark.jbodb.Page.PAGEHEADERSIZE;
import static com.shark.jbodb.PageFlag.FreelistPageFlag;

/**
 * 在内存中表示时分为两部分，一部分是可以分配的空闲页列表 ids，另一部分是按事务 id 分别记录了在对应事务期间新增的空闲页列表。
 *
 * 其中 pending 部分需要单独记录主要是为了做 MVCC 的事务：
 *
 *     写事务回滚时，对应事务待释放的空闲页列表要从 pending 项中删除。
 *     某个写事务（比如 txid=7）已经提交，但可能仍有一些读事务（如 txid <=7）仍然在使用其刚释放的页，因此不能立即用作分配。
 *
 * 物理结构如下
 *   pgid | flags | count | overflow | count | pgid1(8个字节) | pgid2(8个字节) | pgid3(8个字节) ...
 *   |         page header                   |              count 个 8个字节                     |
 *
 */
public class FreeList {

    /**
     * 记录的空闲页面
     */
    private List<Long> pgids;

    /**
     * 记录 每一个事务（主要是写事物）准备撤销的页面，在下次事务启动时会将其加入 pgids
     */
    private Map<Long/** txid **/, List<Long/** pgid**/>> pending;

    private Map<Long/** pgid **/, Boolean> cache;


    private FreeList(){
        pending = new HashMap<>();
        cache = new HashMap<>();
    }



    public static FreeList loadFromPage(Page page) {
        FreeList freeList = new FreeList();

        int count = page.getCount();

        if(count == 0){
            freeList.pgids = null;
        }else{
            freeList.pgids = new ArrayList<>(count);
            ByteBuf byteBuf = page.slice(PAGEHEADERSIZE, count * 8/** 一个pageid 的字节数**/);
            for(int i=0;i<count;i++){
                long pgid = byteBuf.readLong();
                freeList.pgids.add(pgid);
            }
            //sort ，便于查找连续的 TODO 是否可以用 伙伴算法 优化
            Collections.sort(freeList.pgids);
        }

        freeList.reIndex();

        return freeList;
    }


    /**
     * 从空闲列表中申请n个连续的page，如果申请失败，则返回 -1
     * @param n
     * @return
     */
    public long allocate(int n){
        //TODO
        return -1;
    }

    // reindex rebuilds the free cache based on available and pending free lists.
    private void reIndex() {
        this.cache = new HashMap<>();
        for(long pgid:this.pgids){
            this.cache.put(pgid, true);
        }

        for(List<Long> pendingPgIds:this.pending.values()){
            for(Long pgid:pendingPgIds){
                this.cache.put(pgid, true);
            }
        }
    }

    // moves all page ids for a transaction id (or older) to the freelist.
    public void release(long txid) {
        List<Long> pgids = new ArrayList<>();
        Iterator<Map.Entry<Long, List<Long>>> it = this.pending.entrySet().iterator();
        while(it.hasNext()){
             Map.Entry<Long, List<Long>> mapEntry = it.next();
             long tid = mapEntry.getKey();
             if(tid <= txid){
                 pgids.addAll(mapEntry.getValue());
             }

        }
        Collections.sort(pgids);

        //TODO
        this.mergeIds(pgids);

    }


    //根据 freeid 释放对应的page(可能不知一个page，具体查看首个page的 overflow 属性)
    public void free(long txid, Page page) {

        //cannot free page 0 or 1
        assert page.getPgid() > 1;

        List<Long> ids = this.pending.get(txid);
        for(long id = page.getPgid(); id <= page.getPgid() + page.getOverflow(); id++){
            // Verify that page is not already free.
            if(this.cache.get(id)){
                throw new RuntimeException(String.format("page %d already freed", id));
            }

            ids.add(id);
            this.cache.put(id, true);
        }

        this.pending.put(txid, ids);

    }

    public int size() {
        int n = this.count();
        if(n >= 0xffff){
            n ++;
        }
        return PAGEHEADERSIZE + (8 /** pgid size bytes **/ * n);
    }

    public int count(){
        return this.pgids.size() + this.pending.size();
    }

    // write writes the page ids onto a freelist page. All free and pending ids are
    // saved to disk since in the event of a program crash, all pending ids will
    // become free.
    public void write(Page page) {

        page.setFlag(((short)(page.getFlag() | FreelistPageFlag)));

        int lenids = this.count();
        if(lenids == 0){
            page.setCount((short)0);
        }else if(lenids < 0xffff){
            page.setCount((short)lenids);
            //TODO 将 ids 和 pendings 写入 page的中

        }else{
            page.setCount((short)0xffff);
            //TODO 将 lenids 写入 page中
            //TODO 将 ids 和 pendings 写入 page 的中
        }
    }

    public int pendingCount(){
        int count = 0;
        Collection<List<Long/** pgid**/>> collection = this.pending.values();
        for(List l:collection){
            count += l.size();
        }

        return count;

    }

    // rollback removes the pages from a given pending tx.
    public void rollback(long txid){
        // Remove page ids from cache.
        for(long pgid:this.pending.get(txid)){
            this.cache.remove(pgid);
        }
        // Remove pages from pending list.
        this.pending.remove(txid);

    }

    public int freeCount(){
        return this.pgids.size();
    }




    /**
     *   合并操作
     *   难点在于得到更多的 连续页
     */
    private void mergeIds(List<Long> pgids) {
        //TODO
    }
}
