package com.shark.jbodb;

import io.netty.buffer.ByteBuf;

import java.util.*;

import static com.shark.jbodb.Page.PAGEHEADERSIZE;

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

    private List<Long> pgids;

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

    private void reIndex() {

        //?
    }

    public void release(long txid) {
        //TODO
    }

    //根据 freeid 释放对应的page
    public void free(long txid, Page page) {
    }

    public int size() {
        return -1;
    }

    public void write(Page page) {
    }
}
