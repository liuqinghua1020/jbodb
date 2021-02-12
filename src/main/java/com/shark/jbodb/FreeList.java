package com.shark.jbodb;

import io.netty.buffer.ByteBuf;

import java.util.*;

import static com.shark.jbodb.Page.CONTENT_OFFSET;

/**
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
            ByteBuf byteBuf = page.slice(CONTENT_OFFSET, count * 8/** 一个pageid 的字节数**/);
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
