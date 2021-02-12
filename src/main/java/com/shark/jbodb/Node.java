package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;

/**
 * 一个page 映射到内存的结构
 */
@Getter
@Builder
public class Node {


    private Bucket bucket;

    private boolean leaf;

    private boolean unbalanced;

    private boolean spilled;

    private byte[] key;

    private long pgid;

    private Node parent;

    private Node[] children;
    /**
     *
     */
    private Entry[] entries;

    //在本节点插入Key和Value
    public void put(byte[] oldKey, byte[] newKey, byte[] value, long pgid, int bucketLeafFlag) {
    }

    /**
     * 返回一个 page header + inode 内容的大小的总共字节数
     * @return
     */
    public int size() {
        return 0;
    }

    /**
     *  将 node 序列化到 page中,主要是将 node中的inode信息序列化到page里面
     */

    public void write(Page page) {
    }
}
