package com.shark.jbodb;

/**
 * 一个page 映射到内存的结构
 */
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

}
