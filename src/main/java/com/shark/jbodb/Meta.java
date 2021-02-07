package com.shark.jbodb;

public class Meta {

    private int magic;

    private int version;

    private int pageSize;

    private int flags;

    private Bucket root;

    private long freeListPgid;

    private long pgid;

    private long txid;

    private long checksum;

}
