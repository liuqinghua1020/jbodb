package com.shark.jbodb;

import java.util.Map;

public class Tx {
    private boolean writeable;

    private boolean managed;

    private DB db;

    private Meta meta;

    private Bucket root;

    private Map<Long, Page> pages;

}
