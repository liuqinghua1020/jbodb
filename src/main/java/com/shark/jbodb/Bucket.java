package com.shark.jbodb;

import java.util.Map;

public class Bucket {

    private long rootPgid;

    private long sequence;

    private Tx tx;

    private Map<String, Bucket> subBucket;

    /**
     * inline page
     */
    private Page page;

    private Node rootNode;

    private Map<Long, Node> nodes;

}
