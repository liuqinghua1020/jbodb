package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;

import java.util.Map;

@Getter
@Builder
public class Bucket {

    private long rootPgid;

    private long sequence;

    //对应的事务
    private Tx tx;

    /**
     * 子subBucket
     */
    private Map<String, Bucket> subBucket;

    /**
     * inline page
     */
    private Page page;

    private Node rootNode;

    private Map<Long, Node> nodes;


    public Bucket(Tx tx){

    }

}
