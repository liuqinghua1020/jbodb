package com.shark.jbodb;

import lombok.Getter;

public class LeafPageElement {

    public static final int leafPageElementSize = 4 /** flag 4 byte**/ + 4 + 4 + 4;

    /**
     *  0 表示 普通的value值
     *  1 表示 value是一个bucket， @
     */
    @Getter
    private int flag;

    /**
     * 参展 page的物理格式分布，指明 一个element 的 真实 k/v 的起始位置
     */
    private int pos;

    /**
     * k的长度
     */
    private int kSize;

    /**
     * v的长度
     */
    private int vSize;

    public byte[] getKey(){
        //TODO
        return null;
    }

    public byte[] getValue(){
        //TODO
        return null;
    }


}
