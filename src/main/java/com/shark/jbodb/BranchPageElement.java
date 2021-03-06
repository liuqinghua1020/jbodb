package com.shark.jbodb;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class BranchPageElement {

    public static final int branchPageElementSize = 4 /** flag 4 byte**/ + 4 + 4;


    /**
     * 参展 page的物理格式分布，指明 一个element 的 真实 k 的起始位置
     */
    private int pos;

    /**
     * k的长度
     */
    private int kSize;

    /**
     * pgid
     */
    private long pgid;

}
