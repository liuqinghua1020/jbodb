package com.shark.jbodb;

/**
 *
 *  |         page header             |
 *   pageid | flag | count | overflow |  ele1   |  ele2   | k1|v1 | k2|v2 |
 *                                    |                   |
 */
public class Page {

    //page编号
    private long pgid;
    //page的类型，参考 @linker PageFlag
    private short flag;
    //本页面包含的 page count
    private short count;
    // ？
    private int overflow;
    //具体数据的位置
    private long offset;

}
