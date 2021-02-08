package com.shark.jbodb;

public class PageFlag {

    /**
     * Page flag
     */
    public static final byte BranchPageFlag = 0x01;
    public static final byte LeafPageFlag = 0x02;
    public static final byte MetaPageFlag = 0x03;
    public static final byte FreelistPageFlag = 0x10;

    /**
     * Page element flag
     */
    public static final short normalLeafFlag = 0x00;
    public static final short bucketLeafFlag = 0x01;

}
