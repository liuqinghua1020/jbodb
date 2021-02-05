package com.shark.jbodb;

public class PageFlag {

    /**
     * Page flag
     */
    private static final byte BranchPageFlag = 0x01;
    private static final byte LeafPageFlag = 0x02;
    private static final byte MetaPageFlag = 0x03;
    private static final byte FreelistPageFlag = 0x10;

    /**
     * Page element flag
     */
    public static final short normalLeafFlag = 0x00;
    public static final short bucketLeafFlag = 0x01;

}
