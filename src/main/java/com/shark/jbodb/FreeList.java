package com.shark.jbodb;

import java.util.Map;

public class FreeList {

    private long[] pgids;

    private Map<Long/** txid **/, Long/** pgid**/ > pending;

    private Map<Long/** pgid **/, Boolean> cache;
}
