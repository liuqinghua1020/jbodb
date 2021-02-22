package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class Entry {

    private int flags;

    private long pgid;

    private byte[] key;

    private byte[] value;

}
