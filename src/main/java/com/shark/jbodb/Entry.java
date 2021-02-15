package com.shark.jbodb;

import lombok.Getter;

@Getter
public class Entry {

    private int flags;

    private long pgid;

    private byte[] key;

    private byte[] value;

}
