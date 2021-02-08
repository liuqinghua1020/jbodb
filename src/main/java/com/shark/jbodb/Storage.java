package com.shark.jbodb;

import io.netty.buffer.ByteBuf;

public interface Storage {

    void init();


    boolean isNewCreate();


    ByteBuf slice(int position, int length);

    void sync();

    Page page(long pgid);
}
