package com.shark.jbodb;

import java.nio.charset.StandardCharsets;

public class SerializeUtil {

    public byte[] str2Bytes(String str){
        return str.getBytes(StandardCharsets.UTF_8);
    }

}
