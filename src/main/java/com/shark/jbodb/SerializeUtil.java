package com.shark.jbodb;

import java.nio.charset.StandardCharsets;

public class SerializeUtil {

    public static byte[] str2Bytes(String str){
        return str.getBytes(StandardCharsets.UTF_8);
    }
    

    //将number填充到offset开始的bytes字节数组中
    public static void setBytes(long number, byte[] bytes, int offet){
        //TODO
    }

}
