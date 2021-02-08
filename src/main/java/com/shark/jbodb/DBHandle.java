package com.shark.jbodb;

public interface DBHandle {

    void handle(Tx tx) throws Exception;

}
