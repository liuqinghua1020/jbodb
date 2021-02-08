package com.shark.jbodb;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
public class Config {

    public static final int PAGE_SIZE = 1024 * 1024 * 4; //4K

    @Getter
    private String dataDir;

}
