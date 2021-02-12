package com.shark.jbodb;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;

@Data
@Builder
public class Config {

    @Getter
    private String dataDir;

}
