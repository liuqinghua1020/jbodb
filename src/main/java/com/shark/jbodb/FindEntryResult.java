package com.shark.jbodb;

import lombok.Builder;
import lombok.Getter;

/**
 * 用于在
 *   Page
 *   Node
 *   中
 *   查看key的内容
 */
@Builder
@Getter
public class FindEntryResult {

    /**
     * 属于page/node中的哪一条记录
     */
    private int index;
    /**
     * 是否精确匹配
     */
    private boolean exact;

}
