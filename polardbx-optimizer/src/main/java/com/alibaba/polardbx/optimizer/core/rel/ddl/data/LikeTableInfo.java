package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-27 10:59
 **/
@NoArgsConstructor
@AllArgsConstructor
@Data
public class LikeTableInfo {
    private String schemaName;
    private String tableName;
}
