package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * @author wumu
 */
@Getter
@Setter
public class AlterTableArchivePartitionPreparedData {
    String primaryTableSchema;

    String primaryTableName;

    String tmpTableSchema;

    String tmpTableName;

    List<String> phyPartitionNames;

    List<String> firstLevelPartitionNames;

}
