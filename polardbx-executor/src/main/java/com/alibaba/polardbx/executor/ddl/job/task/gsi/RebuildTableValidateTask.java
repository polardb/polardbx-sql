package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import lombok.Getter;

import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RebuildTableValidateTask")
public class RebuildTableValidateTask extends AlterPartitionCountValidateTask {
    public RebuildTableValidateTask(String schemaName, String primaryTable,
                                    Map<String, String> tableNameMap,
                                    List<TableGroupConfig> tableGroupConfigList) {
        super(schemaName, primaryTable, tableNameMap, tableGroupConfigList);
    }
}
