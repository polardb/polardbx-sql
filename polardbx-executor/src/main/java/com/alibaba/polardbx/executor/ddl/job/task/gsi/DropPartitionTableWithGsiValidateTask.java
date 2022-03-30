package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

@Getter
@TaskName(name = "DropPartitionTableWithGsiValidateTask")
public class DropPartitionTableWithGsiValidateTask extends BaseValidateTask {

    protected final String primaryTable;
    protected final List<String> indexNames;
    protected final List<TableGroupConfig> tableGroupConfigList;

    @JSONCreator
    public DropPartitionTableWithGsiValidateTask(String schemaName,
                                                 String primaryTable,
                                                 List<String> indexNames,
                                                 List<TableGroupConfig> tableGroupConfigList) {
        super(schemaName);
        this.primaryTable = primaryTable;
        this.indexNames = indexNames;
        this.tableGroupConfigList = tableGroupConfigList;
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        TableValidator.validateTableExistence(schemaName, primaryTable, executionContext);

        // validate current gsi existence
        for (String gsiName : indexNames) {
            GsiValidator.validateGsiExistence(schemaName, primaryTable, gsiName, executionContext);
        }

        if (tableGroupConfigList != null) {
            tableGroupConfigList.forEach(e -> TableValidator.validateTableGroupChange(schemaName, e));
        }
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + primaryTable;
    }
}
