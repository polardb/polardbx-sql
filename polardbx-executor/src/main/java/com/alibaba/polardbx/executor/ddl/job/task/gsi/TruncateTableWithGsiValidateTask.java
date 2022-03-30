package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;

@Getter
@TaskName(name = "TruncateTableWithGsiValidateTask")
public class TruncateTableWithGsiValidateTask extends DropPartitionTableWithGsiValidateTask{

    @JSONCreator
    public TruncateTableWithGsiValidateTask(String schemaName, String primaryTable,
                                            List<String> indexNames,
                                            List<TableGroupConfig> tableGroupConfigList) {
        super(schemaName, primaryTable, indexNames, tableGroupConfigList);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        GsiValidator.validateAllowTruncateOnTable(schemaName, primaryTable, executionContext);

        super.executeImpl(executionContext);
    }

    @Override
    protected String remark() {
        return "|logicalTableName: " + primaryTable;
    }
}
