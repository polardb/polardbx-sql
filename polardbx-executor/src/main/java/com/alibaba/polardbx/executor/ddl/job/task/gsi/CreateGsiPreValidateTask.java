package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

@TaskName(name = "CreateGsiPreValidateTask")
@Getter
public class CreateGsiPreValidateTask extends BaseValidateTask {

    final private String primaryTableName;
    final private String indexName;
    private List<Long> tableGroupIds;
    private TableGroupConfig tableGroupConfig;

    // this task will be execute at the beginning of creating primary table(following with CreateTableValidateTask)
    @JSONCreator
    public CreateGsiPreValidateTask(String schemaName, String primaryTableName, String indexName,
                                    List<Long> tableGroupIds, TableGroupConfig tableGroupConfig) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
        this.tableGroupIds = tableGroupIds;
        this.tableGroupConfig = tableGroupConfig;
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The table name shouldn't be empty");
        }
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        doValidate(executionContext);
    }

    public void doValidate(ExecutionContext executionContext) {
        TableValidator.validateTableGroupExistence(schemaName, tableGroupIds, executionContext);
        TableValidator.validateTableGroupChange(schemaName, tableGroupConfig);
    }

    @Override
    protected String remark() {
        return "|indexName: " + indexName;
    }

}
