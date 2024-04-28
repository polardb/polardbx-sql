package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

@TaskName(name = "CreateColumnarIndexValidateTask")
@Getter
public class CreateColumnarIndexValidateTask extends BaseValidateTask {

    final private String primaryTableName;
    final private String indexName;

    @JSONCreator
    public CreateColumnarIndexValidateTask(String schemaName, String primaryTableName, String indexName) {
        super(schemaName);
        this.primaryTableName = primaryTableName;
        this.indexName = indexName;
        if (StringUtils.isEmpty(indexName) || StringUtils.isEmpty(primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED, "validate",
                "The table name shouldn't be empty");
        }
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        if (!TableValidator.checkIfTableExists(schemaName, primaryTableName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, schemaName, primaryTableName);
        }
        IndexValidator.validateIndexNonExistence(schemaName, primaryTableName, indexName);
        //IndexValidator.validateColumnarIndexNonExistence(schemaName, primaryTableName);
        IndexValidator.validateColumnarIndexNumLimit(schemaName, primaryTableName,
            executionContext.getParamManager().getLong(ConnectionParams.MAX_CCI_COUNT));

        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexName, executionContext);
    }

    @Override
    protected String remark() {
        return "|primaryTableName: " + primaryTableName;
    }

}