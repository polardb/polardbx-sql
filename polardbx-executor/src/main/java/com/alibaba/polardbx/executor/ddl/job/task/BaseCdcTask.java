package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * @author wumu
 */
public abstract class BaseCdcTask extends BaseDdlTask {
    public BaseCdcTask(String schemaName) {
        super(schemaName);
    }

    @Override
    final protected void beforeRollbackTransaction(ExecutionContext executionContext) {
        throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC,
            "Rollback ddl failed, as the CDC task has already been completed.");
    }
}
