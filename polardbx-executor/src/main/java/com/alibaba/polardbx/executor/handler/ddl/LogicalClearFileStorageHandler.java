package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.ClearFileStorageJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalClearFileStorage;

public class LogicalClearFileStorageHandler extends LogicalCommonDdlHandler {

    public LogicalClearFileStorageHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        LogicalClearFileStorage logicalClearFileStorage = (LogicalClearFileStorage) logicalDdlPlan;
        logicalClearFileStorage.preparedData();
        return new ClearFileStorageJobFactory(logicalClearFileStorage.getPreparedData(), executionContext).create();
    }
}
