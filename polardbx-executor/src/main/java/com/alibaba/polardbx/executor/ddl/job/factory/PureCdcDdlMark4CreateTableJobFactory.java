package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateTableIfNotExistsMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;

import java.util.Set;

public class PureCdcDdlMark4CreateTableJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String tableName;

    public PureCdcDdlMark4CreateTableJobFactory(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    @Override
    protected void validate() {
        LimitValidator.validateTableNameLength(schemaName);
        LimitValidator.validateTableNameLength(tableName);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        CdcCreateTableIfNotExistsMarkTask task = new CdcCreateTableIfNotExistsMarkTask(schemaName, tableName);
        executableDdlJob.addTask(task);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, tableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
