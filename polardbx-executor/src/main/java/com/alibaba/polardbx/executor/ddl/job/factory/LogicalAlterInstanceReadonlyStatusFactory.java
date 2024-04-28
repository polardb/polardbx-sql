package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.ChangeInstanceReadonlyStatusTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalAcquireMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.GlobalReleaseMdlLockInDbSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalAlterInstanceReadonlyStatusFactory extends DdlJobFactory {
    protected final boolean readonly;

    public LogicalAlterInstanceReadonlyStatusFactory(boolean readonly) {
        this.readonly = readonly;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {

        final String defaultSchema = "__cdc__";
        DbInfoManager dbInfoManager = DbInfoManager.getInstance();
        List<String> allSchemaList = dbInfoManager
            .getDbList().stream().filter(s -> !SystemDbHelper.isDBBuildIn(s))
            .collect(Collectors.toList());

        DdlTask globalAcquireMdlLockInDbTask = new GlobalAcquireMdlLockInDbSyncTask(
            defaultSchema,
            ImmutableSet.copyOf(allSchemaList)
        );
        DdlTask globalReleaseMdlLockInDbTask = new GlobalReleaseMdlLockInDbSyncTask(
            defaultSchema,
            ImmutableSet.copyOf(allSchemaList)
        );
        DdlTask changeDatabaseReadWriteStatus = new ChangeInstanceReadonlyStatusTask(defaultSchema, readonly);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ImmutableList.of(
            globalAcquireMdlLockInDbTask,
            changeDatabaseReadWriteStatus,
            globalReleaseMdlLockInDbTask
        ));

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        //forbid all database's ddl
        resources.add("__cdc__");
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
