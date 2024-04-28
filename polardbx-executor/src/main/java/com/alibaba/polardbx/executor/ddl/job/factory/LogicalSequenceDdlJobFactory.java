package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalHandleSequenceTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalSequenceValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SequenceClearPlanCacheSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SequenceSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcLogicalSequenceMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SequenceBean;
import org.eclipse.jetty.util.StringUtil;

import java.util.List;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalSequenceDdlJobFactory extends DdlJobFactory {
    protected final SequenceBean sequenceBean;
    protected final ExecutionContext executionContext;
    protected final String schemName;

    protected final String tableName;

    public LogicalSequenceDdlJobFactory(String schemaName, String tableName, SequenceBean sequenceBean,
                                        ExecutionContext executionContext) {
        this.sequenceBean = sequenceBean;
        this.executionContext = executionContext;
        this.schemName = schemaName;
        this.tableName = tableName;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemName, sequenceBean.getName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        if (!StringUtil.isEmpty(tableName)) {
            resources.add(concatWithDot(schemName, tableName));
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        LogicalSequenceValidateTask logicalSequenceValidateTask =
            new LogicalSequenceValidateTask(schemName, sequenceBean);
        LogicalHandleSequenceTask logicalHandleSequenceTask =
            new LogicalHandleSequenceTask(schemName, tableName, sequenceBean);
        CdcLogicalSequenceMarkTask cdcMarkTask =
            new CdcLogicalSequenceMarkTask(schemName, sequenceBean.getName(), executionContext.getOriginSql(),
                sequenceBean.getKind());
        SequenceSyncTask sequenceSyncTask =
            new SequenceSyncTask(schemName, sequenceBean.getName(), sequenceBean.getKind());
        SequenceClearPlanCacheSyncTask sequenceClearPlanCacheSyncTask =
            new SequenceClearPlanCacheSyncTask(schemName, sequenceBean.getName(), sequenceBean.getKind());

        List<DdlTask> taskList = ImmutableList.of(
            logicalSequenceValidateTask,
            logicalHandleSequenceTask,
            cdcMarkTask,
            sequenceSyncTask,
            sequenceClearPlanCacheSyncTask
        );

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(String schemaName, String tableName, SequenceBean sequenceBean,
                                          ExecutionContext executionContext) {
        return new LogicalSequenceDdlJobFactory(schemaName, tableName, sequenceBean, executionContext).create();
    }

}
