package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ConvertAllSequenceValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ConvertSequenceInSchemasTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcLogicalSequenceMarkTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlKind;

import java.util.List;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ConvertAllSequencesJobFactory extends DdlJobFactory {
    final List<String> schemaNames;
    final Type fromType;
    final Type toType;
    final boolean onlySingleSchema;

    final ExecutionContext executionContext;

    public ConvertAllSequencesJobFactory(List<String> schemaNames, Type fromType, Type toType, boolean onlySingleSchema,
                                         ExecutionContext executionContext) {
        this.schemaNames = schemaNames;
        this.fromType = fromType;
        this.toType = toType;
        this.onlySingleSchema = onlySingleSchema;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        resources.addAll(schemaNames);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ConvertAllSequenceValidateTask validateTask = new ConvertAllSequenceValidateTask(schemaNames, onlySingleSchema);
        ConvertSequenceInSchemasTask convertSequenceTask =
            new ConvertSequenceInSchemasTask(schemaNames, fromType, toType);
        CdcLogicalSequenceMarkTask cdcLogicalSequenceMarkTask = new CdcLogicalSequenceMarkTask(
            schemaNames.size() == 1 ? schemaNames.get(0) : SystemDbHelper.DEFAULT_DB_NAME,
            "*",
            executionContext.getOriginSql(),
            SqlKind.CONVERT_ALL_SEQUENCES
        );
        List<DdlTask> ddlTaskList = ImmutableList.of(
            validateTask,
            convertSequenceTask,
            cdcLogicalSequenceMarkTask
        );

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ddlTaskList);
        return executableDdlJob;
    }

}
