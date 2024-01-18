/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.ddl.job.factory.gsi.TruncateTableWithGsiJobFactory;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.LogicalInsertTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTruncateTableWithGsiMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.TruncateCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.TruncateSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4InsertOverwrite;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.TruncateTableWithGsiPreparedData;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author lijiu.lzw
 */
public class InsertOverwriteJobFactory extends TruncateTableWithGsiJobFactory {
    private final String insertSql;

    public InsertOverwriteJobFactory(TruncateTableWithGsiPreparedData preparedData,
                                     String insertSql,
                                     ExecutionContext executionContext) {
        super(preparedData, executionContext);
        this.insertSql = insertSql;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, logicalTableName, executionContext);
        GsiValidator.validateAllowTruncateOnTable(schemaName, logicalTableName, executionContext);
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));
        resources.add(concatWithDot(schemaName, tmpPrimaryTableName));

        String tgName = FactoryUtils.getTableGroupNameByTableName(schemaName, logicalTableName);
        if (tgName != null) {
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob4InsertOverwrite result = new ExecutableDdlJob4InsertOverwrite();

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getTableName(), preparedData.getTableVersion());
        ValidateTableVersionTask
            validateTableVersionTask = new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);
        DdlTask validateTruncateTask = generateValidateTruncateTask();
        ExecutableDdlJob validateJob = new ExecutableDdlJob();
        validateJob.addSequentialTasks(Lists.newArrayList(validateTableVersionTask, validateTruncateTask));

        //生成创建临时表任务
        ExecutableDdlJob createTmpTableJob =
            isNewPartDb ? generateCreateTmpPartitionTableJob() : generateCreateTmpTableJob();

        //生成执行Insert任务
        Map<Integer, ParameterContext> parameters = null;
        if (executionContext.isExecutingPreparedStmt() && executionContext.getParams() != null
            && !executionContext.getParams().getCurrentParameter().isEmpty()) {
            //用户使用PreparedStatement执行时，需要把sql的附带数值也传输
            parameters = executionContext.getParams().getCurrentParameter();
        }
        DdlTask insertTask =
            new LogicalInsertTask(schemaName, logicalTableName, tmpPrimaryTableName, insertSql, parameters, 0);
        ExecutableDdlJob insertJob = new ExecutableDdlJob();
        insertJob.addTask(insertTask);
        //生成切换表结构任务
        ExecutableDdlJob cutOverJob = generateCutOverJob();

        //生成删除临时表任务
        ExecutableDdlJob dropTmpTableJob = isNewPartDb ? generateDropTmpPartitionTableJob() : generateDropTmpTableJob();

        result.appendJob2(validateJob);
        result.appendJob2(createTmpTableJob);
        result.appendJob2(insertJob);
        result.appendJob2(cutOverJob);
        result.appendJob2(dropTmpTableJob);

        result.setExceptionActionForAllSuccessor(validateTableVersionTask, DdlExceptionAction.ROLLBACK);
        result.setExceptionActionForAllSuccessor(recoverThenRollbackTask,
            DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
        result.setExceptionActionForAllSuccessor(recoverThenPauseTask, DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        result.setInsertTask(insertTask);
        //insert 只能rollback，无法重试
        insertTask.setExceptionAction(DdlExceptionAction.ROLLBACK);

        return result;
    }

    private ExecutableDdlJob generateCutOverJob() {
        ExecutableDdlJob cutOverJob = new ExecutableDdlJob();
        CdcTruncateTableWithGsiMarkTask cdcTask =
            new CdcTruncateTableWithGsiMarkTask(schemaName, logicalTableName, tmpPrimaryTableName);
        TruncateCutOverTask cutOverTask =
            new TruncateCutOverTask(schemaName, logicalTableName, tmpIndexTableMap, tmpPrimaryTableName);
        TruncateSyncTask syncTask =
            new TruncateSyncTask(schemaName, logicalTableName, tmpPrimaryTableName, tmpIndexTableMap.keySet());

        cutOverJob.addSequentialTasks(Lists.newArrayList(
            cdcTask,
            cutOverTask,
            syncTask
        ));

        recoverThenPauseTask = cdcTask;
        return cutOverJob;
    }
}
