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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ClearAutoPartitionFlagTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RepartitionSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropGsi;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Sets;
import org.apache.calcite.sql.SqlAlterTablePartitionKey;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator.getExpectedPrimaryAndShardingKeys;

/**
 * @author guxu
 */
public class RepartitionJobFactory extends DdlJobFactory {

    private final SqlAlterTablePartitionKey ast;

    private final String schemaName;
    private final String primaryTableName;
    private final String indexTableName;
    private final boolean isSingle;
    private final boolean isBroadcast;
    final List<SqlIndexColumnName> columns;
    final List<SqlIndexColumnName> coverings;
    final PhysicalPlanData physicalPlanData;

    private final ExecutionContext executionContext;

    public RepartitionJobFactory(SqlAlterTablePartitionKey ast,
                                 String schemaName,
                                 String primaryTableName,
                                 String indexTableName,
                                 boolean isSingle,
                                 boolean isBroadcast,
                                 List<SqlIndexColumnName> columns,
                                 List<SqlIndexColumnName> coverings,
                                 PhysicalPlanData physicalPlanData,
                                 ExecutionContext executionContext) {
        this.ast = ast;
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.isSingle = isSingle;
        this.isBroadcast = isBroadcast;
        this.columns = columns;
        this.coverings = coverings;
        this.physicalPlanData = physicalPlanData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
        RepartitionValidator.validateGsiColumns(
            schemaName,
            primaryTableName,
            isSingle,
            isBroadcast,
            ast.getDbPartitionBy(),
            ast.getTablePartitionBy(),
            executionContext
        );
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob repartitionJob = new ExecutableDdlJob();

        CreateGsiJobFactory createGsiJobFactory = new CreateGsiJobFactory(
            schemaName,
            primaryTableName,
            indexTableName,
            columns,
            coverings,
            false,
            "for repartition",
            null,
            false, physicalPlanData,
            executionContext
        );
        createGsiJobFactory.stayAtBackFill = true;
        ExecutableDdlJob4CreateGsi createGsiJob = (ExecutableDdlJob4CreateGsi) createGsiJobFactory.doCreate();

        RepartitionCutOverTask cutOverTask =
            new RepartitionCutOverTask(schemaName, primaryTableName, indexTableName, isSingle, isBroadcast);
        RepartitionSyncTask repartitionSyncTask = new RepartitionSyncTask(schemaName, primaryTableName, indexTableName);

        DdlTask cdcDdlMarkTask = new CdcRepartitionMarkTask(schemaName, primaryTableName, SqlKind.ALTER_TABLE);

        ExecutableDdlJob4DropGsi dropGsiJob = (ExecutableDdlJob4DropGsi) new DropGsiJobFactory(
            schemaName,
            primaryTableName,
            indexTableName,
            executionContext
        ).doCreate();
        //rollback is not supported after CutOver
        dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);

        repartitionJob.combineTasks(createGsiJob);

        final boolean skipCutOver =
            StringUtils.equalsIgnoreCase(
                executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER), "true");
        if (!skipCutOver) {
            repartitionJob.addTaskRelationship(createGsiJob.getLastTask(), cutOverTask);
            repartitionJob.addTaskRelationship(cutOverTask, repartitionSyncTask);
            repartitionJob.addTaskRelationship(repartitionSyncTask, cdcDdlMarkTask);
        } else {
            repartitionJob.addTaskRelationship(createGsiJob.getLastTask(), cdcDdlMarkTask);
        }

        final boolean skipCleanUp =
            StringUtils.equalsIgnoreCase(
                executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CLEANUP), "true");
        if (!skipCleanUp) {
            repartitionJob.combineTasks(dropGsiJob);
            repartitionJob.addTaskRelationship(cdcDdlMarkTask, dropGsiJob.getValidateTask());
        }

        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        if (primaryTableMeta.isAutoPartition()) {
            ClearAutoPartitionFlagTask clearFlagTask = new ClearAutoPartitionFlagTask(schemaName, primaryTableName);
            TableSyncTask tableSyncTask = new TableSyncTask(schemaName, primaryTableName);
            repartitionJob.addTaskRelationship(clearFlagTask, tableSyncTask);
            repartitionJob.addTaskRelationship(tableSyncTask, createGsiJob.getCreateGsiValidateTask());
        }
        return repartitionJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    private Map<String, List<String>> initIndexInfoForRebuildingGsi() {

        final Map<String, List<String>> result = new HashMap<>();

        List<TableMeta> gsiTableMetaList =
            GlobalIndexMeta.getIndex(primaryTableName, schemaName, executionContext);
        if (CollectionUtils.isEmpty(gsiTableMetaList)) {
            return result;
        }

        //拆分变更后，GSI表必须包含的列
        final Set<String> expectedPkSkList = getExpectedPrimaryAndShardingKeys(
            schemaName,
            primaryTableName,
            isSingle,
            isBroadcast,
            ast.getDbPartitionBy(),
            ast.getTablePartitionBy()
        );

        for (TableMeta gsiTableMeta : gsiTableMetaList) {
            final String gsiName = gsiTableMeta.getTableName();
            final Set<String> gsiAllColumns = Sets.newHashSet(columnMeta2nameStr(gsiTableMeta.getAllColumns()));

            //如果发现GSI缺少列，则将该列加入重新后GSI的covering列
            if (!CollectionUtils.isSubCollection(expectedPkSkList, gsiAllColumns)) {
                final Collection<String> missingColumnNames = CollectionUtils.subtract(expectedPkSkList, gsiAllColumns);
                if (!result.containsKey(gsiName)) {
                    result.put(gsiName, new ArrayList<>());
                }
                result.get(gsiName).addAll(missingColumnNames);
            }
        }

        return result;
    }

    private List<String> columnMeta2nameStr(List<ColumnMeta> columnMetaList) {
        if (CollectionUtils.isEmpty(columnMetaList)) {
            return new ArrayList<>();
        }
        return columnMetaList.stream()
            .map(e -> StringUtils.lowerCase(e.getName()))
            .collect(Collectors.toList());
    }

}
