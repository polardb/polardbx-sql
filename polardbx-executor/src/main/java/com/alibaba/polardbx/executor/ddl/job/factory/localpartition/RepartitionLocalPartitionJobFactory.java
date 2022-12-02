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

package com.alibaba.polardbx.executor.ddl.job.factory.localpartition;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AddLocalPartitionTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RemoveLocalPartitionTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.LocalPartitionDefinitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author guxu
 */
public class RepartitionLocalPartitionJobFactory extends DdlJobFactory {

    private DDL ddl;
    private String schemaName;
    private String primaryTableName;
    private LocalPartitionDefinitionInfo definitionInfo;
    private ExecutionContext executionContext;

    public RepartitionLocalPartitionJobFactory(String schemaName,
                                               String primaryTableName,
                                               LocalPartitionDefinitionInfo definitionInfo,
                                               DDL ddl,
                                               ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.definitionInfo = definitionInfo;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        final TableMeta primaryTableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        if(primaryTableMeta.getLocalPartitionDefinitionInfo() != null){
            LocalPartitionValidateTask localPartitionValidateTask = new LocalPartitionValidateTask(schemaName, primaryTableName);
            localPartitionValidateTask.executeImpl(executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final TableMeta primaryTableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);

        checkLocalPartitionColumnInUk(primaryTableMeta);
        List<TableMeta> gsiList = GlobalIndexMeta.getIndex(primaryTableName, schemaName, executionContext);
        if(CollectionUtils.isNotEmpty(gsiList)){
            for(TableMeta gsiMeta: gsiList){
                checkLocalPartitionColumnInUk(gsiMeta);
            }
        }

        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(executionContext);

        SQLPartitionByRange partitionByRange = LocalPartitionDefinitionInfo
            .generateLocalPartitionStmtForCreate(definitionInfo, pivotDate);

        SQLAlterTableStatement alterTableStatement = new SQLAlterTableStatement();
        alterTableStatement.setPartition(partitionByRange);
        alterTableStatement.setTableSource(new SQLExprTableSource(new SQLIdentifierExpr("?")));
        alterTableStatement.setDbType(DbType.mysql);
        final String phySql = alterTableStatement.toString();

        Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = primaryTableMeta.getGsiPublished();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        if(primaryTableMeta.getLocalPartitionDefinitionInfo() != null){
            LocalPartitionValidateTask localPartitionValidateTask = new LocalPartitionValidateTask(schemaName, primaryTableName);
            taskList.add(localPartitionValidateTask);
        }
        taskList.add(genPhyDdlTask(schemaName, primaryTableName, phySql));
        if(publishedGsi != null){
            publishedGsi.forEach((gsiName, gsiIndexMetaBean) -> {
                taskList.add(genPhyDdlTask(schemaName, gsiName, phySql));
            });
        }
        if(primaryTableMeta.getLocalPartitionDefinitionInfo() != null){
            taskList.add(new RemoveLocalPartitionTask(schemaName, primaryTableName));
        }
        taskList.add(new AddLocalPartitionTask(definitionInfo));
        taskList.add(new TableSyncTask(schemaName, primaryTableName));
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    private void checkLocalPartitionColumnInUk(TableMeta tableMeta) {
        final String columnName = definitionInfo.getColumnName();
        List<IndexMeta> ukList = tableMeta.getUniqueIndexes(true);
        if(CollectionUtils.isNotEmpty(ukList)){
            for(IndexMeta indexMeta: ukList){
                if(indexMeta.getKeyColumn(columnName)==null){
                    throw new TddlNestableRuntimeException(String.format(
                        "Unsupported index table structure, Primary/Unique Key must contain local partition column: %s", columnName
                    ));
                }
            }
        }
    }

    private LocalPartitionPhyDdlTask genPhyDdlTask(String schemaName, String tableName, String phySql){
        ddl.sqlNode = SqlPhyDdlWrapper.createForAllocateLocalPartition(new SqlIdentifier(tableName, SqlParserPos.ZERO), phySql);
        DirectPhysicalSqlPlanBuilder builder = new DirectPhysicalSqlPlanBuilder(
            ddl, new ReorganizeLocalPartitionPreparedData(schemaName, tableName), executionContext
        );
        builder.build();
        LocalPartitionPhyDdlTask phyDdlTask = new LocalPartitionPhyDdlTask(schemaName, builder.genPhysicalPlanData());
        return phyDdlTask;
    }

    private boolean modifyDefinitionOnly(LocalPartitionDefinitionInfo origin, LocalPartitionDefinitionInfo definitionInfo){
        if(origin==null){
            return false;
        }
        if(!StringUtils.equalsIgnoreCase(origin.getColumnName(), definitionInfo.getColumnName())){
            return false;
        }
        if(!StringUtils.equalsIgnoreCase(origin.getIntervalUnit(), definitionInfo.getIntervalUnit())){
            return false;
        }
        if(origin.getIntervalCount() != definitionInfo.getIntervalCount()){
            return false;
        }
        if(origin.getPreAllocateCount() < definitionInfo.getPreAllocateCount()){
            return false;
        }
        return true;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}