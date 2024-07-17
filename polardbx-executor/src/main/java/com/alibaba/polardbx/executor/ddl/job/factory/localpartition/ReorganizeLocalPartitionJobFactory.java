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
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableReorgPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.alibaba.polardbx.common.ddl.newengine.DdlLocalPartitionConstants.PMAX;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_OVERRIDE_NOW;

/**
 * @author guxu
 */
public class ReorganizeLocalPartitionJobFactory extends DdlJobFactory {

    private DDL ddl;
    private String schemaName;
    private String primaryTableName;
    private ExecutionContext executionContext;

    public ReorganizeLocalPartitionJobFactory(String schemaName,
                                              String primaryTableName,
                                              DDL ddl,
                                              ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        //校验local partition对齐
        LocalPartitionValidateTask localPartitionValidateTask =
            new LocalPartitionValidateTask(schemaName, primaryTableName);
        localPartitionValidateTask.executeImpl(executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (definitionInfo == null) {
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", schemaName, primaryTableName));
        }

        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(executionContext);
        FailPoint.injectFromHint(FP_OVERRIDE_NOW, executionContext, (k, v) -> {
            MysqlDateTime parseDatetime = StringTimeParser.parseDatetime(v.getBytes());
            pivotDate.setYear(parseDatetime.getYear());
            pivotDate.setMonth(parseDatetime.getMonth());
            pivotDate.setDay(parseDatetime.getDay());
        });

        IRepository repository = ExecutorContext.getContext(schemaName)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            (MyRepository) repository,
            schemaName,
            primaryTableName,
            true);
        MysqlDateTime newestPartitionDate = LocalPartitionManager.getNewestPartitionDate(tableDescriptionList.get(0));
        if (newestPartitionDate == null) {
            newestPartitionDate = definitionInfo.evalPivotDate(executionContext);
        }
        Optional<SQLPartitionByRange> partitionByRange = LocalPartitionDefinitionInfo
            .generatePreAllocateLocalPartitionStmt(definitionInfo, newestPartitionDate, pivotDate);
        if (!partitionByRange.isPresent()) {
            return new TransientDdlJob();
        }

        SQLAlterTableStatement alterTableStatement = new SQLAlterTableStatement();
        SQLAlterTableReorgPartition reOrganizeStmt = new SQLAlterTableReorgPartition();
        reOrganizeStmt.getNames().add(new SQLIdentifierExpr(PMAX));
        partitionByRange.get().getPartitions().forEach(reOrganizeStmt::addPartition);

        alterTableStatement.setTableSource(new SQLExprTableSource(new SQLIdentifierExpr("?")));
        alterTableStatement.addItem(reOrganizeStmt);
        alterTableStatement.setDbType(DbType.mysql);
        final String phySql = alterTableStatement.toString();

        Map<String, Long> versionMap = new HashMap<>();
        versionMap.put(primaryTableName, primaryTableMeta.getVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, versionMap);

        LocalPartitionValidateTask localPartitionValidateTask =
            new LocalPartitionValidateTask(schemaName, primaryTableName);
        Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = primaryTableMeta.getGsiPublished();

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        taskList.add(validateTableVersionTask);
        taskList.add(localPartitionValidateTask);
        taskList.add(genPhyDdlTask(schemaName, primaryTableName, phySql));
        if (publishedGsi != null) {
            publishedGsi.forEach((gsiName, gsiIndexMetaBean) -> {
                taskList.add(genPhyDdlTask(schemaName, gsiName, phySql));
            });
        }
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    private LocalPartitionPhyDdlTask genPhyDdlTask(String schemaName, String tableName, String phySql) {
        ddl.sqlNode =
            SqlPhyDdlWrapper.createForAllocateLocalPartition(new SqlIdentifier(tableName, SqlParserPos.ZERO), phySql);
        DirectPhysicalSqlPlanBuilder builder = new DirectPhysicalSqlPlanBuilder(
            ddl, new ReorganizeLocalPartitionPreparedData(schemaName, tableName), executionContext
        );
        builder.build();
        LocalPartitionPhyDdlTask phyDdlTask = new LocalPartitionPhyDdlTask(schemaName, builder.genPhysicalPlanData());
        return phyDdlTask;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
