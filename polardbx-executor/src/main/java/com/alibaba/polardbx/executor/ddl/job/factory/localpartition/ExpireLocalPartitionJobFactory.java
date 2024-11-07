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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.parser.StringTimeParser;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropPartition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.builder.DirectPhysicalSqlPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.BaseSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataMppTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.ArchiveOSSTableDataWithPauseTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.FileValidationMppTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.FileValidationTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OSSTaskUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileCommitTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcExpireLocalPartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcExpireLocalPartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.localpartition.LocalPartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.shared.EmptyTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.ReorganizeLocalPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPhyDdlWrapper;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_OVERRIDE_NOW;

/**
 * @author guxu
 */
public class ExpireLocalPartitionJobFactory extends DdlJobFactory {

    private DDL ddl;
    private String schemaName;
    private String primaryTableName;
    private List<String> designatedPartitionNameList;
    private ExecutionContext executionContext;

    public ExpireLocalPartitionJobFactory(String schemaName,
                                          String primaryTableName,
                                          List<String> designatedPartitionNameList,
                                          DDL ddl,
                                          ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.designatedPartitionNameList = designatedPartitionNameList;
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
        IRepository repository = ExecutorContext.getContext(schemaName).getTopologyHandler()
            .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(primaryTableName);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (definitionInfo == null) {
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", schemaName, primaryTableName));
        }

        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            (MyRepository) repository, schemaName, primaryTableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        if (CollectionUtils.isNotEmpty(designatedPartitionNameList)) {
            for (String partition : designatedPartitionNameList) {
                if (!tableDescription.containsLocalPartition(partition)) {
                    throw new TddlNestableRuntimeException(
                        String.format("local partition %s doesn't exist", partition));
                }
            }
        }
        MysqlDateTime pivotDate = definitionInfo.evalPivotDate(executionContext);

        FailPoint.injectFromHint(FP_OVERRIDE_NOW, executionContext, (k, v) -> {
            MysqlDateTime parseDatetime = StringTimeParser.parseDatetime(v.getBytes());
            pivotDate.setYear(parseDatetime.getYear());
            pivotDate.setMonth(parseDatetime.getMonth());
            pivotDate.setDay(parseDatetime.getDay());
        });

        final List<LocalPartitionDescription> expiredLocalPartitionDescriptionList =
            LocalPartitionManager.getExpiredLocalPartitionDescriptionList(
                definitionInfo, tableDescription, pivotDate
            );
        final List<String> expiredPartitionNameList =
            expiredLocalPartitionDescriptionList.stream().map(e -> e.getPartitionName()).collect(Collectors.toList());

        final List<String> allPartitionsToExpire = new ArrayList<>();
        if (CollectionUtils.isEmpty(designatedPartitionNameList)) {
            allPartitionsToExpire.addAll(expiredPartitionNameList);
        } else {
            Set<String> expiredPartitionNameSet = Sets.newHashSet(
                expiredPartitionNameList.stream().map(e -> e.toLowerCase()).collect(Collectors.toList()));
            Set<String> designatedPartitionNameSet = Sets.newHashSet(
                designatedPartitionNameList.stream().map(e -> e.toLowerCase()).collect(Collectors.toList()));
            for (String partition : designatedPartitionNameSet) {
                if (!expiredPartitionNameSet.contains(partition)) {
                    throw new TddlNestableRuntimeException(
                        String.format("local partition %s is not yet expired", partition));
                }
            }
            allPartitionsToExpire.addAll(designatedPartitionNameSet);
        }

        if (CollectionUtils.isEmpty(allPartitionsToExpire)) {
            return new TransientDdlJob();
        }

        SQLAlterTableStatement alterTableStatement = new SQLAlterTableStatement();
        SQLAlterTableDropPartition dropLocalPartitionStmt = new SQLAlterTableDropPartition();
        allPartitionsToExpire.forEach(e -> dropLocalPartitionStmt.addPartition(new SQLIdentifierExpr(e)));
        alterTableStatement.setTableSource(new SQLExprTableSource(new SQLIdentifierExpr("?")));
        alterTableStatement.addItem(dropLocalPartitionStmt);
        alterTableStatement.setDbType(DbType.mysql);
        dropLocalPartitionStmt.putAttribute("SIMPLE", true);
        final String phySql = alterTableStatement.toString();

        Map<String, GsiMetaManager.GsiIndexMetaBean> publishedGsi = primaryTableMeta.getGsiPublished();
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        Map<String, Long> versionMap = new HashMap<>();
        versionMap.put(primaryTableName, primaryTableMeta.getVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, versionMap);
        executableDdlJob.addTask(validateTableVersionTask);

        LocalPartitionValidateTask localPartitionValidateTask =
            new LocalPartitionValidateTask(schemaName, primaryTableName);
        executableDdlJob.addTask(localPartitionValidateTask);
        executableDdlJob.addTaskRelationship(validateTableVersionTask, localPartitionValidateTask);

        DdlTask headTask = localPartitionValidateTask;

        // check if archive table exist
        final String archiveTableName = definitionInfo.getArchiveTableName();
        final String archiveTableSchema = GeneralUtil.coalesce(definitionInfo.getArchiveTableSchema(), schemaName);
        TableMeta archiveTableMeta;
        if (archiveTableName != null
            && (archiveTableMeta = OptimizerContext
            .getContext(archiveTableSchema)
            .getLatestSchemaManager()
            .getTableWithNull(archiveTableName)) != null) {

            // validate the topology and columns
            TableValidator.checkColumnConsistency(primaryTableMeta, archiveTableMeta);
            TableValidator.checkTopologyConsistency(primaryTableMeta, archiveTableMeta);

            // build back-fill tasks for all expired partitions
            Engine targetTableEngine = archiveTableMeta.getEngine();
            List<Long> archiveOSSTableDataTaskIdList = new ArrayList<>();
            if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MPP_FILE_STORE_BACKFILL)) {
                // enable mpp backfill in physical table level
                for (String physicalPartitionName : allPartitionsToExpire) {
                    EmptyTask emptyTask = new EmptyTask(schemaName);
                    executableDdlJob.addTask(emptyTask);

                    int totalNum = OSSTaskUtils.getMppParallelism(executionContext, primaryTableMeta);
                    for (int serialNum = 0; serialNum < totalNum; serialNum++) {
                        ArchiveOSSTableDataMppTask archiveOSSTableDataMPPTask = new ArchiveOSSTableDataMppTask(
                            archiveTableSchema, archiveTableName,
                            schemaName, primaryTableName,
                            physicalPartitionName, targetTableEngine,
                            totalNum, serialNum);
                        archiveOSSTableDataMPPTask.setTaskId(ID_GENERATOR.nextId());

                        FileValidationMppTask fileValidationMppTask = new FileValidationMppTask(
                            archiveTableSchema, archiveTableName,
                            schemaName, primaryTableName,
                            physicalPartitionName, totalNum, serialNum
                        );
                        executableDdlJob.addTask(archiveOSSTableDataMPPTask);
                        executableDdlJob.addTask(fileValidationMppTask);
                        executableDdlJob.addTaskRelationship(headTask, archiveOSSTableDataMPPTask);
                        executableDdlJob.addTaskRelationship(archiveOSSTableDataMPPTask, fileValidationMppTask);
                        executableDdlJob.addTaskRelationship(fileValidationMppTask, emptyTask);
                        archiveOSSTableDataTaskIdList.add(archiveOSSTableDataMPPTask.getTaskId());
                    }
                    headTask = emptyTask;
                }
                executableDdlJob.setMaxParallelism(OSSTaskUtils.getArchiveParallelism(executionContext));
            } else {
                allPartitionsToExpire.forEach(physicalPartitionName -> {
                    ArchiveOSSTableDataTask archiveOSSTableDataTask;
                    if (!executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_EXPIRE_FILE_STORAGE_PAUSE)) {
                        archiveOSSTableDataTask = new ArchiveOSSTableDataTask(
                            archiveTableSchema, archiveTableName,
                            schemaName, primaryTableName,
                            physicalPartitionName, targetTableEngine
                        );
                    } else {
                        archiveOSSTableDataTask = new ArchiveOSSTableDataWithPauseTask(
                            archiveTableSchema, archiveTableName,
                            schemaName, primaryTableName,
                            physicalPartitionName, targetTableEngine
                        );
                    }
                    archiveOSSTableDataTask.setTaskId(ID_GENERATOR.nextId());
                    taskList.add(archiveOSSTableDataTask);
                    archiveOSSTableDataTaskIdList.add(archiveOSSTableDataTask.getTaskId());

                    FileValidationTask fileValidationTask = new FileValidationTask(
                        archiveTableSchema, archiveTableName,
                        schemaName, primaryTableName,
                        physicalPartitionName
                    );
                    taskList.add(fileValidationTask);
                });
            }

            // build timestamp update task
            UpdateFileCommitTsTask updateFileCommitTsTask =
                new UpdateFileCommitTsTask(targetTableEngine.name(), archiveTableSchema, archiveTableName,
                    archiveOSSTableDataTaskIdList);
            taskList.add(updateFileCommitTsTask);
            BaseSyncTask tableSyncTask = new TableSyncTask(archiveTableSchema, archiveTableName);
            taskList.add(tableSyncTask);
        }

        taskList.add(genPhyDdlTask(schemaName, primaryTableName, phySql));
        if (publishedGsi != null) {
            publishedGsi.forEach((gsiName, gsiIndexMetaBean) -> {
                taskList.add(genPhyDdlTask(schemaName, gsiName, phySql));
            });
        }
        CdcExpireLocalPartitionMarkTask
            cdcMarkTask = new CdcExpireLocalPartitionMarkTask(schemaName, primaryTableName);
        taskList.add(cdcMarkTask);
        executableDdlJob.addSequentialTasksAfter(headTask, taskList);
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

        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, primaryTableName);
        archive.ifPresent(x -> {
            resources.add(concatWithDot(x.getKey(), x.getValue()));
        });
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        // add forbid drop read lock if the 'expire' ddl is cross schema
        Optional<Pair<String, String>> archive = CheckOSSArchiveUtil.getArchive(schemaName, primaryTableName);
        archive.ifPresent(x -> {
            if (!x.getKey().equalsIgnoreCase(schemaName)) {
                resources.add(LockUtil.genForbidDropResourceName(x.getKey()));
            }
        });
    }

}
