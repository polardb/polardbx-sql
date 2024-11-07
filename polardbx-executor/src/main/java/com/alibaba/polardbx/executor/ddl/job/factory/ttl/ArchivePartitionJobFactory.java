package com.alibaba.polardbx.executor.ddl.job.factory.ttl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OssArchiveCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.OssTableArchivePartitionTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.oss.UpdateFileCommitTsTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.ArchivePartitionValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.UpdateTtlTmpTablePartStateTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.ttl.TtlPartArcState;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager.ID_GENERATOR;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author wumu
 */
public class ArchivePartitionJobFactory extends DdlJobFactory {

    private final String schemaName;
    private final String ossTableName;
    private final String archiveTmpTableSchema;
    private final String archiveTmpTableName;
    private final List<String> designatedPartitionNameList;
    private final List<String> firstLevelPartitionNameList;
    private final ExecutionContext executionContext;

    public ArchivePartitionJobFactory(String schemaName,
                                      String ossTableName,
                                      String archiveTmpTableSchema,
                                      String archiveTmpTableName,
                                      List<String> designatedPartitionNameList,
                                      List<String> firstLevelPartitionNameList,
                                      ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.ossTableName = ossTableName;
        this.archiveTmpTableSchema = archiveTmpTableSchema;
        this.archiveTmpTableName = archiveTmpTableName;
        this.designatedPartitionNameList = designatedPartitionNameList;
        this.firstLevelPartitionNameList = firstLevelPartitionNameList;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        CheckOSSArchiveUtil.checkColumnConsistency(schemaName, ossTableName, archiveTmpTableSchema,
            archiveTmpTableName);

        List<String> partNamesAndSubPartNamesToBeUpdateArcState = new ArrayList<>();
        partNamesAndSubPartNamesToBeUpdateArcState.addAll(firstLevelPartitionNameList);
        partNamesAndSubPartNamesToBeUpdateArcState.addAll(designatedPartitionNameList);
        CheckOSSArchiveUtil.validateArchivePartitions(archiveTmpTableSchema, archiveTmpTableName,
            partNamesAndSubPartNamesToBeUpdateArcState, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        SchemaManager sm = OptimizerContext.getContext(archiveTmpTableSchema).getLatestSchemaManager();
        final TableMeta ossTableMeta = sm.getTable(ossTableName);

        List<DdlTask> taskList = new ArrayList<>();
        Map<String, Long> versionMap = new HashMap<>(1);
        versionMap.put(ossTableName, ossTableMeta.getVersion());
        ValidateTableVersionTask validateTableVersionTask = new ValidateTableVersionTask(schemaName, versionMap);
        taskList.add(validateTableVersionTask);

        List<String> partNamesAndSubPartNamesToBeUpdateArcState = new ArrayList<>();
        partNamesAndSubPartNamesToBeUpdateArcState.addAll(firstLevelPartitionNameList);
        partNamesAndSubPartNamesToBeUpdateArcState.addAll(designatedPartitionNameList);
        ArchivePartitionValidateTask archivePartitionValidateTask =
            new ArchivePartitionValidateTask(schemaName, ossTableName, archiveTmpTableSchema, archiveTmpTableName,
                partNamesAndSubPartNamesToBeUpdateArcState);
        taskList.add(archivePartitionValidateTask);

        TableMeta arcTmpTblMeta =
            OptimizerContext.getContext(archiveTmpTableSchema).getLatestSchemaManager().getTable(archiveTmpTableName);
        PartitionInfo arcTmpTblPartInfo = arcTmpTblMeta.getPartitionInfo();

        List<String> partAndSubPartNamesForReadyState = new ArrayList<>();
        List<String> partAndSubPartNamesForReReadyState = new ArrayList<>();

        for (int i = 0; i < firstLevelPartitionNameList.size(); i++) {
            String firstLevelPartName = firstLevelPartitionNameList.get(i);
            PartitionSpec firstLevelPartSpec =
                arcTmpTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(firstLevelPartName);
            if (firstLevelPartSpec.getArcState() == TtlPartArcState.ARC_STATE_READY.getArcState()) {
                partAndSubPartNamesForReadyState.add(firstLevelPartName);
            } else if (firstLevelPartSpec.getArcState() == TtlPartArcState.ARC_STATE_REREADY.getArcState()) {
                partAndSubPartNamesForReReadyState.add(firstLevelPartName);
            }
        }

        for (int i = 0; i < designatedPartitionNameList.size(); i++) {
            String phyPartName = designatedPartitionNameList.get(i);
            PartitionSpec phyPartSpec = arcTmpTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(phyPartName);
            if (phyPartSpec != null) {
                if (phyPartSpec.getArcState() == TtlPartArcState.ARC_STATE_READY.getArcState()) {
                    partAndSubPartNamesForReadyState.add(phyPartName);
                } else if (phyPartSpec.getArcState() == TtlPartArcState.ARC_STATE_REREADY.getArcState()) {
                    partAndSubPartNamesForReReadyState.add(phyPartName);
                }
            }
        }

        if (!partAndSubPartNamesForReadyState.isEmpty()) {
            UpdateTtlTmpTablePartStateTask updateArchivingTaskForReadyState =
                new UpdateTtlTmpTablePartStateTask(archiveTmpTableSchema, archiveTmpTableName,
                    partAndSubPartNamesForReadyState,
                    TtlPartArcState.ARC_STATE_READY.getArcState(), TtlPartArcState.ARC_STATE_ARCHIVING.getArcState());
            taskList.add(updateArchivingTaskForReadyState);
        }

        if (!partAndSubPartNamesForReReadyState.isEmpty()) {
            UpdateTtlTmpTablePartStateTask updateArchivingTaskForReReady =
                new UpdateTtlTmpTablePartStateTask(archiveTmpTableSchema, archiveTmpTableName,
                    partAndSubPartNamesForReReadyState,
                    TtlPartArcState.ARC_STATE_REREADY.getArcState(), TtlPartArcState.ARC_STATE_ARCHIVING.getArcState());
            taskList.add(updateArchivingTaskForReReady);
        }

        TableSyncTask archivingSyncTask = new TableSyncTask(archiveTmpTableSchema, archiveTmpTableName);
        taskList.add(archivingSyncTask);

        // build back-fill tasks for all expired partitions
        Engine targetTableEngine = ossTableMeta.getEngine();
        List<Long> archiveOssTableDataTaskIdList = new ArrayList<>();

        OssTableArchivePartitionTask ossTableArchivePartitionTask = new OssTableArchivePartitionTask(
            schemaName, ossTableName,
            archiveTmpTableSchema, archiveTmpTableName,
            designatedPartitionNameList, targetTableEngine
        );
        ossTableArchivePartitionTask.setTaskId(ID_GENERATOR.nextId());
        taskList.add(ossTableArchivePartitionTask);
        archiveOssTableDataTaskIdList.add(ossTableArchivePartitionTask.getTaskId());

        OssArchiveCheckTask ossArchiveCheckTask = new OssArchiveCheckTask(
            schemaName, ossTableName,
            archiveTmpTableSchema, archiveTmpTableName,
            designatedPartitionNameList, ossTableArchivePartitionTask.getTaskId()
        );

        taskList.add(ossArchiveCheckTask);

        // build timestamp update task
        UpdateFileCommitTsTask updateFileCommitTsTask =
            new UpdateFileCommitTsTask(targetTableEngine.name(), schemaName, ossTableName,
                archiveOssTableDataTaskIdList);
        taskList.add(updateFileCommitTsTask);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, ossTableName);
        taskList.add(tableSyncTask);

        SubJobTask subJobTask = buildTruncatePartitionsTask();
        taskList.add(subJobTask);

        UpdateTtlTmpTablePartStateTask updateArchivedTask =
            new UpdateTtlTmpTablePartStateTask(archiveTmpTableSchema, archiveTmpTableName,
                partNamesAndSubPartNamesToBeUpdateArcState,
                TtlPartArcState.ARC_STATE_ARCHIVING.getArcState(), TtlPartArcState.ARC_STATE_ARCHIVED.getArcState());
        TableSyncTask archivedSyncTask = new TableSyncTask(archiveTmpTableSchema, archiveTmpTableName);
        taskList.add(updateArchivedTask);
        taskList.add(archivedSyncTask);

        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, ossTableName));

        resources.add(concatWithDot(archiveTmpTableSchema, archiveTmpTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
        // add forbid drop read lock if the 'archive' ddl is cross schema
        resources.add(LockUtil.genForbidDropResourceName(archiveTmpTableSchema));
    }

    protected SubJobTask buildTruncatePartitionsTask() {
        // alter table truncate partition
        String sql = String.format("ALTER TABLE %s.%s TRUNCATE PARTITION %s",
            surroundWithBacktick(archiveTmpTableSchema), surroundWithBacktick(archiveTmpTableName),
            String.join(",", firstLevelPartitionNameList));
        SubJobTask subJobTask = new SubJobTask(archiveTmpTableSchema, sql, null);
        subJobTask.setParentAcquireResource(true);
        return subJobTask;
    }
}
