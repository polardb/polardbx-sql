package com.alibaba.polardbx.executor.ddl.job.factory.gsi;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterColumnDefaultTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.ModifyPartitionKeyRemoveTableStatisticTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.RebuildTableChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcModifyPartitionKeyMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ModifyPartitionKeySyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RebuildTableCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.RebuildTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RebuildTablePrepareData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author wumu
 */
public class RebuildTableJobFactory extends DdlJobFactory {
    private final String schemaName;
    private final String primaryTableName;
    private final Map<String, String> tableNameMap;
    private final Map<String, String> tableNameMapReverse;
    private final Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData;
    private final ExecutionContext executionContext;
    private List<String> alterDefaultColumns;
    private List<String> changedColumns;

    private boolean needDropImplicitKey;

    private final Map<String, String> virtualColumnMap;
    private final Map<String, String> columnNewDef;
    private final Map<String, String> backfillColumnMap;
    private final PhysicalPlanData oldPhysicalPlanData;

    private final Map<String, Boolean> needRehash;
    private final List<String> modifyStringColumns;
    private final List<String> addNewColumns;

    public RebuildTableJobFactory(String schemaName, String primaryTableName,
                                  Map<CreateGlobalIndexPreparedData, PhysicalPlanData> globalIndexPrepareData,
                                  RebuildTablePrepareData rebuildTablePrepareData,
                                  PhysicalPlanData oldPhysicalPlanData,
                                  ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.globalIndexPrepareData = globalIndexPrepareData;
        this.executionContext = executionContext;
        this.needDropImplicitKey = false;
        this.alterDefaultColumns = null;
        this.tableNameMap = rebuildTablePrepareData.getTableNameMap();
        this.tableNameMapReverse = rebuildTablePrepareData.getTableNameMapReverse();
        this.virtualColumnMap = rebuildTablePrepareData.getVirtualColumnMap();
        this.columnNewDef = rebuildTablePrepareData.getColumnNewDef();
        this.backfillColumnMap = rebuildTablePrepareData.getBackfillColumnMap();
        this.needRehash = rebuildTablePrepareData.getNeedReHash();
        this.modifyStringColumns = rebuildTablePrepareData.getModifyStringColumns();
        this.addNewColumns = rebuildTablePrepareData.getAddNewColumns();
        this.oldPhysicalPlanData = oldPhysicalPlanData;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
        GsiValidator.validateGsiSupport(schemaName, executionContext);

        for (String indexTableName : tableNameMap.values()) {
            GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final boolean useChangeSet =
            ChangeSetUtils.isChangeSetProcedure(executionContext) && executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_CHANGESET_FOR_OMC);
        final boolean enableBackFillPushDown =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_BACKFILL_OPT_FOR_OMC);

        ExecutableDdlJob ddlJob = new ExecutableDdlJob();

        assert !globalIndexPrepareData.isEmpty();

        // alter table partitions validate
        // get old table groups
        List<TableGroupConfig> tableGroupConfigs = new ArrayList<>();
        List<TableGroupConfig> oldTableGroupConfigs =
            FactoryUtils.getTableGroupConfigByTableName(schemaName, new ArrayList<>(tableNameMap.keySet()));
        tableGroupConfigs.addAll(oldTableGroupConfigs);
        // get new table groups
        tableGroupConfigs.addAll(
            globalIndexPrepareData.values().stream()
                .map(PhysicalPlanData::getTableGroupConfig).collect(Collectors.toList())
        );
        DdlTask validateTask =
            new RebuildTableValidateTask(schemaName, primaryTableName, tableNameMap, tableGroupConfigs);

        // 标记开始 rebuild
        RebuildTableChangeMetaTask rebuildTableChangeMetaTask =
            new RebuildTableChangeMetaTask(schemaName, primaryTableName);
        TableSyncTask rebuildSyncTask = new TableSyncTask(schemaName, primaryTableName);

        // for modify default column
        DdlTask beginAlterColumnDefault = null;
        DdlTask beginAlterColumnDefaultSyncTask = null;
        if (CollectionUtils.isNotEmpty(alterDefaultColumns)) {
            beginAlterColumnDefault =
                new AlterColumnDefaultTask(schemaName, primaryTableName, alterDefaultColumns, true);
            beginAlterColumnDefaultSyncTask = new TableSyncTask(schemaName, primaryTableName);
        }

        List<DdlTask> checkerTasks = genCheckerTasks();

        // create gsi
        List<ExecutableDdlJob> createGsiJobs = new ArrayList<>();
        AtomicBoolean hasSubJob = new AtomicBoolean(false);
        List<Pair<CreateGlobalIndexPreparedData, PhysicalPlanData>> listGlobalIndexPrepareData = new ArrayList<>();
        globalIndexPrepareData.forEach((createGlobalIndexPreparedData, physicalPlanData) -> {
            listGlobalIndexPrepareData.add(new Pair<>(createGlobalIndexPreparedData, physicalPlanData));
        });

        Collections.sort(listGlobalIndexPrepareData,
            new Comparator<Pair<CreateGlobalIndexPreparedData, PhysicalPlanData>>() {
                @Override
                public int compare(Pair<CreateGlobalIndexPreparedData, PhysicalPlanData> o1,
                                   Pair<CreateGlobalIndexPreparedData, PhysicalPlanData> o2) {
                    CreateGlobalIndexPreparedData data1 = o1.getKey();
                    CreateGlobalIndexPreparedData data2 = o2.getKey();

                    // 检查fieldA是否为空（这里假设fieldA是String类型）
                    boolean isData1TableGroupAlignWithTargetTableEmpty =
                        StringUtils.isEmpty(data1.getTableGroupAlignWithTargetTable());
                    boolean isData2TableGroupAlignWithTargetTableEmpty =
                        StringUtils.isEmpty(data2.getTableGroupAlignWithTargetTable());

                    if (isData1TableGroupAlignWithTargetTableEmpty && !isData2TableGroupAlignWithTargetTableEmpty) {
                        return -1; // data1中的TableGroupAlignWithTargetTable为空，应该排在前面
                    } else if (!isData1TableGroupAlignWithTargetTableEmpty
                        && isData2TableGroupAlignWithTargetTableEmpty) {
                        return 1; // data2中的TableGroupAlignWithTargetTable为空，data1应该排在后面
                    } else {
                        return 0; // 两者都为空或都不为空，视为相等
                    }
                }
            });
        for (int i = 0; i < listGlobalIndexPrepareData.size(); i++) {
            Pair<CreateGlobalIndexPreparedData, PhysicalPlanData> pair = listGlobalIndexPrepareData.get(i);
            PhysicalPlanData physicalPlanData = pair.getValue();
            CreateGlobalIndexPreparedData createGlobalIndexPreparedData = pair.getKey();
            if (!hasSubJob.get()) {
                if (physicalPlanData.getTableGroupConfig() != null) {
                    TableGroupRecord tableGroupRecord = physicalPlanData.getTableGroupConfig().getTableGroupRecord();
                    if (tableGroupRecord != null && (tableGroupRecord.id == null
                        || tableGroupRecord.id == TableGroupRecord.INVALID_TABLE_GROUP_ID)
                        && tableGroupRecord.getTg_type() == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
                        OptimizerContext oc =
                            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                        TableGroupConfig tableGroupConfig = oc.getTableGroupInfoManager()
                            .getTableGroupConfigByName(TableGroupNameUtil.SINGLE_DEFAULT_TG_NAME_TEMPLATE);
                        if (tableGroupConfig != null) {
                            tableGroupRecord.setTg_type(TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG);
                        }
                    }
                }

                CreateGsiJobFactory createGsiJobFactory =
                    CreateGsiJobFactory.create(createGlobalIndexPreparedData, physicalPlanData, executionContext);
                createGsiJobFactory.stayAtBackFill = true;
                createGsiJobFactory.setVirtualColumnMap(virtualColumnMap);
                createGsiJobFactory.setBackfillColumnMap(backfillColumnMap);
                String oldIndexName = tableNameMapReverse.get(createGlobalIndexPreparedData.getIndexTableName());
                createGsiJobFactory.setOldIndexName(oldIndexName);

                boolean mirrorCopy = !needRehash.get(createGlobalIndexPreparedData.getIndexTableName());
                if (enableBackFillPushDown) {
                    createGsiJobFactory.setMirrorCopy(mirrorCopy);
                }
                TableMeta gsiTableMeta = executionContext.getSchemaManager(schemaName).getTable(oldIndexName);
                if (useChangeSet && ChangeSetUtils.supportUseChangeSet(
                    ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN, gsiTableMeta)) {
                    createGsiJobFactory.setUseChangeSet(mirrorCopy);
                }

                createGsiJobFactory.setOnlineModifyColumn(true);
                createGsiJobFactory.setModifyStringColumns(modifyStringColumns);
                createGsiJobFactory.setAddNewColumns(addNewColumns);
                ExecutableDdlJob gsiJob = createGsiJobFactory.create();
                SubJobTask subJobTask = createGsiJobFactory.rerunTask;
                if (createGlobalIndexPreparedData.isNeedToGetTableGroupLock() && !hasSubJob.get()) {
                    if (StringUtils.isEmpty(subJobTask.getDdlStmt())) {
                        continue;
                    } else {
                        createGsiJobs.add(gsiJob);
                        hasSubJob.set(true);
                        break;
                    }
                } else {
                    createGsiJobs.add(gsiJob);
                }
            }
        }

        if (hasSubJob.get()) {
            createGsiJobs.forEach(ddlJob::appendJob2);
            return ddlJob;
        }
        TableMeta tableMeta = executionContext.getSchemaManager().getTable(primaryTableName);
        TddlRuleManager tddlRuleManager = executionContext.getSchemaManager().getTddlRuleManager();
        // cut over
        RebuildTableCutOverTask cutOverTask =
            new RebuildTableCutOverTask(schemaName, primaryTableName, tableNameMap,
                tableMeta.isAutoPartition(),
                tddlRuleManager.isTableInSingleDb(primaryTableName),
                tddlRuleManager.isBroadCast(primaryTableName)
            );
        ModifyPartitionKeySyncTask
            modifyPartitionKeySyncTask = new ModifyPartitionKeySyncTask(schemaName, primaryTableName, tableNameMap);

        ModifyPartitionKeyRemoveTableStatisticTask removeTableStatisticTask =
            new ModifyPartitionKeyRemoveTableStatisticTask(schemaName, primaryTableName, changedColumns);

        // cdc
        DdlTask cdcDdlMarkTask =
            new CdcModifyPartitionKeyMarkTask(schemaName, primaryTableName, tableNameMap.get(primaryTableName),
                SqlKind.ALTER_TABLE, tableNameMap);

        // drop gsi
        List<ExecutableDdlJob> dropGsiJobs = new ArrayList<>();

        for (Map.Entry<String, String> entries : tableNameMap.entrySet()) {
            String newIndexTableName = entries.getValue();
            DropGlobalIndexPreparedData dropGlobalIndexPreparedData =
                new DropGlobalIndexPreparedData(schemaName, primaryTableName, newIndexTableName, false);
            dropGlobalIndexPreparedData.setRepartition(true);
            dropGlobalIndexPreparedData.setRepartitionTableName(entries.getKey());
            ExecutableDdlJob dropGsiJob =
                DropGsiJobFactory.create(dropGlobalIndexPreparedData, executionContext, false, false);
            // rollback is not supported after CutOver
            dropGsiJob.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
            dropGsiJobs.add(dropGsiJob);
        }

        // table groups sync task
        TableGroupsSyncTask tableGroupsSyncTask = new TableGroupsSyncTask(schemaName,
            oldTableGroupConfigs.stream()
                .map(e -> e.getTableGroupRecord().getTg_name())
                .collect(Collectors.toList())
        );

        List<DdlTask> ddlTasks = new ArrayList<>();
        ddlTasks.add(validateTask);
        if (CollectionUtils.isNotEmpty(alterDefaultColumns)) {
            ddlTasks.add(beginAlterColumnDefault);
            ddlTasks.add(beginAlterColumnDefaultSyncTask);
        }
        ddlTasks.add(rebuildTableChangeMetaTask);
        ddlTasks.add(rebuildSyncTask);

        if (CollectionUtils.isNotEmpty(checkerTasks)) {
            ddlTasks.addAll(checkerTasks);
        }
        ddlJob.addSequentialTasks(ddlTasks);
        for (ExecutableDdlJob exeDdljob : createGsiJobs) {
            if (exeDdljob instanceof ExecutableDdlJob4CreatePartitionGsi) {
                TableGroupConfig tgConfig =
                    ((ExecutableDdlJob4CreatePartitionGsi) (exeDdljob)).getCreateGsiValidateTask()
                        .getTableGroupConfig();
                tgConfig.setPartitionGroupRecords(
                    null);//do not validate tablegroup again, it will do it ModifyPartitionKeyValidateTask
            }
        }
        createGsiJobs.forEach(ddlJob::appendJob2);

        final boolean skipCutOver = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER), "true");
        if (!skipCutOver) {
            ddlJob.appendTask(cdcDdlMarkTask);
            ddlJob.addTaskRelationship(cdcDdlMarkTask, cutOverTask);
            ddlJob.addTaskRelationship(cutOverTask, modifyPartitionKeySyncTask);
            ddlJob.addTaskRelationship(modifyPartitionKeySyncTask, removeTableStatisticTask);
        }

        final boolean skipCleanUp = StringUtils.equalsIgnoreCase(
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CLEANUP), "true");
        if (!skipCleanUp) {
            dropGsiJobs.forEach(ddlJob::appendJob2);

            ddlJob.addTaskRelationship(
                (dropGsiJobs.get(dropGsiJobs.size() - 1)).getTail(),
                tableGroupsSyncTask);

            if (needDropImplicitKey) {
                SubJobTask dropImplicitKeySubJobTask =
                    new SubJobTask(schemaName,
                        String.format("alter table %s drop column %s", surroundWithBacktick(primaryTableName),
                            IMPLICIT_COL_NAME), null);
                dropImplicitKeySubJobTask.setParentAcquireResource(true);

                ddlJob.addTaskRelationship(tableGroupsSyncTask, dropImplicitKeySubJobTask);
            }
        } else {
            if (!skipCutOver) {
                ddlJob.addTaskRelationship(removeTableStatisticTask, tableGroupsSyncTask);
            } else {
                ddlJob.appendTask(tableGroupsSyncTask);
            }
        }

        ddlJob.labelAsHead(validateTask);
        return ddlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        for (String indexTableName : tableNameMap.values()) {
            resources.add(concatWithDot(schemaName, indexTableName));
        }

        // lock table group of primary table
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(primaryTableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    private List<DdlTask> genCheckerTasks() {
        List<DdlTask> result = new ArrayList<>();

        if (MapUtils.isEmpty(virtualColumnMap) || MapUtils.isEmpty(columnNewDef)) {
            return null;
        }

        String tableNameWithBacktick = surroundWithBacktick(primaryTableName);
        virtualColumnMap.forEach((colName, virColName) -> {
            String addSqlFormatter =
                String.format("ALTER TABLE %%s ADD COLUMN `%s` %s GENERATED ALWAYS AS (ALTER_TYPE(`%s`)) VIRTUAL",
                    virColName, columnNewDef.get(colName), colName);
            String dropSqlFormatter = String.format("ALTER TABLE %%s DROP COLUMN `%s`", virColName);
            String addSql = String.format(addSqlFormatter, tableNameWithBacktick);
            String dropSql = String.format(dropSqlFormatter, tableNameWithBacktick);
            String addSqlTemplate = String.format(addSqlFormatter, "?");
            String dropSqlTemplate = String.format(dropSqlFormatter, "?");

            result.add(
                genAlterTablePhyTask(addSql, dropSql, addSqlTemplate, dropSqlTemplate, primaryTableName, "INPLACE"));
        });

        return result;
    }

    private DdlTask genAlterTablePhyTask(String sql, String reverseSql, String sqlTemplate, String reverseSqlTemplate,
                                         String tableName, String algorithm) {
        sql = sql + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSql)) {
            reverseSql = reverseSql + ", ALGORITHM=" + algorithm;
        }

        sqlTemplate = sqlTemplate + ", ALGORITHM=" + algorithm;
        if (!StringUtils.isEmpty(reverseSqlTemplate)) {
            reverseSqlTemplate = reverseSqlTemplate + ", ALGORITHM=" + algorithm;
        }

        PhysicalPlanData newPhysicalPlanData = oldPhysicalPlanData.clone();
        newPhysicalPlanData.setSqlTemplate(sqlTemplate);
        AlterTablePhyDdlTask task;
        task = new AlterTablePhyDdlTask(schemaName, tableName, newPhysicalPlanData);
        task.setSourceSql(sql);
        if (!StringUtils.isEmpty(reverseSql)) {
            task.setRollbackSql(reverseSql);
            task.setRollbackSqlTemplate(reverseSqlTemplate);
        }
        return task;
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    public void setNeedDropImplicitKey(boolean needDropImplicitKey) {
        this.needDropImplicitKey = needDropImplicitKey;
    }

    public void setAlterDefaultColumns(List<String> alterDefaultColumns) {
        this.alterDefaultColumns = alterDefaultColumns;
    }

    public void setChangedColumns(List<String> changedColumns) {
        this.changedColumns = changedColumns;
    }
}
