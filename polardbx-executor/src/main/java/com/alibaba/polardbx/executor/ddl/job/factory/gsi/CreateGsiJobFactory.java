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

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesExtMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertIndexMetaTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateGsi;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.columnAst2SubPart;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.columnAst2nameStr;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.DELETE_ONLY;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.WRITE_ONLY;

/**
 * 1. create [global|clustered] [unique] index
 * 2. alter table xxx add [global|clustered] [unique] index
 * <p>
 * for create table with [global|clustered] [unique] index, see class: CreateTableWithGsiJobFactory
 *
 * @author guxu
 */
public class CreateGsiJobFactory extends DdlJobFactory {

    protected final String schemaName;
    protected final String primaryTableName;
    protected final String indexTableName;
    protected final List<SqlIndexColumnName> columns;
    protected final List<SqlIndexColumnName> coverings;
    protected final boolean unique;
    protected final String indexComment;
    protected final String indexType;
    protected PhysicalPlanData physicalPlanData;
    protected PhysicalPlanData physicalPlanDataForLocalIndex;
    protected final boolean clusteredIndex;
    protected final boolean hasTimestampColumnDefault;
    protected final Map<String, String> specialDefaultValues;
    protected final Map<String, Long> specialDefaultValueFlags;
    protected ExecutionContext executionContext;
    public boolean needOnlineSchemaChange = true;
    public boolean onlineModifyColumn = false;
    public boolean useChangeSet = false;
    public boolean mirrorCopy = false;
    public boolean stayAtBackFill = false;
    public boolean buildBroadCast = false;
    public boolean splitByPkRange = false;
    public boolean splitByPartition = false;

    /**
     * Foreign key
     */
    protected List<String> referencedTables;
    protected List<ForeignKeyData> addedForeignKeys;

    /**
     * for online modify column checker
     */
    public Map<String, String> srcVirtualColumnMap = null;
    public Map<String, String> dstVirtualColumnMap = null;
    public Map<String, SQLColumnDefinition> dstColumnNewDefinitions = null;

    /**
     * for online modify column (change column name), column map, oldName ----> newName
     */
    public Map<String, String> omcColumnMap = null;

    public List<String> modifyStringColumns = null;

    public List<String> addNewColumns = null;

    /**
     * for online modify column, backfillSourceTable ----> newIndex
     */
    public String backfillSourceTableName;

    public SubJobTask rerunTask;

    private boolean visible;

    protected boolean gsiCdcMark = true;

    protected boolean removePartitioning = false;

    public CreateGsiJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                               PhysicalPlanData physicalPlanData,
                               PhysicalPlanData physicalPlanDataForLocalIndex,
                               Boolean create4CreateTableWithGsi,
                               ExecutionContext executionContext) {
        this(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexTableName(),
            globalIndexPreparedData.getColumns(),
            globalIndexPreparedData.getCoverings(),
            globalIndexPreparedData.isUnique(),
            globalIndexPreparedData.getComment(),
            globalIndexPreparedData.getIndexType(),
            globalIndexPreparedData.isClusteredIndex(),
            globalIndexPreparedData.getIndexTablePreparedData() != null
                && globalIndexPreparedData.getIndexTablePreparedData().isTimestampColumnDefault(),
            globalIndexPreparedData.getIndexTablePreparedData() != null ?
                globalIndexPreparedData.getIndexTablePreparedData().getSpecialDefaultValues() :
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
            globalIndexPreparedData.getIndexTablePreparedData() != null ?
                globalIndexPreparedData.getIndexTablePreparedData().getSpecialDefaultValueFlags() :
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_BACKFILL_BY_PK_RANGE),
            executionContext.getParamManager().getBoolean(ConnectionParams.GSI_BACKFILL_BY_PARTITION),
            physicalPlanData,
            physicalPlanDataForLocalIndex,
            create4CreateTableWithGsi,
            globalIndexPreparedData.getAddedForeignKeys(),
            executionContext
        );

        this.visible = globalIndexPreparedData.isVisible();
        this.gsiCdcMark = !globalIndexPreparedData.isRepartition();
    }

    public static CreateGsiJobFactory create(CreateGlobalIndexPreparedData preparedData,
                                             PhysicalPlanData physicalPlanData,
                                             PhysicalPlanData physicalPlanDataForLocalIndex,
                                             ExecutionContext ec) {
        final String schema = preparedData.getSchemaName();
        return DbInfoManager.getInstance().isNewPartitionDb(schema) ?
            new CreatePartitionGsiJobFactory(preparedData, physicalPlanData, physicalPlanDataForLocalIndex, false, ec) :
            new CreateGsiJobFactory(preparedData, physicalPlanData, physicalPlanDataForLocalIndex, false, ec);
    }

    protected CreateGsiJobFactory(String schemaName,
                                  String primaryTableName,
                                  String indexTableName,
                                  List<SqlIndexColumnName> columns,
                                  List<SqlIndexColumnName> coverings,
                                  boolean unique,
                                  String indexComment,
                                  String indexType,
                                  boolean clusteredIndex,
                                  boolean hasTimestampColumnDefault,
                                  Map<String, String> specialDefaultValues,
                                  Map<String, Long> specialDefaultValueFlags,
                                  boolean splitByPkRange,
                                  boolean splitByPartition,
                                  PhysicalPlanData physicalPlanData,
                                  PhysicalPlanData physicalPlanDataForLocalIndex,
                                  Boolean create4CreateTableWithGsi,
                                  List<ForeignKeyData> addedForeignKeys,
                                  ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.indexTableName = indexTableName;
        this.columns = columns;
        this.coverings = coverings;
        this.unique = unique;
        this.indexComment = indexComment == null ? "" : indexComment;
        this.indexType = indexType;
        this.physicalPlanData = physicalPlanData;
        this.physicalPlanDataForLocalIndex = physicalPlanDataForLocalIndex;
        this.clusteredIndex = clusteredIndex;
        this.hasTimestampColumnDefault = hasTimestampColumnDefault;
        this.specialDefaultValues = specialDefaultValues;
        this.specialDefaultValueFlags = specialDefaultValueFlags;
        this.executionContext = executionContext;
        this.addedForeignKeys = addedForeignKeys;
        if (!unique && !create4CreateTableWithGsi) {
            this.splitByPkRange = splitByPkRange;
        }
        this.splitByPartition = splitByPartition;
    }

    @Override
    protected void validate() {
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        CreateGsiValidateTask validateTask =
            new CreateGsiValidateTask(schemaName, primaryTableName, indexTableName, null, null, removePartitioning, false);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
        final boolean stayAtDeleteOnly = StringUtils.equalsIgnoreCase(DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly = StringUtils.equalsIgnoreCase(WRITE_ONLY.name(), finalStatus);

        Map<String, String> columnMapping = omcColumnMap == null ? null :
            omcColumnMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));

        List<String> columns = columnAst2nameStr(this.columns);
        List<Long> subParts = columnAst2SubPart(this.columns);
        List<String> coverings = columnAst2nameStr(this.coverings);
        List<DdlTask> bringUpGsi = null;
        if (useChangeSet) {
            // online modify column
            bringUpGsi = GsiTaskFactory.addGlobalIndexTasksChangeSet(
                schemaName,
                primaryTableName,
                backfillSourceTableName,
                indexTableName,
                stayAtDeleteOnly,
                stayAtWriteOnly,
                stayAtBackFill,
                srcVirtualColumnMap,
                dstVirtualColumnMap,
                dstColumnNewDefinitions,
                modifyStringColumns,
                onlineModifyColumn,
                mirrorCopy,
                physicalPlanData,
                null
            );
        } else if (needOnlineSchemaChange) {
            // add index, use schema change
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(primaryTableName);
            bringUpGsi = GsiTaskFactory.addGlobalIndexTasks(
                schemaName,
                primaryTableName,
                backfillSourceTableName,
                indexTableName,
                stayAtDeleteOnly,
                stayAtWriteOnly,
                stayAtBackFill,
                srcVirtualColumnMap,
                dstVirtualColumnMap,
                dstColumnNewDefinitions,
                modifyStringColumns,
                physicalPlanData,
                physicalPlanDataForLocalIndex,
                tableMeta,
                gsiCdcMark,
                onlineModifyColumn,
                mirrorCopy,
                executionContext.getOriginSql()
            );
        } else {
            // create table with gsi, not use schema change
            bringUpGsi = GsiTaskFactory.createGlobalIndexTasks(
                schemaName,
                primaryTableName,
                indexTableName
            );
        }
        CreateTableAddTablesExtMetaTask addTablesExtMetaTask =
            new CreateTableAddTablesExtMetaTask(
                schemaName,
                indexTableName,
                physicalPlanData.isTemporary(),
                physicalPlanData.getTablesExtRecord(),
                physicalPlanData.getTablesExtRecord().isAutoPartition()
            );
        CreateTableAddTablesMetaTask addTablesMetaTask =
            new CreateTableAddTablesMetaTask(
                schemaName,
                indexTableName,
                physicalPlanData.getDefaultDbIndex(),
                physicalPlanData.getDefaultPhyTableName(),
                physicalPlanData.getSequence(),
                physicalPlanData.getTablesExtRecord(),
                physicalPlanData.isPartitioned(),
                physicalPlanData.isIfNotExists(),
                physicalPlanData.getKind(),
                addedForeignKeys,
                hasTimestampColumnDefault,
                specialDefaultValues,
                specialDefaultValueFlags,
                columnMapping,
                addNewColumns);
        CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(schemaName, indexTableName);
        GsiInsertIndexMetaTask addIndexMetaTask =
            new GsiInsertIndexMetaTask(
                schemaName,
                primaryTableName,
                indexTableName,
                columns,
                subParts,
                coverings,
                unique,
                indexComment,
                indexType,
                IndexStatus.CREATING,
                clusteredIndex,
                visible ? IndexVisibility.VISIBLE : IndexVisibility.INVISIBLE,
                LackLocalIndexStatus.convert(physicalPlanData.isPopLocalIndex()),
                needOnlineSchemaChange,
                columnMapping,
                addNewColumns
            );

        List<DdlTask> taskList = new ArrayList<>();
        //1. validate
        taskList.add(validateTask);

        //2. create gsi table
        //2.1 insert tables_ext meta for gsi table
        taskList.add(addTablesExtMetaTask);
        //2.2 create gsi physical table
        CreateGsiPhyDdlTask createGsiPhyDdlTask =
            new CreateGsiPhyDdlTask(schemaName, primaryTableName, indexTableName, physicalPlanData);
        taskList.add(createGsiPhyDdlTask);
        //2.3 insert tables meta for gsi table
        taskList.add(addTablesMetaTask);
        taskList.add(showTableMetaTask.onExceptionTryRecoveryThenRollback());

        //3.1 insert indexes meta for primary table
        taskList.add(addIndexMetaTask.onExceptionTryRecoveryThenRollback());
        //3.2 gsi status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> PUBLIC
        taskList.addAll(bringUpGsi);

        final ExecutableDdlJob4CreateGsi result = new ExecutableDdlJob4CreateGsi();
        result.addSequentialTasks(taskList);
        //todo delete me
        result.labelAsHead(validateTask);
        result.labelAsTail(bringUpGsi.get(bringUpGsi.size() - 1));

        result.setCreateGsiValidateTask(validateTask);
        result.setCreateTableAddTablesExtMetaTask(addTablesExtMetaTask);
        result.setCreateTableAddTablesMetaTask(addTablesMetaTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setGsiInsertIndexMetaTask(addIndexMetaTask);
        result.setCreateGsiPhyDdlTask(createGsiPhyDdlTask);
        result.setLastUpdateGsiStatusTask(bringUpGsi.get(bringUpGsi.size() - 1));

        result.setLastTask(bringUpGsi.get(bringUpGsi.size() - 1));

//        result.setExceptionActionForAllTasks(DdlExceptionAction.TRY_RECOVERY_THEN_ROLLBACK);
//        result.setExceptionActionForAllSuccessor();

        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, indexTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    /**
     * for create table with gsi
     * needOnlineSchemaChange = false, will skip Online Schema Change and BackFill
     */
    public static ExecutableDdlJob create4CreateTableWithGsi(@Deprecated DDL ddl,
                                                             CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                             ExecutionContext ec) {
        boolean autoPartition = globalIndexPreparedData.getIndexTablePreparedData().isAutoPartition();
        ec.setForbidBuildLocalIndexLater(true);
        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(ddl, globalIndexPreparedData, ec).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData(autoPartition);
        ec.getDdlContext().setIgnoreCdcGsiMark(true);
        CreateGsiJobFactory gsiJobFactory =
            new CreateGsiJobFactory(globalIndexPreparedData, physicalPlanData, null, true, ec);
        gsiJobFactory.needOnlineSchemaChange = false;
        return gsiJobFactory.create();
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          CreateGlobalIndexPreparedData preparedData,
                                          ExecutionContext ec) {
        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(ddl, preparedData, ec).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();
        PhysicalPlanData physicalPlanDataForLocalIndex = DdlPhyPlanBuilder.getPhysicalPlanDataForLocalIndex(builder, false);

        CreateGsiJobFactory gsiJobFactory = CreateGsiJobFactory.create(preparedData, physicalPlanData, physicalPlanDataForLocalIndex, ec);
        return gsiJobFactory.create();
    }

    public void setStayAtBackFill(boolean stayAtBackFill) {
        this.stayAtBackFill = stayAtBackFill;
    }

    public void setSrcVirtualColumnMap(Map<String, String> srcVirtualColumnMap) {
        this.srcVirtualColumnMap = srcVirtualColumnMap;
    }

    public void setDstVirtualColumnMap(Map<String, String> dstVirtualColumnMap) {
        this.dstVirtualColumnMap = dstVirtualColumnMap;
    }

    public void setDstColumnNewDefinitions(Map<String, SQLColumnDefinition> dstColumnNewDefinitions) {
        this.dstColumnNewDefinitions = dstColumnNewDefinitions;
    }

    public void setOmcColumnMap(Map<String, String> omcColumnMap) {
        this.omcColumnMap = omcColumnMap;
    }

    public void setBackfillSourceTableName(String backfillSourceTableName) {
        this.backfillSourceTableName = backfillSourceTableName;
    }

    public void setOnlineModifyColumn(boolean onlineModifyColumn) {
        this.onlineModifyColumn = onlineModifyColumn;
    }

    public void setUseChangeSet(boolean useChangeSet) {
        this.useChangeSet = useChangeSet;
    }

    public void setMirrorCopy(boolean mirrorCopy) {
        this.mirrorCopy = mirrorCopy;
    }

    public void setModifyStringColumns(List<String> modifyStringColumns) {
        this.modifyStringColumns = modifyStringColumns;
    }

    public void setAddNewColumns(List<String> addNewColumns) {
        this.addNewColumns = addNewColumns;
    }

    public void setGsiCdcMark(boolean gsiCdcMark) {
        this.gsiCdcMark = gsiCdcMark;
    }

    public void setRemovePartitioning(boolean removePartitioning) {
        this.removePartitioning = removePartitioning;
    }
}
