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

package com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.DdlJobDataConverter;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcCreateColumnarIndexTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.*;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CciUpdateIndexStatusTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreateColumnarIndex;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.tablegroup.TableGroupDetailConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateTablePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlCreateIndex;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.isVersionIdInitialized;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.columnAst2nameStr;

/**
 * create cluster columnar index
 */
public class CreateColumnarIndexJobFactory extends DdlJobFactory {

    private static final PartitionStrategy DEFAULT_PARTITION_STRATEGY = PartitionStrategy.KEY;

    protected final String schemaName;
    protected final String primaryTableName;
    protected final String columnarIndexTableName;
    protected final boolean clusteredIndex;
    protected final List<String> primaryKeys;
    protected final List<String> coverings;
    protected final String indexComment;
    protected final String indexType;
    protected final CreateTablePreparedData preparedData;
    protected final Engine engine;
    protected List<String> partitionKeys = null;
    protected PartitionStrategy partitionStrategy = null;
    protected List<String> sortKeys;
    protected final SqlCreateIndex sqlCreateIndex;
    protected ExecutionContext executionContext;
    protected PartitionInfo partitionInfo = null;

    /**
     * FOR TEST USE ONLY!
     */
    protected final Set<String> skipDdlTasks;
    /**
     * For create table with cci,
     * {@link TableSyncTask} of primary table should be executed after {@link CreateTableShowTableMetaTask} task finished
     */
    protected final boolean createTableWithCci;
    protected final Long versionId;

    public CreateColumnarIndexJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                         ExecutionContext executionContext) {
        this(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexTableName(),
            globalIndexPreparedData.isClusteredIndex(),
            globalIndexPreparedData.getIndexTablePreparedData(),
            globalIndexPreparedData.getColumns(),
            globalIndexPreparedData.getCoverings(),
            globalIndexPreparedData.getComment(),
            globalIndexPreparedData.getIndexType(),
            globalIndexPreparedData.getOrBuildOriginCreateIndex(),
            globalIndexPreparedData.getEngineName(),
            globalIndexPreparedData.isCreateTableWithIndex(),
            globalIndexPreparedData.getIndexPartitionInfo(),
            globalIndexPreparedData.getDdlVersionId(),
            executionContext
        );
    }

    protected CreateColumnarIndexJobFactory(String schemaName,
                                            String primaryTableName,
                                            String indexTableName,
                                            boolean clusteredIndex,
                                            CreateTablePreparedData preparedData,
                                            List<SqlIndexColumnName> sortKeys,
                                            List<SqlIndexColumnName> covering,
                                            String indexComment,
                                            String indexType,
                                            SqlCreateIndex sqlCreateIndex,
                                            SqlNode engineName,
                                            boolean createTableWithCci,
                                            PartitionInfo indexPartitionInfo,
                                            Long versionId,
                                            ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.columnarIndexTableName = indexTableName;
        this.clusteredIndex = clusteredIndex;
        this.preparedData = preparedData;
        this.sortKeys = columnAst2nameStr(sortKeys);
        this.coverings = columnAst2nameStr(covering);
        this.indexComment = indexComment == null ? "" : indexComment;
        this.indexType = indexType;
        this.sqlCreateIndex = sqlCreateIndex;
        this.executionContext = executionContext;
        // CreateTablePreparedData#tableMeta is the metadata of primary table
        // Getting metadata of primary table from schema manager is not always possible
        // For case like CREATE TABLE with CCI, there might not be published metadata at this step
        this.primaryKeys =
            preparedData.getTableMeta().getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        this.skipDdlTasks = executionContext.skipDdlTasks();
        this.createTableWithCci = createTableWithCci;
        this.partitionInfo = indexPartitionInfo;

        if (engineName == null) {
            this.engine = Engine.DEFAULT_COLUMNAR_ENGINE;
        } else {
            this.engine = Engine.of(engineName.toString());
        }
        Preconditions.checkNotNull(versionId);
        Preconditions.checkArgument(isVersionIdInitialized(versionId), "Ddl versionId is not initialized");
        this.versionId = versionId;
        if (null != executionContext.getDdlContext()) {
            executionContext.getDdlContext().setPausedPolicy(DdlState.PAUSED);
        }
    }

    public static CreateColumnarIndexJobFactory create(CreateGlobalIndexPreparedData preparedData,
                                                       ExecutionContext ec) {
        final String schema = preparedData.getSchemaName();
        if (!DbInfoManager.getInstance().isNewPartitionDb(schema)) {
            throw new UnsupportedOperationException("clustered columnar index is not supported in DRDS mode");
        }
        return new CreateColumnarIndexJobFactory(preparedData, ec);
    }

    @Override
    protected void validate() {
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, columnarIndexTableName, executionContext);
        validateEngine();
        validateColumnarTableKeys();
        validateDictColumns();
    }

    private void validateDictColumns() {
        TableMeta tableMeta = this.preparedData.getTableMeta();
        List<SqlIndexColumnName> dictColumns = sqlCreateIndex.getDictColumns();
        if (dictColumns != null) {
            for (SqlIndexColumnName dictCol : dictColumns) {
                String columnNameStr = dictCol.getColumnNameStr();
                ColumnMeta column = tableMeta.getColumnIgnoreCase(columnNameStr);
                if (column == null) {
                    throw new TddlNestableRuntimeException("unknown dictionary column name: " + columnNameStr);
                }
            }
        }
    }

    private void validateEngine() {
        if (!Engine.supportColumnar(engine)) {
            throw new UnsupportedOperationException("Engine: " + engine + " is not supported in columnar index");
        }
    }

    private void validateColumnarTableKeys() {
        Preconditions.checkArgument(!primaryKeys.isEmpty(),
            "Columnar index only supports need primary key");
        Preconditions.checkArgument(clusteredIndex,
            "Do not support columnar index which is not clustered");
        TableMeta tableMeta = this.preparedData.getTableMeta();

        buildColumnarPartitionKey();
        buildColumnarSortKey();
        buildPartitionInfo(tableMeta);
    }

    private void buildPartitionInfo(TableMeta tableMeta) {
        ColumnMeta pkMeta = tableMeta.getColumn(primaryKeys.get(0));
        List<ColumnMeta> pkColMetas = Collections.singletonList(pkMeta);
        List<ColumnMeta> allColMetas = tableMeta.getAllColumns();

        String tableGroupName = preparedData.getTableGroupName() == null ? null :
            ((SqlIdentifier) preparedData.getTableGroupName()).getLastName();
        String joinGroupName = preparedData.getJoinGroupName() == null ? null :
            ((SqlIdentifier) preparedData.getJoinGroupName()).getLastName();
        partitionInfo = PartitionInfoBuilder.buildPartitionInfoByPartDefAst(
            preparedData.getSchemaName(), columnarIndexTableName,
            tableGroupName, preparedData.isWithImplicitTableGroup(), joinGroupName,
            (SqlPartitionBy) preparedData.getPartitioning(), preparedData.getPartBoundExprInfo(),
            pkColMetas, allColMetas, PartitionTableType.COLUMNAR_TABLE,
            executionContext, new LocalityDesc());
    }

    private void buildCoverings(TableMeta tableMeta) {
        if (!clusteredIndex) {
            throw new UnsupportedOperationException("Do not support columnar index which is not clustered");
        }
    }

    /**
     * Build partition key by primary key,
     * using default partition strategy
     */
    private void buildColumnarPartitionKey() {
        if (preparedData.getPartitioning() instanceof SqlPartitionBy) {
            SqlPartitionBy partitionBy = (SqlPartitionBy) preparedData.getPartitioning();
            List<SqlNode> partitionColumns = partitionBy.getColumns();
            this.partitionKeys = partitionColumns.stream().map(SqlNode::toString).collect(Collectors.toList());
            this.partitionStrategy = PartitionInfoBuilder.getPartitionStrategy(partitionBy);
        } else {
            partitionKeys = Collections.singletonList(primaryKeys.get(0));
            partitionStrategy = DEFAULT_PARTITION_STRATEGY;
        }
    }

    private void buildColumnarSortKey() {
        if (CollectionUtils.isEmpty(sortKeys)) {
            sortKeys = ImmutableList.copyOf(primaryKeys);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        final TableGroupDetailConfig tableGroupConfig = DdlJobDataConverter.buildTableGroupConfig(partitionInfo, false);

        final List<DdlTask> taskList = new ArrayList<>();
        // 1. validate
        final CreateColumnarIndexValidateTask validateTask = new CreateColumnarIndexValidateTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName);
        taskList.add(validateTask);

        // 2. create columnar table
        // 2.1 insert tablePartition meta for columnar table
        final AddColumnarTablesPartitionInfoMetaTask addColumnarTablesPartitionInfoMetaTask =
            new AddColumnarTablesPartitionInfoMetaTask(
                schemaName,
                columnarIndexTableName,
                tableGroupConfig,
                primaryTableName);
        taskList.add(addColumnarTablesPartitionInfoMetaTask);

        // 2.2 insert tables meta for columnar table
        final AddColumnarTablesMetaTask addColumnarTablesMetaTask = new AddColumnarTablesMetaTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName,
            versionId, engine);
        taskList.add(addColumnarTablesMetaTask);

        // Change tables.status
        final CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(
            schemaName,
            columnarIndexTableName);
        taskList.add(showTableMetaTask);

        // 2.3 insert indexes meta for primary table
        final InsertColumnarIndexMetaTask insertColumnarIndexMetaTask = new InsertColumnarIndexMetaTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName,
            sortKeys,
            coverings,
            false,
            indexComment,
            indexType,
            IndexStatus.CREATING,
            clusteredIndex);
        taskList.add(insertColumnarIndexMetaTask);

        // 2.4 CDC mark create columnar table
        CdcCreateColumnarIndexTask cdcCreateColumnarIndexTask = null;
        CreateMockColumnarIndexTask createMockColumnarIndexTask = null;
        if (executionContext.getParamManager().getBoolean(ConnectionParams.MOCK_COLUMNAR_INDEX)) {
            //create mock columnar index
            createMockColumnarIndexTask =
                new CreateMockColumnarIndexTask(schemaName, columnarIndexTableName, versionId);
            createMockColumnarIndexTask.setMciFormat(
                executionContext.getParamManager().getString(ConnectionParams.MCI_FORMAT));
            taskList.add(createMockColumnarIndexTask);
        } else if (!createTableWithCci) {
            // 2.4 CDC mark create columnar table
            cdcCreateColumnarIndexTask = new CdcCreateColumnarIndexTask(
                schemaName,
                primaryTableName,
                columnarIndexTableName,
                sqlCreateIndex.getOriginIndexName().getLastName(),
                sqlCreateIndex.getColumnarOptions(),
                sqlCreateIndex.toString(true),
                versionId);
            taskList.add(cdcCreateColumnarIndexTask);

            // 2.5 table sync
            DdlTask tableSyncTask = new TableSyncTask(schemaName, columnarIndexTableName);
            taskList.add(tableSyncTask);
        }

        // 3.1.1 wait columnar table creation
        final WaitColumnarTableCreationTask waitColumnarTableCreationTask = new WaitColumnarTableCreationTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName,
            skipDdlTasks.contains(WaitColumnarTableCreationTask.class.getSimpleName()));
        taskList.add(waitColumnarTableCreationTask);

        // 3.1.2 check consistency
        final CciUpdateIndexStatusTask changeCreatingToChecking =
            (CciUpdateIndexStatusTask) new CciUpdateIndexStatusTask(
                schemaName,
                primaryTableName,
                columnarIndexTableName,
                ColumnarTableStatus.CREATING,
                ColumnarTableStatus.PUBLIC,
                IndexStatus.CREATING,
                IndexStatus.WRITE_REORG,
                false
            ).onExceptionTryRecoveryThenRollback();
        taskList.add(changeCreatingToChecking);

        DdlTask checkingTableSyncTask = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(checkingTableSyncTask);

        final CreateCheckCciTask createCheckCciTask = new CreateCheckCciTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName,
            skipDdlTasks.contains(CreateCheckCciTask.class.getSimpleName())
        );
        createCheckCciTask.setExceptionAction(DdlExceptionAction.PAUSE);
        taskList.add(createCheckCciTask);

        // 3.2 change cci status to PUBLIC
        final CciUpdateIndexStatusTask updateCciStatusTask = (CciUpdateIndexStatusTask) new CciUpdateIndexStatusTask(
            schemaName,
            primaryTableName,
            columnarIndexTableName,
            ColumnarTableStatus.PUBLIC,
            ColumnarTableStatus.PUBLIC,
            IndexStatus.WRITE_REORG,
            IndexStatus.PUBLIC,
            false
        ).onExceptionTryRecoveryThenRollback();
        taskList.add(updateCciStatusTask);

        // 3.3 final table sync
        DdlTask finalTableSyncTask = new TableSyncTask(schemaName, primaryTableName);
        taskList.add(finalTableSyncTask);

        final ExecutableDdlJob4CreateColumnarIndex result = new ExecutableDdlJob4CreateColumnarIndex();
        result.addSequentialTasks(taskList);

        result.setCreateColumnarIndexValidateTask(validateTask);
        result.setAddColumnarTablesPartitionInfoMetaTask(addColumnarTablesPartitionInfoMetaTask);
        result.setCdcCreateColumnarIndexTask(cdcCreateColumnarIndexTask);
        result.setCreateMockColumnarIndexTask(createMockColumnarIndexTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setInsertColumnarIndexMetaTask(insertColumnarIndexMetaTask);
        result.setWaitColumnarTableCreationTask(waitColumnarTableCreationTask);
        result.setChangeCreatingToChecking(changeCreatingToChecking);
        result.setCreateCheckCciTask(createCheckCciTask);
        result.setCciUpdateIndexStatusTask(updateCciStatusTask);

        result.setLastTask(taskList.get(taskList.size() - 1));
        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));
        resources.add(concatWithDot(schemaName, columnarIndexTableName));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    public static ExecutableDdlJob create4CreateCci(@Deprecated DDL ddl,
                                                    CreateGlobalIndexPreparedData preparedData,
                                                    ExecutionContext ec) {
        Preconditions.checkArgument(preparedData.isClusteredIndex(), "Columnar index can only be clustered index");

        final CreateGlobalIndexBuilder builder =
            new CreatePartitionGlobalIndexBuilder(ddl, preparedData, null, false, ec);
        builder.build();

        final CreateColumnarIndexJobFactory gsiJobFactory = CreateColumnarIndexJobFactory.create(preparedData, ec);
        return gsiJobFactory.create();
    }
}
