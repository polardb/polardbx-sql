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
import com.alibaba.polardbx.executor.ddl.job.builder.DdlPhyPlanBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreateGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesExtMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
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
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlIndexColumnName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
    protected final boolean clusteredIndex;
    protected final boolean hasTimestampColumnDefault;
    protected final Map<String, String> binaryColumnDefaultValues;
    protected ExecutionContext executionContext;
    public boolean needOnlineSchemaChange = true;
    public boolean stayAtBackFill = false;

    public CreateGsiJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                               PhysicalPlanData physicalPlanData,
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
                globalIndexPreparedData.getIndexTablePreparedData().getBinaryColumnDefaultValues() :
                new TreeMap<>(String.CASE_INSENSITIVE_ORDER),
            physicalPlanData,
            executionContext
        );
    }

    public static CreateGsiJobFactory create(CreateGlobalIndexPreparedData preparedData,
                                             PhysicalPlanData physicalPlanData,
                                             ExecutionContext ec) {
        final String schema = preparedData.getSchemaName();
        return DbInfoManager.getInstance().isNewPartitionDb(schema) ?
            new CreatePartitionGsiJobFactory(preparedData, physicalPlanData, ec) :
            new CreateGsiJobFactory(preparedData, physicalPlanData, ec);
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
                                  Map<String, String> binaryColumnDefaultValues,
                                  PhysicalPlanData physicalPlanData,
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
        this.clusteredIndex = clusteredIndex;
        this.hasTimestampColumnDefault = hasTimestampColumnDefault;
        this.binaryColumnDefaultValues = binaryColumnDefaultValues;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        GsiValidator.validateGsiSupport(schemaName, executionContext);
        GsiValidator.validateCreateOnGsi(schemaName, indexTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        CreateGsiValidateTask validateTask =
            new CreateGsiValidateTask(schemaName, primaryTableName, indexTableName, null, null);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
        final boolean stayAtDeleteOnly = StringUtils.equalsIgnoreCase(DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly = StringUtils.equalsIgnoreCase(WRITE_ONLY.name(), finalStatus);

        List<String> columns = columnAst2nameStr(this.columns);
        List<String> coverings = columnAst2nameStr(this.coverings);
        List<DdlTask> bringUpGsi = null;
        if (needOnlineSchemaChange) {
            bringUpGsi = GsiTaskFactory.addGlobalIndexTasks(
                schemaName,
                primaryTableName,
                indexTableName,
                stayAtDeleteOnly,
                stayAtWriteOnly,
                stayAtBackFill
            );
        } else {
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
                hasTimestampColumnDefault,
                binaryColumnDefaultValues
            );
        CreateTableShowTableMetaTask showTableMetaTask = new CreateTableShowTableMetaTask(schemaName, indexTableName);
        GsiInsertIndexMetaTask addIndexMetaTask =
            new GsiInsertIndexMetaTask(
                schemaName,
                primaryTableName,
                indexTableName,
                columns,
                coverings,
                unique,
                indexComment,
                indexType,
                IndexStatus.CREATING,
                clusteredIndex
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

    protected List<String> columnAst2nameStr(List<SqlIndexColumnName> columnDefList) {
        if (CollectionUtils.isEmpty(columnDefList)) {
            return new ArrayList<>();
        }
        return columnDefList.stream()
            .map(e -> e.getColumnNameStr())
            .collect(Collectors.toList());
    }

    /**
     * for create table with gsi
     * needOnlineSchemaChange = false, will skip Online Schema Change and BackFill
     */
    public static ExecutableDdlJob create4CreateTableWithGsi(@Deprecated DDL ddl,
                                                             CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                             ExecutionContext ec) {
        boolean autoPartition = globalIndexPreparedData.getIndexTablePreparedData().isAutoPartition();
        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(ddl, globalIndexPreparedData, ec).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData(autoPartition);

        CreateGsiJobFactory gsiJobFactory = new CreateGsiJobFactory(globalIndexPreparedData, physicalPlanData, ec);
        gsiJobFactory.needOnlineSchemaChange = false;
        return gsiJobFactory.create();
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          CreateGlobalIndexPreparedData preparedData,
                                          ExecutionContext ec) {
        DdlPhyPlanBuilder builder = new CreateGlobalIndexBuilder(ddl, preparedData, ec).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        CreateGsiJobFactory gsiJobFactory = CreateGsiJobFactory.create(preparedData, physicalPlanData, ec);
        return gsiJobFactory.create();
    }

}
