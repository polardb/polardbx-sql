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
import com.alibaba.polardbx.executor.ddl.job.builder.gsi.CreatePartitionGlobalIndexBuilder;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableAddTablesPartitionInfoMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreateTableShowTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.factory.GsiTaskFactory;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiPhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CreateGsiValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.GsiInsertIndexMetaTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4CreatePartitionGsi;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.DELETE_ONLY;
import static com.alibaba.polardbx.gms.metadb.table.IndexStatus.WRITE_ONLY;

/**
 * 1. create [unique] global index
 * 2. alter table xxx add [unique] global index
 * <p>
 * for create table with [unique] gsi, see class: CreateTableWithGsiJobFactory
 *
 * @author guxu
 */
public class CreatePartitionGsiJobFactory extends CreateGsiJobFactory {

    public CreatePartitionGsiJobFactory(CreateGlobalIndexPreparedData globalIndexPreparedData,
                                        PhysicalPlanData physicalPlanData,
                                        ExecutionContext executionContext) {
        super(
            globalIndexPreparedData.getSchemaName(),
            globalIndexPreparedData.getPrimaryTableName(),
            globalIndexPreparedData.getIndexTableName(),
            globalIndexPreparedData.getColumns(),
            globalIndexPreparedData.getCoverings(),
            globalIndexPreparedData.isUnique(),
            globalIndexPreparedData.getComment(),
            globalIndexPreparedData.getIndexType(),
            globalIndexPreparedData.isClusteredIndex(),
            physicalPlanData,
            executionContext
        );
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        CreateGsiValidateTask validateTask =
            new CreateGsiValidateTask(schemaName, primaryTableName, indexTableName);

        List<String> columns = columnAst2nameStr(this.columns);
        List<String> coverings = columnAst2nameStr(this.coverings);

        final String finalStatus =
            executionContext.getParamManager().getString(ConnectionParams.GSI_FINAL_STATUS_DEBUG);
        final boolean stayAtDeleteOnly = StringUtils.equalsIgnoreCase(DELETE_ONLY.name(), finalStatus);
        final boolean stayAtWriteOnly = StringUtils.equalsIgnoreCase(WRITE_ONLY.name(), finalStatus);

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
        CreateTableAddTablesPartitionInfoMetaTask createTableAddTablesPartitionInfoMetaTask =
            new CreateTableAddTablesPartitionInfoMetaTask(schemaName, indexTableName, physicalPlanData.isTemporary(),
                physicalPlanData.getTableGroupConfig());
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
                physicalPlanData.getKind()
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
        //2.1 insert tablePartition meta for gsi table
        taskList.add(createTableAddTablesPartitionInfoMetaTask);
        //2.2 create gsi physical table
        CreateGsiPhyDdlTask createGsiPhyDdlTask =
            new CreateGsiPhyDdlTask(schemaName, primaryTableName, indexTableName, physicalPlanData);
        taskList.add(createGsiPhyDdlTask);
        //2.3 insert tables meta for gsi table
        taskList.add(addTablesMetaTask);

        taskList.add(showTableMetaTask);
        //3.1 insert indexes meta for primary table
        taskList.add(addIndexMetaTask);
//        taskList.add(new GsiSyncTask(schemaName, primaryTableName, indexTableName));
        //3.2 gsi status: CREATING -> DELETE_ONLY -> WRITE_ONLY -> WRITE_REORG -> PUBLIC
        taskList.addAll(bringUpGsi);

        DdlTask tableSyncTask = new TableSyncTask(schemaName, indexTableName);
        taskList.add(tableSyncTask);

        final ExecutableDdlJob4CreatePartitionGsi result = new ExecutableDdlJob4CreatePartitionGsi();
        result.addSequentialTasks(taskList);
        //todo delete me
        result.labelAsHead(validateTask);
        result.labelAsTail(tableSyncTask);

        result.setCreateGsiValidateTask(validateTask);
        result.setCreateTableAddTablesPartitionInfoMetaTask(createTableAddTablesPartitionInfoMetaTask);
        result.setCreateTableAddTablesMetaTask(addTablesMetaTask);
        result.setCreateTableShowTableMetaTask(showTableMetaTask);
        result.setGsiInsertIndexMetaTask(addIndexMetaTask);
        result.setCreateGsiPhyDdlTask(createGsiPhyDdlTask);
        result.setLastUpdateGsiStatusTask(bringUpGsi.get(bringUpGsi.size() - 1));

        result.setLastTask(tableSyncTask);

        return result;
    }

    public static ExecutableDdlJob create(@Deprecated DDL ddl,
                                          CreateGlobalIndexPreparedData globalIndexPreparedData,
                                          ExecutionContext executionContext) {
        DdlPhyPlanBuilder builder =
            CreateGlobalIndexBuilder.create(ddl, globalIndexPreparedData, executionContext).build();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData();

        return CreateGsiJobFactory.create(globalIndexPreparedData, physicalPlanData, executionContext).create();
    }

    public static ExecutableDdlJob create4CreateTableWithGsi(@Deprecated DDL ddl,
                                                             CreateGlobalIndexPreparedData globalIndexPreparedData,
                                                             ExecutionContext ec) {
        CreateGlobalIndexBuilder builder = new CreatePartitionGlobalIndexBuilder(ddl, globalIndexPreparedData, ec);
        builder.build();

        boolean autoPartition = globalIndexPreparedData.getIndexTablePreparedData().isAutoPartition();
        PhysicalPlanData physicalPlanData = builder.genPhysicalPlanData(autoPartition);

        CreateGsiJobFactory gsiJobFactory = new CreatePartitionGsiJobFactory(
            globalIndexPreparedData,
            physicalPlanData,
            ec
        );
        gsiJobFactory.needOnlineSchemaChange = false;
        return gsiJobFactory.create();
    }
}
