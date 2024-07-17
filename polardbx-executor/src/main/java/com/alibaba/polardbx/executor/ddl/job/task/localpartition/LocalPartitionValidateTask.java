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

package com.alibaba.polardbx.executor.ddl.job.task.localpartition;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.task.BaseValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.LocalPartitionDefinitionInfo;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import lombok.Getter;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.List;

@Getter
@TaskName(name = "LocalPartitionValidateTask")
public class LocalPartitionValidateTask extends BaseValidateTask {

    protected String logicalTableName;

    public LocalPartitionValidateTask(String schemaName, String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        TableValidator.validateTableWithCCI(schemaName, logicalTableName, executionContext, SqlKind.LOCAL_PARTITION);
        final LocalPartitionDefinitionInfo definitionInfo = primaryTableMeta.getLocalPartitionDefinitionInfo();
        if (definitionInfo == null) {
            throw new TddlNestableRuntimeException(String.format(
                "table %s.%s is not a local partition table", schemaName, logicalTableName));
        }

        IRepository repository =
            ExecutorContext.getContext(schemaName)
                .getTopologyHandler()
                .getRepositoryHolder()
                .get(Group.GroupType.MYSQL_JDBC.toString());
        List<TableDescription> primaryTableDesc =
            LocalPartitionManager.getLocalPartitionInfoList(
                (MyRepository) repository,
                schemaName,
                logicalTableName,
                true
            );
        TableDescription expect = primaryTableDesc.get(0);

        List<TableMeta> tablesToCheck = new ArrayList<>();
        tablesToCheck.add(primaryTableMeta);
        List<TableMeta> gsiMetaList =
            GlobalIndexMeta.getIndex(logicalTableName, schemaName, IndexStatus.ALL, null);
        if (!GlobalIndexMeta.isAllGsiPublished(gsiMetaList, executionContext)) {
            throw new TddlNestableRuntimeException("Found Non-Public Global Secondary Index");
        }

        for (TableMeta gsiMeta : gsiMetaList) {
            tablesToCheck.add(gsiMeta);
        }

        for (TableMeta meta : tablesToCheck) {
            //1. 去DN拉取所有的local partition信息. 校验local partition对齐
            List<TableDescription> tableDescriptionList =
                LocalPartitionManager.getLocalPartitionInfoList(
                    (MyRepository) repository,
                    schemaName,
                    meta.getTableName(),
                    false
                );
            boolean consistency = LocalPartitionManager.checkLocalPartitionConsistency(expect, tableDescriptionList);
            if (!consistency) {
                throw new TddlNestableRuntimeException(
                    String.format(
                        "Found inconsistent local partitions, "
                            + "Please use 'CHECK TABLE %s WITH LOCAL PARTITION' to check for details",
                        logicalTableName
                    ));
            }
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        updateSupportedCommands(true, false, null);
    }
}