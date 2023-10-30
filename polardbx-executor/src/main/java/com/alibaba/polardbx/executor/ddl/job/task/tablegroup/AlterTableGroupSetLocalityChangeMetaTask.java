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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

@Getter
@TaskName(name = "AlterTableGroupSetLocalityPartitionChangeMetaTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AlterTableGroupSetLocalityChangeMetaTask extends BaseDdlTask {

    protected String tableGroupName;
    protected String targetLocality;
    protected List<String> logicalTableNames;
    protected List<LocalityDetailInfoRecord> toChangeMetaLocalityItems;
    protected Boolean rollback;

    @JSONCreator
    public AlterTableGroupSetLocalityChangeMetaTask(String schemaName, String tableGroupName,
                                                    List<String> logicalTableNames, String targetLocality,
                                                    List<LocalityDetailInfoRecord> toChangeMetaLocalityItems
    ) {
        super(schemaName);
        this.tableGroupName = tableGroupName;
        this.logicalTableNames = logicalTableNames;
        this.targetLocality = targetLocality;
        this.toChangeMetaLocalityItems = toChangeMetaLocalityItems;
        this.rollback = false;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);
        tableGroupAccessor.setConnection(metaDbConnection);

        String targetLocality = "";
        if (rollback) {
            targetLocality = toChangeMetaLocalityItems.get(0).getLocality();
        } else {
            targetLocality = this.targetLocality;
        }

        //we would update the meta data in table_partition, partition_group and table_group.
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < toChangeMetaLocalityItems.size(); i++) {
            LocalityDetailInfoRecord localityDetailInfoRecord = toChangeMetaLocalityItems.get(i);
            try {
                int objectType = localityDetailInfoRecord.getObjectType();
                if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP) {
                    tableGroupAccessor.updateTableGroupLocality(schemaName, localityDetailInfoRecord.getObjectName(),
                        targetLocality);
                } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                    //partition_group ignored.
                } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLE) {
                    tablePartitionAccessor.setTableLocalityByTableName(schemaName,
                        localityDetailInfoRecord.getObjectName(), targetLocality);
                    tableNames.add(localityDetailInfoRecord.getObjectName());
                }
            } catch (Throwable t) {
                LOGGER.error(String.format(
                    "error occurs while update tablegroup, schemaName:%s, tableGroupName:%s",
                    schemaName, tableGroupName));
                throw GeneralUtil.nestedException(t);
            }
        }
        for (String table : tableNames) {
            try {
                TableInfoManager.updateTableVersion(schemaName, table, metaDbConnection);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        this.rollback = true;
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //ComplexTaskMetaManager.getInstance().reload();
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

}
