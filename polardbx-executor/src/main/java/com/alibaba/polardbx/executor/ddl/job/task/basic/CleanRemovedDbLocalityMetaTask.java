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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.locality.LocalityDetailInfoRecord;
import com.alibaba.polardbx.gms.locality.LocalityInfoAccessor;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@TaskName(name = "CleanRemovedDbLocalityMetaTask")
public class CleanRemovedDbLocalityMetaTask extends BaseDdlTask {

    private List<LocalityDetailInfoRecord> toRemovedLocalityItem;

    @JSONCreator
    public CleanRemovedDbLocalityMetaTask(String schemaName, List<LocalityDetailInfoRecord> toRemovedLocalityItem) {
        super(schemaName);
        this.toRemovedLocalityItem = toRemovedLocalityItem;
    }

    @SneakyThrows
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

        tableGroupAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);

        String dbName = schemaName;
        List<Long> removedLocalityPgIds = new ArrayList<>();
        List<String> tableNames = new ArrayList<>();
        for (int i = 0; i < toRemovedLocalityItem.size(); i++) {
            LocalityDetailInfoRecord localityDetailInfoRecord = toRemovedLocalityItem.get(i);
            int objectType = localityDetailInfoRecord.getObjectType();
            if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP) {
                tableGroupAccessor.updateTableGroupLocality(dbName, localityDetailInfoRecord.getObjectName(), "");
                //table partition
            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                partitionGroupAccessor.updatePartitionGroupLocality(localityDetailInfoRecord.getObjectId(), "");
                removedLocalityPgIds.add(localityDetailInfoRecord.getObjectId());
            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLE) {
                tableNames.add(localityDetailInfoRecord.objectName);
                tablePartitionAccessor.setTableLocalityByTableName(schemaName,
                    localityDetailInfoRecord.getObjectName(), "");
            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_DATABASE) {
                LocalityManager.getInstance().setLocalityOfDb(localityDetailInfoRecord.getObjectId(), "");
            }
        }
        tablePartitionAccessor.resetTablePartitionsLocalityByGroupIds(schemaName, removedLocalityPgIds);
        /**
         * One group may contain multi group details because of read-only inst of cn
         */

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

    @SneakyThrows
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {

        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
        LocalityInfoAccessor localityInfoAccessor = new LocalityInfoAccessor();
        TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();

        localityInfoAccessor.setConnection(metaDbConnection);
        tableGroupAccessor.setConnection(metaDbConnection);
        partitionGroupAccessor.setConnection(metaDbConnection);
        tablePartitionAccessor.setConnection(metaDbConnection);

        String dbName = schemaName;
        for (int i = 0; i < toRemovedLocalityItem.size(); i++) {
            LocalityDetailInfoRecord localityDetailInfoRecord = toRemovedLocalityItem.get(i);
            int objectType = localityDetailInfoRecord.getObjectType();
            if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLEGROUP) {
                tableGroupAccessor.updateTableGroupLocality(dbName, localityDetailInfoRecord.getObjectName(),
                    localityDetailInfoRecord.getLocality());
                //table partition
            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_PARTITIONGROUP) {
                partitionGroupAccessor.updatePartitionGroupLocality(localityDetailInfoRecord.getObjectId(),
                    localityDetailInfoRecord.getLocality());
                tablePartitionAccessor.resetTablePartitionsLocalityByGroupIds(schemaName,
                    Arrays.asList(localityDetailInfoRecord.getObjectId()), localityDetailInfoRecord.getLocality());
            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_TABLE) {
                //nothing happened

            } else if (objectType == LocalityDetailInfoRecord.LOCALITY_TYPE_DATABASE) {
                LocalityManager.getInstance()
                    .setLocalityOfDb(localityDetailInfoRecord.getObjectId(), localityDetailInfoRecord.getLocality());
            }
        }
        /**
         * One group may contain multi group details because of read-only inst of cn
         */
//            for (int j = 0; j < groupDetails.size(); j++) {
//                GroupDetailInfoRecord oneGrpDetail = groupDetails.get(j);
//                String cnId = oneGrpDetail.getInstId();
//                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(cnId, dbName, targetGrpName);
//
//                /**
//                 * Remove the data_id for group detail
//                 */
//                MetaDbConfigManager.getInstance().unregister(grpDataId, metaDbConnection);
//            }
//
//            /**
//             * Remove group detail infos
//             */
//            groupDetailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, targetGrpName);
//
//            /**
//             * Remove db group info
//             */
//            dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, targetGrpName);
//        }
//
//        updateSupportedCommands(true, false, metaDbConnection);

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
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
