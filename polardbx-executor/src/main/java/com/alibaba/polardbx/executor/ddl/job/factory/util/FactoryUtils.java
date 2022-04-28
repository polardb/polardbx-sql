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

package com.alibaba.polardbx.executor.ddl.job.factory.util;

import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FactoryUtils {

    /**
     * for validate create table with new single/broadcast table group
     */
    public static void checkDefaultTableGroup(String schemaName,
                                              PartitionInfo partitionInfo,
                                              PhysicalPlanData physicalPlanData,
                                              boolean isCreateTableGroup,
                                              boolean checkSingleTgNotExists,
                                              boolean checkBroadcastTgNotExists) {
        boolean isSigleTable = false;
        boolean isBroadCastTable = false;
        boolean lockSingleGroup = false;
        boolean lockbroadcastGroup = false;
        if (partitionInfo != null) {
            isSigleTable = partitionInfo.isGsiSingleOrSingleTable();
            isBroadCastTable = partitionInfo.isGsiBroadcastOrBroadcast();

            TableGroupConfig tgConfig = physicalPlanData.getTableGroupConfig();
            for (TablePartRecordInfoContext entry : tgConfig.getTables()) {
                Long tableGroupId = entry.getLogTbRec().getGroupId();
                if (tableGroupId != null && tableGroupId != -1) {
                    OptimizerContext oc =
                        Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
                    TableGroupConfig tableGroupConfig =
                        oc.getTableGroupInfoManager().getTableGroupConfigById(tableGroupId);
                    TableGroupRecord record = tableGroupConfig.getTableGroupRecord();
                    if (record.tg_type == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG) {
                        lockbroadcastGroup = true;
                    } else if (record.tg_type == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
                        lockSingleGroup = true;
                    }
                }
            }

            if (isCreateTableGroup) {
                if (isSigleTable) {
                    if (!lockSingleGroup) {
                        checkSingleTgNotExists = true;
                    }
                } else if (isBroadCastTable) {
                    if (!lockbroadcastGroup) {
                        checkBroadcastTgNotExists = true;
                    }
                }
            }
        }
    }


    public static List<TableGroupConfig> getTableGroupConfigByTableName(String schemaName, List<String> tableNames) {
        List<TableGroupConfig> results = new ArrayList<>();

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        for (String tableName : tableNames) {
            PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
            if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
                TableGroupConfig tableGroupConfig =
                    oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
                results.add(tableGroupConfig);
            }
        }
        return results;
    }

    public static String getTableGroupNameByTableName(String schemaName, String tableName) {
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(tableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            return tableGroupConfig.getTableGroupRecord().getTg_name();
        }
        return null;
    }
}
