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

package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetLocalityPreparedData;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSetLocality;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupSetLocality extends BaseDdlOperation {

    private AlterTableGroupSetLocalityPreparedData preparedData;

    public LogicalAlterTableGroupSetLocality(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupSetLocality alterTableGroupSetLocality = (AlterTableGroupSetLocality) relDdl;
        String tableGroupName = alterTableGroupSetLocality.getTableGroupName();
        String targetLocality = alterTableGroupSetLocality.getTargetLocality();
        LocalityDesc targetLocalityDesc = LocalityDesc.parse(targetLocality);

        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        Long tableGroupId = tableGroupConfig.getTableGroupRecord().getId();
        int tgType = tableGroupConfig.getTableGroupRecord().tg_type;
        if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format(
                    "invalid alter locality action for table group! table group [%s] is default single table group or broadcast table group",
                    tableGroupName));
        }
        if (tgType == TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG) {
            if (targetLocalityDesc.getDnList().size() != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format(
                        "invalid alter locality action for single table group! you can only set one dn as locality for single table group [%s]",
                        tableGroupName));

            }
        }
        LocalityDesc originalLocalityDesc = tableGroupConfig.getLocalityDesc();

        List<String> schemaDnList =
            TableGroupLocation.getOrderedGroupList(schemaName).stream().map(group -> group.getStorageInstId())
                .collect(Collectors.toList());
        List<String> partitionGroupDnList =
            tableGroupConfig.getPartitionGroupRecords().stream().map(group -> group.getLocality())
                .collect(Collectors.toList());

        List<String> targetDnList = targetLocalityDesc.getDnList();
        List<String> orignialDnList = originalLocalityDesc.getDnList();
        List<String> drainDnList = new ArrayList<>();
        Boolean withRebalance;
        String rebalanceSql = "";
        // validate locality
        // generate drain node list
        // generate metadb task
        if (schemaDnList.containsAll(targetDnList)) {
            for (PartitionGroupRecord partitionGroupRecord : tableGroupConfig.getPartitionGroupRecords()) {
                LocalityDesc localityDesc = LocalityDesc.parse(partitionGroupRecord.getLocality());
                //support override
//                if (!localityDesc.isEmpty() && !targetLocalityDesc.compactiableWith(localityDesc)) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
//                        String.format("invalid locality: \"%s\", incompactible with locality \"%s\" of partition %s!",
//                            targetLocality, partitionGroupRecord.locality, partitionGroupRecord.partition_name);
//                }
            }
            if (targetDnList.isEmpty() || (targetDnList.containsAll(orignialDnList) && !orignialDnList.isEmpty())) {
                withRebalance = false;
            } else {
                schemaDnList.removeAll(targetDnList);
                drainDnList = schemaDnList;
                withRebalance = true;
                rebalanceSql = String.format("rebalance tablegroup %s", tableGroupName);
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("invalid locality: \"%s\", incompactible with database %s!", targetLocality, schemaName));
        }
        preparedData = new AlterTableGroupSetLocalityPreparedData();
        preparedData.setTargetLocality(targetLocality);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setDrainNodeList(drainDnList);
        preparedData.setWithRebalance(withRebalance);
        preparedData.setRebalanceSql(rebalanceSql);
    }

    public AlterTableGroupSetLocalityPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupSetLocality create(DDL ddl) {
        return new LogicalAlterTableGroupSetLocality(ddl);
    }

}
