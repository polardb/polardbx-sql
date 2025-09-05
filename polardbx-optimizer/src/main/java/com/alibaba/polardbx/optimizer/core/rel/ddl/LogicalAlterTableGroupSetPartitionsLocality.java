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
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetPartitionsLocalityPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSetPartitionsLocality;
import org.apache.calcite.util.PrecedenceClimbingParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableGroupSetPartitionsLocality extends BaseDdlOperation {

    private AlterTableGroupSetPartitionsLocalityPreparedData preparedData;

    public LogicalAlterTableGroupSetPartitionsLocality(DDL ddl) {
        super(ddl);
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public void preparedData() {
        AlterTableGroupSetPartitionsLocality alterTableGroupSetPartitionLocality =
            (AlterTableGroupSetPartitionsLocality) relDdl;
        String tableGroupName = alterTableGroupSetPartitionLocality.getTableGroupName();
        String partition = alterTableGroupSetPartitionLocality.getPartition();
        String targetLocality = alterTableGroupSetPartitionLocality.getTargetLocality();
        LocalityDesc targetLocalityDesc = LocalityInfoUtils.parse(targetLocality);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig.getTables().size() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format(
                    "invalid alter locality operation on partition! table group [%s] contains no table",
                    tableGroupName));
        }
        String tableName = tableGroupConfig.getTables().get(0);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);

        int tgType = tableGroupConfig.getTableGroupRecord().tg_type;
        if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format(
                    "invalid alter locality operation on partition! table group [%s] is single table group or broadcast table group",
                    tableGroupName));
        }

        PartitionSpec partitionSpec = partitionInfo.getPartitionBy().getPartitionByPartName(partition);
        if (partitionSpec.isLogical()) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("invalid alter locality operation on logical partition[%s]", partition));
        }
        String originalPartitionGroupLocality = partitionSpec.getLocality();
        LocalityDesc originalPartitionGroupLocalityDesc = LocalityInfoUtils.parse(originalPartitionGroupLocality);
        String grpKey = partitionSpec.getLocation().getGroupKey();
        String realTopology = DbTopologyManager.getStorageInstIdByGroupName(schemaName, grpKey);
        Set<String> targetDnList = targetLocalityDesc.getDnSet();
        Set<String> originalDnList = originalPartitionGroupLocalityDesc.getDnSet();
        String rebalanceSql = "";
        // validate locality
        // generate drain node list
        // generate metadb task

        LocalityInfo localityOfDb = LocalityManager.getInstance().getLocalityOfDb(schemaName);
        LocalityDesc localityDescOfDb = LocalityInfoUtils.parse(localityOfDb.getLocality());
//        if (!localityDescOfDb.compactiableWith(targetLocalityDesc)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
//                String.format("invalid locality: '%s', conflict with locality of database [%s]: '%s'",
//                    targetLocality, schemaName, localityDescOfDb));
//        }
        Boolean skipRebalance = false;
        // originalDnList is empty, then we judge this by realTopology.
        if (GeneralUtil.isEmpty(originalDnList)) {
            if (targetDnList.contains(realTopology)) {
                skipRebalance = true;
            }
            // targetDnList is empty
        } else if (targetDnList.isEmpty()) {
            skipRebalance = true;
            // originalDnList is not empty and targetDnList is not empty.
        } else if (!GeneralUtil.isEmpty(originalDnList) && !GeneralUtil.isEmpty(targetDnList)) {
            if (targetDnList.containsAll(originalDnList) && targetDnList.contains(realTopology)) {
                skipRebalance = true;
            }
        }
        rebalanceSql = String.format("schedule rebalance tablegroup `%s` policy = 'data_balance'", tableGroupName);
        preparedData = new AlterTableGroupSetPartitionsLocalityPreparedData();
        preparedData.setTargetLocality(targetLocality);
        preparedData.setPartition(partition);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setWithRebalance(!skipRebalance);
        preparedData.setRebalanceSql(rebalanceSql);
    }

    public AlterTableGroupSetPartitionsLocalityPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableGroupSetPartitionsLocality create(DDL ddl) {
        return new LogicalAlterTableGroupSetPartitionsLocality(ddl);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSetPartitionsLocality alterTableGroupSetPartitionLocality =
            (AlterTableGroupSetPartitionsLocality) relDdl;
        String tableGroupName = alterTableGroupSetPartitionLocality.getTableGroupName();
        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSetPartitionsLocality alterTableGroupSetPartitionLocality =
            (AlterTableGroupSetPartitionsLocality) relDdl;
        String tableGroupName = alterTableGroupSetPartitionLocality.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
