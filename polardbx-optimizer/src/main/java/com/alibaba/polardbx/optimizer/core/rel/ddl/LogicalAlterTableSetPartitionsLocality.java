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
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupUtils;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSetPartitionsLocalityPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfo;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSetPartitionsLocality;
import org.apache.commons.lang3.RandomStringUtils;

import java.util.Set;

public class LogicalAlterTableSetPartitionsLocality extends BaseDdlOperation {

    private AlterTableGroupSetPartitionsLocalityPreparedData preparedData;

    public LogicalAlterTableSetPartitionsLocality(DDL ddl) {
        super(ddl);
    }

    public void preparedData() {
        AlterTableGroupSetPartitionsLocality alterTableGroupSetPartitionLocality =
            (AlterTableGroupSetPartitionsLocality) relDdl;
        String tableName = alterTableGroupSetPartitionLocality.getTableGroupName();
        String partition = alterTableGroupSetPartitionLocality.getPartition();
        String targetLocality = alterTableGroupSetPartitionLocality.getTargetLocality();
        LocalityDesc targetLocalityDesc = LocalityInfoUtils.parse(targetLocality);
        Long tgId =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName).getPartitionInfo()
                .getTableGroupId();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigById(tgId);
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
        int tgType = tableGroupConfig.getTableGroupRecord().tg_type;
        if (tgType == TableGroupRecord.TG_TYPE_BROADCAST_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_DEFAULT_SINGLE_TBL_TG
            || tgType == TableGroupRecord.TG_TYPE_NON_DEFAULT_SINGLE_TBL_TG) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format(
                    "invalid alter locality operation on partition! table group [%s] is single table group or broadcast table group",
                    tableGroupName));
        }

        PartitionGroupRecord partitionGroupRecord = tableGroupConfig.getPartitionGroupByName(partition);
        String originalPartitionGroupLocality = partitionGroupRecord.getLocality();
        LocalityDesc originalPartitionGroupLocalityDesc = LocalityInfoUtils.parse(originalPartitionGroupLocality);

        Set<String> targetDnList = targetLocalityDesc.getDnSet();
        Set<String> orignialDnList = originalPartitionGroupLocalityDesc.getDnSet();
        Boolean withRebalance;
        String rebalanceSql = "";
        // validate locality
        // generate drain node list
        // generate metadb task

//        if (!localityDescOfDb.compactiableWith(targetLocalityDesc)) {
//            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
//                String.format("invalid locality: '%s', conflict with locality of database [%s]: '%s'",
//                    targetLocality, schemaName, localityDescOfDb));
//        }
        withRebalance = true;
        String newTableGroupName = "tg_" + RandomStringUtils.randomAlphanumeric(4);
        String newTableGroupSql = String.format("create tablegroup `%s`", newTableGroupName);
        String setTableGroupSql = String.format("alter table `%s` set tablegroup = '%s'", tableName, newTableGroupName);
        rebalanceSql =
            String.format("alter tablegroup `%s` set partitions `%s` locality = '%s'", newTableGroupName, partition,
                targetLocality);

        preparedData = new AlterTableGroupSetPartitionsLocalityPreparedData();
        preparedData.setTargetLocality(targetLocality);
        preparedData.setPartition(partition);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSchemaName(schemaName);
        preparedData.setWithRebalance(withRebalance);
        preparedData.setRebalanceSql(rebalanceSql);
        preparedData.setNewTgSql(newTableGroupSql);
        preparedData.setSetTgSql(setTableGroupSql);
    }

    public AlterTableGroupSetPartitionsLocalityPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSetPartitionsLocality create(DDL ddl) {
        return new LogicalAlterTableSetPartitionsLocality(ddl);
    }

}
