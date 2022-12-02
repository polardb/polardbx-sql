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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableRenamePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.ddl.AlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupRenamePartition;
import org.apache.calcite.sql.SqlAlterTableMovePartition;
import org.apache.calcite.sql.SqlAlterTableRenamePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class LogicalAlterTableRenamePartition extends BaseDdlOperation {

    protected AlterTableGroupRenamePartitionPreparedData preparedData;

    public LogicalAlterTableRenamePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableRenamePartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public static LogicalAlterTableRenamePartition create(DDL ddl) {
        return new LogicalAlterTableRenamePartition(ddl);
    }

    public void prepareData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableMovePartition;
        SqlAlterTableRenamePartition sqlAlterTableRenamePartition =
            (SqlAlterTableRenamePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();
        Map<String, String> changePartitions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Pair<String, String> pair : sqlAlterTableRenamePartition.getChangePartitionsPair()) {
            changePartitions.put(pair.getKey(), pair.getValue());
        }
        PartitionInfo newPartInfo =
            AlterTableGroupSnapShotUtils.getNewPartitionInfoForRenamePartition(curPartitionInfo, changePartitions);
        preparedData = new AlterTableRenamePartitionPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setChangePartitionsPair(sqlAlterTableRenamePartition.getChangePartitionsPair());
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setTableName(logicalTableName);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
            null, flag, ec);
    }

    public AlterTableGroupRenamePartitionPreparedData getPreparedData() {
        return preparedData;
    }
}
