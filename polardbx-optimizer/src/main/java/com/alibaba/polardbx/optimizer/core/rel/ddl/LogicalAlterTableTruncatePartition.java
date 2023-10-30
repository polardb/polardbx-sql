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

import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableTruncatePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableTruncatePartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;

import java.util.Set;
import java.util.stream.Collectors;

public class LogicalAlterTableTruncatePartition extends BaseDdlOperation {

    protected AlterTableGroupTruncatePartitionPreparedData preparedData;

    public LogicalAlterTableTruncatePartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableTruncatePartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void prepareData() {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();

        SqlAlterTableTruncatePartition sqlAlterTableTruncatePartition =
            (SqlAlterTableTruncatePartition) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);

        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

        TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(logicalTableName);

        PartitionInfo partitionInfo = optimizerContext.getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            optimizerContext.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());

        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        Set<String> actualPartitionNames =
            getTruncatingPartitionNames(sqlAlterTableTruncatePartition, partitionInfo, tableGroupConfig);

        preparedData = new AlterTableTruncatePartitionPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setTableVersion(tableMeta.getVersion());
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTruncatePartitionNames(actualPartitionNames);
    }

    protected Set<String> getTruncatingPartitionNames(SqlAlterTableTruncatePartition sqlAlterTableTruncatePartition,
                                                      PartitionInfo partitionInfo,
                                                      TableGroupConfig tableGroupConfig) {
        Set<String> partitionNames = sqlAlterTableTruncatePartition.getPartitionNames().stream()
            .map(sqlNode -> ((SqlIdentifier) sqlNode).getLastName().toLowerCase()).collect(Collectors.toSet());

        String allPartitions = partitionNames.stream().filter(p -> p.equalsIgnoreCase("ALL")).findFirst().orElse(null);
        if (allPartitions != null) {
            return PartitionInfoUtil.getAllPartitionGroupNames(tableGroupConfig);
        }

        return PartitionInfoUtil.checkAndExpandPartitions(partitionInfo, tableGroupConfig, partitionNames,
            sqlAlterTableTruncatePartition.isSubPartition());
    }

    public AlterTableGroupTruncatePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableTruncatePartition create(DDL ddl) {
        return new LogicalAlterTableTruncatePartition(ddl);
    }

}
