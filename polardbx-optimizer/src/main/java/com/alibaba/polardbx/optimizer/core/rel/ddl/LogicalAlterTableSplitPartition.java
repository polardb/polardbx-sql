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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class LogicalAlterTableSplitPartition extends BaseDdlOperation {

    protected AlterTableGroupSplitPartitionPreparedData preparedData;

    public LogicalAlterTableSplitPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableSplitPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    public void preparedData(ExecutionContext ec) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartition;
        SqlAlterTableSplitPartition sqlAlterTableSplitPartition =
            (SqlAlterTableSplitPartition) sqlAlterTable.getAlters().get(0);

        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        boolean ignoreNameAndLocality = GeneralUtil.isEmpty(sqlAlterTableSplitPartition.getNewPartitions());

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(curPartitionInfo.getTableGroupId());
        String tableGroupName = tableGroupConfig.getTableGroupRecord().getTg_name();

        normalizeSqlSplitPartition(sqlAlterTableSplitPartition, tableGroupName);
        String splitPartitionName =
            Util.last(((SqlIdentifier) (sqlAlterTableSplitPartition.getSplitPartitionName())).names);
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(splitPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        preparedData = new AlterTableSplitPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableSplitPartition.getNewPartitions());
        preparedData.setIncludeFullPartitionDefinition(sqlAlterTableSplitPartition.getAtValue() == null);
        preparedData.setTargetStorageInstIds(null);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setAtVal(sqlAlterTableSplitPartition.getAtValue());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        List<Pair<String, String>> mockOrderedTargetTableLocations = new ArrayList<>(newPartitionGroups.size());
        if (ignoreNameAndLocality) {
            for (int j = 0; j < newPartitionGroups.size(); j++) {
                Pair<String, String> pair = new Pair<>("", "");
                mockOrderedTargetTableLocations.add(pair);
            }
            flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
        } else {
            int i = 0;
            for (int j = 0; j < newPartitionGroups.size(); j++) {
                GroupDetailInfoExRecord groupDetailInfoExRecord =
                    preparedData.getTargetGroupDetailInfoExRecords().get(i++);

                String mockTableName = "";
                mockOrderedTargetTableLocations.add(new Pair<>(mockTableName, groupDetailInfoExRecord.getGroupName()));
                if (i >= preparedData.getTargetGroupDetailInfoExRecords().size()) {
                    i = 0;
                }
            }
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfoForSplitType(ec, curPartitionInfo, preparedData.getInvisiblePartitionGroups(),
                sqlAlterTableSplitPartition, tableGroupName, splitPartitionName,
                mockOrderedTargetTableLocations, preparedData.getPartBoundExprInfo());

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo,
            sqlAlterTableSplitPartition.getNewPartitions(), null, flag, ec);
    }

    public void normalizeSqlSplitPartition(SqlAlterTableSplitPartition sqlAlterTableGroupSplitPartition,
                                           String tableGroupName) {
        if (GeneralUtil.isEmpty(sqlAlterTableGroupSplitPartition.getNewPartitions())) {
            final TableGroupInfoManager tableGroupInfoManager =
                OptimizerContext.getContext(schemaName).getTableGroupInfoManager();

            TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
            List<String> newPartitionNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 2);
            assert newPartitionNames.size() == 2;
            SqlIdentifier name1 = new SqlIdentifier(newPartitionNames.get(0), SqlParserPos.ZERO);
            SqlPartition sqlPartition1 = new SqlPartition(name1, null, SqlParserPos.ZERO);
            SqlIdentifier name2 = new SqlIdentifier(newPartitionNames.get(1), SqlParserPos.ZERO);
            SqlPartition sqlPartition2 = new SqlPartition(name2, null, SqlParserPos.ZERO);
            sqlAlterTableGroupSplitPartition.getNewPartitions().add(sqlPartition1);
            sqlAlterTableGroupSplitPartition.getNewPartitions().add(sqlPartition2);
        }

    }

    public AlterTableGroupSplitPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSplitPartition create(DDL ddl) {
        return new LogicalAlterTableSplitPartition(ddl);
    }

}
