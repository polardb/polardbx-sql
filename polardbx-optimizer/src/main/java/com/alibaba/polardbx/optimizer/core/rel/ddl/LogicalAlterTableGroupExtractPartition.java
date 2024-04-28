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
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupExtractPartition;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupExtractPartition;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class LogicalAlterTableGroupExtractPartition extends LogicalAlterTableExtractPartition {

    public LogicalAlterTableGroupExtractPartition(DDL ddl) {
        super(ddl, true);
    }

    @Override
    public void preparedData(ExecutionContext executionContext) {
        AlterTableGroupExtractPartition alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        Map<SqlNode, RexNode> partBoundExprInfo = alterTableGroupExtractPartition.getPartBoundExprInfo();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupExtractPartition.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS,
                "tablegroup:" + tableGroupName + " doesn't exists");
        }
        if (tableGroupConfig.getTableCount() == 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "can't modify the tablegroup:" + tableGroupName + " when it's empty");
        }
        String firstTableInTableGroup = tableGroupConfig.getTables().get(0);

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupExtractPartition;
        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition =
            (SqlAlterTableGroupExtractPartition) sqlAlterTableGroup.getAlters().get(0);

        List<Long[]> splitPoints =
            normalizeSqlExtractPartition(sqlAlterTableGroupExtractPartition, firstTableInTableGroup,
                alterTableGroupExtractPartition.getPartBoundExprInfo(), executionContext);

        String extractPartitionName = sqlAlterTableGroupExtractPartition.getExtractPartitionName();
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(extractPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);
        preparedData = new AlterTableGroupExtractPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setSplitPartitions(splitPartitions);
        preparedData.setNewPartitions(sqlAlterTableGroupExtractPartition.getNewPartitions());
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(partBoundExprInfo);
        preparedData.setHotKeys(sqlAlterTableGroupExtractPartition.getHotKeys());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.EXTRACT_PARTITION);
        preparedData.setSplitPoints(splitPoints);
    }

//    private List<Long[]> normalizeSqlExtractPartition(
//        SqlAlterTableGroupExtractPartition sqlAlterTableGroupExtractPartition,
//        String tableGroupName,
//        ExecutionContext executionContext) {
//        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
//            .getTableGroupConfigByName(tableGroupName);
//        List<RexNode> rexNodes = new ArrayList<>();
//        sqlAlterTableGroupExtractPartition.getHotKeys()
//            .forEach(o -> rexNodes.add(alterTableGroupExtractPartition.getPartBoundExprInfo()
//                .get(o)));
//
//        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
//        PartitionInfo partitionInfo =
//            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableInCurrentGroup);
//
//        if (sqlAlterTableGroupExtractPartition.getHotKeys().size() != partitionInfo.getPartitionColumns().size()) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "the size of hot key list should equal to the size of sharding key list");
//        }
//
//        PartitionSpec partitionSpec = PartitionTupleRouteInfoBuilder
//            .getPartitionSpecByExprValues(partitionInfo, rexNodes, executionContext);
//        assert partitionSpec != null;
//        PartitionSpec prevPartitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
//            .filter(o -> o.getPosition().longValue() == partitionSpec.getPosition().longValue() - 1).findFirst()
//            .orElse(null);
//        PartitionGroupRecord splitPartitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
//            .filter(o -> o.id.longValue() == partitionSpec.getLocation().getPartitionGroupId().longValue()).findFirst()
//            .orElse(null);
//        assert splitPartitionGroupRecord != null;
//
//        sqlAlterTableGroupExtractPartition.setExtractPartitionName(splitPartitionGroupRecord.partition_name);
//        Long[] atVal = PartitionTupleRouteInfoBuilder
//            .computeExprValuesHashCode(partitionInfo, rexNodes, executionContext);
//        int flag = PartitionPrunerUtils.getExtractPosition(partitionSpec, prevPartitionSpec, atVal);
//        if (flag == -2) {
//            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                "the hot key:" + sqlAlterTableGroupExtractPartition.getHotKeys().toString()
//                    + " is already in a exclusive partition");
//        }
//        String hotKeyPartitioName = StringUtils.EMPTY;
//        if (sqlAlterTableGroupExtractPartition.getHotKeyPartitionName() != null) {
//            hotKeyPartitioName = SQLUtils.normalizeNoTrim(sqlAlterTableGroupExtractPartition.getHotKeyPartitionName().toString());
//            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
//                if (record.partition_name.equalsIgnoreCase(hotKeyPartitioName)) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                        String.format("duplicate partition name:[%s]", hotKeyPartitioName));
//                }
//            }
//        }
//
//        if (StringUtils.isEmpty(hotKeyPartitioName)) {
//            List<String> newPartNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 3);
//            SqlIdentifier name1 = new SqlIdentifier(newPartNames.get(0), SqlParserPos.ZERO);
//            SqlPartition sqlPartition1 = new SqlPartition(name1, null, SqlParserPos.ZERO);
//            sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition1);
//
//            SqlIdentifier name2 = new SqlIdentifier(newPartNames.get(1), SqlParserPos.ZERO);
//            SqlPartition sqlPartition2 = new SqlPartition(name2, null, SqlParserPos.ZERO);
//            sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition2);
//            if (flag == 0) {
//                SqlIdentifier name3 = new SqlIdentifier(newPartNames.get(2), SqlParserPos.ZERO);
//                SqlPartition sqlPartition3 = new SqlPartition(name3, null, SqlParserPos.ZERO);
//                sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition3);
//            }
//        } else {
//            List<String> newPartNames = new ArrayList<>();
//            if (flag == -1) {
//                newPartNames.add(hotKeyPartitioName);
//                newPartNames.addAll(PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1));
//            } else if (flag == 0) {
//                List<String> boundPartNames = PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 2);
//                newPartNames.add(boundPartNames.get(0));
//                newPartNames.add(hotKeyPartitioName);
//                newPartNames.add(boundPartNames.get(1));
//            } else if (flag == 1) {
//                newPartNames.addAll(PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1));
//                newPartNames.add(hotKeyPartitioName);
//            } else {
//                assert false;
//            }
//            for (String partName : newPartNames) {
//                SqlIdentifier name = new SqlIdentifier(partName, SqlParserPos.ZERO);
//                SqlPartition sqlPartition = new SqlPartition(name, null, SqlParserPos.ZERO);
//                sqlAlterTableGroupExtractPartition.getNewPartitions().add(sqlPartition);
//            }
//        }
//        return getSplitPointsForHotValue(atVal, flag);
//    }

//    /**
//     * i.e. p=[1,9), if atVal=1, extractPosition = -1, [1,2),[2,9)
//     * if atval=3, extractPosition = 0, [1,3),[3,4),[4,9)
//     * if atVal=8, extractPosition = 1, [1,8),[8,9)
//     * if p=[1,2) atVal=1, extractPosition=-2 not need to split any more
//     */
//    private List<Long[]> getSplitPointsForHotValue(Long[] atValHashCode,
//                                                   int extractPosition) {
//        List<Long[]> splitPoints = new ArrayList<>();
//        if (extractPosition == -1) {
//            splitPoints.add(getThexNextHashPoint(atValHashCode));
//        } else if (extractPosition == 0) {
//            splitPoints.add(atValHashCode);
//            splitPoints.add(getThexNextHashPoint(atValHashCode));
//        } else if (extractPosition == 1) {
//            splitPoints.add(atValHashCode);
//        }
//        return splitPoints;
//    }

    private Long[] getThexNextHashPoint(Long[] hashVals) {
        Long[] nextPoint = new Long[hashVals.length];
        boolean plusOne = true;
        for (int i = hashVals.length - 1; i >= 0; i--) {
            if (plusOne) {
                if (hashVals[i] == Long.MAX_VALUE) {
                    nextPoint[i] = Long.MIN_VALUE;
                } else {
                    nextPoint[i] = hashVals[i] + 1;
                    plusOne = false;
                }
            } else {
                nextPoint[i] = hashVals[i];
            }
        }
        return nextPoint;
    }

//    public AlterTableGroupExtractPartitionPreparedData getPreparedData() {
//        return preparedData;
//    }

    public static LogicalAlterTableGroupExtractPartition create(DDL ddl) {
        return new LogicalAlterTableGroupExtractPartition(AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupExtractPartition alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroupName);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        AlterTableGroupExtractPartition alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupExtractPartition alterTableGroupExtractPartition = (AlterTableGroupExtractPartition) relDdl;
        String tableGroupName = alterTableGroupExtractPartition.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }
}
