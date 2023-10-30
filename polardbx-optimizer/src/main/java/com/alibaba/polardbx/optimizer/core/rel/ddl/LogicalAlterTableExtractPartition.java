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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupExtractPartitionPreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableExtractPartition;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

public class LogicalAlterTableExtractPartition extends BaseDdlOperation {

    protected AlterTableGroupExtractPartitionPreparedData preparedData;

    public LogicalAlterTableExtractPartition(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableExtractPartition(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        String logicalTableName = Util.last(((SqlIdentifier) relDdl.getTableName()).names);
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + logicalTableName);
    }

    public void preparedData(ExecutionContext executionContext) {

        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableExtractPartition;
        SqlAlterTableExtractPartition sqlAlterTableExtractPartitionByHotValue =
            (SqlAlterTableExtractPartition) sqlAlterTable.getAlters().get(0);

        String hotKeyPartName = org.apache.commons.lang.StringUtils.EMPTY;
        if (sqlAlterTableExtractPartitionByHotValue.getHotKeyPartitionName() != null) {
            hotKeyPartName =
                SQLUtils.normalizeNoTrim(sqlAlterTableExtractPartitionByHotValue.getHotKeyPartitionName().toString());
        }
        boolean ignoreNameAndLocality = org.apache.commons.lang.StringUtils.isEmpty(hotKeyPartName);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        TableGroupConfig tableGroupConfig =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager().getTableGroupConfigById(
                partitionInfo.getTableGroupId());

        List<Long[]> splitPoints =
            normalizeSqlExtractPartition(sqlAlterTableExtractPartitionByHotValue, logicalTableName,
                alterTable.getAllRexExprInfo(), executionContext);

        String extractPartitionName = sqlAlterTableExtractPartitionByHotValue.getExtractPartitionName();
        List<String> splitPartitions = new ArrayList<>();
        splitPartitions.add(extractPartitionName);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName,
                tableGroupConfig.getTableGroupRecord().tg_name);
        preparedData = new AlterTableExtractPartitionPreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);

        preparedData.setTableGroupName(tableGroupConfig.getTableGroupRecord().getTg_name());
        preparedData.setSplitPartitions(splitPartitions);
        if (sqlAlterTableExtractPartitionByHotValue.getLocality() != null) {

        }
        preparedData.setNewPartitions(sqlAlterTableExtractPartitionByHotValue.getNewPartitions());
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setPartBoundExprInfo(alterTable.getAllRexExprInfo());
        preparedData.setHotKeys(sqlAlterTableExtractPartitionByHotValue.getHotKeys());
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.EXTRACT_PARTITION);
        preparedData.setSplitPoints(splitPoints);

        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());

        PartitionInfo curPartitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
        Map<String, Pair<String, String>> mockOrderedTargetTableLocations = new TreeMap<>(String::compareToIgnoreCase);

        int flag = PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION;
        if (ignoreNameAndLocality) {
            for (int j = 0; j < newPartitionGroups.size(); j++) {
                Pair<String, String> pair = new Pair<>("", "");
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, pair);
            }
            flag |= PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY;
        } else {
            int i = 0;
            for (int j = 0; j < newPartitionGroups.size(); j++) {

                String mockTableName = "";
                mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, new Pair<>(mockTableName,
                    GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(j).partition_name)));
            }
        }

        PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
            .getNewPartitionInfo(
                preparedData,
                partitionInfo,
                false,
                sqlAlterTableExtractPartitionByHotValue,
                preparedData.getOldPartitionNames(),
                preparedData.getNewPartitionNames(),
                preparedData.getTableGroupName(),
                null,
                preparedData.getInvisiblePartitionGroups(),
                mockOrderedTargetTableLocations,
                executionContext);

        preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
            null, flag, executionContext);
    }

    protected List<Long[]> normalizeSqlExtractPartition(
        SqlAlterTableExtractPartition sqlAlterTableExtractPartition,
        String logicalTableName,
        Map<SqlNode, RexNode> partBoundExprInfo,
        ExecutionContext executionContext) {
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigById(partitionInfo.getTableGroupId());
        List<RexNode> rexNodes = new ArrayList<>();
        sqlAlterTableExtractPartition.getHotKeys()
            .forEach(o -> rexNodes.add(partBoundExprInfo.get(o)));

        if (sqlAlterTableExtractPartition.getHotKeys().size() != partitionInfo.getPartitionColumns().size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "the size of hot key list should equal to the size of sharding key list");
        }

        PartitionSpec partitionSpec = PartitionTupleRouteInfoBuilder
            .getPartitionSpecByExprValues(partitionInfo, rexNodes, executionContext);
        assert partitionSpec != null;
        PartitionSpec prevPartitionSpec = partitionInfo.getPartitionBy().getPartitions().stream()
            .filter(o -> o.getPosition().longValue() == partitionSpec.getPosition().longValue() - 1).findFirst()
            .orElse(null);
        PartitionGroupRecord splitPartitionGroupRecord = tableGroupConfig.getPartitionGroupRecords().stream()
            .filter(o -> o.id.longValue() == partitionSpec.getLocation().getPartitionGroupId().longValue()).findFirst()
            .orElse(null);
        assert splitPartitionGroupRecord != null;

        sqlAlterTableExtractPartition.setExtractPartitionName(splitPartitionGroupRecord.partition_name);
        List<Long[]> atVals = PartitionTupleRouteInfoBuilder
            .computeExprValuesHashCode(partitionInfo, rexNodes, executionContext);
        Long[] atVal = atVals.get(0);
        int flag = PartitionPrunerUtils.getExtractPosition(partitionSpec, prevPartitionSpec, atVal);
        if (flag == -2) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "the hot key:" + sqlAlterTableExtractPartition.getHotKeys().toString()
                    + " is already in a exclusive partition");
        }
        String hotKeyPartitioName = StringUtils.EMPTY;
        if (sqlAlterTableExtractPartition.getHotKeyPartitionName() != null) {
            hotKeyPartitioName =
                SQLUtils.normalizeNoTrim(sqlAlterTableExtractPartition.getHotKeyPartitionName().toString());
            for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
                if (record.partition_name.equalsIgnoreCase(hotKeyPartitioName)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("duplicate partition name:[%s]", hotKeyPartitioName));
                }
            }
        }

        if (StringUtils.isEmpty(hotKeyPartitioName)) {
            List<String> newPartNames =
                PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 3,
                    new TreeSet<>(String::compareToIgnoreCase), false);
            SqlIdentifier name1 = new SqlIdentifier(newPartNames.get(0), SqlParserPos.ZERO);
            SqlPartition sqlPartition1 = new SqlPartition(name1, null, SqlParserPos.ZERO);
            sqlAlterTableExtractPartition.getNewPartitions().add(sqlPartition1);

            SqlIdentifier name2 = new SqlIdentifier(newPartNames.get(1), SqlParserPos.ZERO);
            SqlPartition sqlPartition2 = new SqlPartition(name2, null, SqlParserPos.ZERO);
            sqlAlterTableExtractPartition.getNewPartitions().add(sqlPartition2);
            if (flag == 0) {
                SqlIdentifier name3 = new SqlIdentifier(newPartNames.get(2), SqlParserPos.ZERO);
                SqlPartition sqlPartition3 = new SqlPartition(name3, null, SqlParserPos.ZERO);
                sqlAlterTableExtractPartition.getNewPartitions().add(sqlPartition3);
            }
        } else {
            List<String> newPartNames = new ArrayList<>();
            if (flag == -1) {
                newPartNames.add(hotKeyPartitioName);
                newPartNames.addAll(
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1,
                        new TreeSet<>(String::compareToIgnoreCase), false));
            } else if (flag == 0) {
                List<String> boundPartNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 2,
                        new TreeSet<>(String::compareToIgnoreCase), false);
                newPartNames.add(boundPartNames.get(0));
                newPartNames.add(hotKeyPartitioName);
                newPartNames.add(boundPartNames.get(1));
            } else if (flag == 1) {
                newPartNames.addAll(
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1,
                        new TreeSet<>(String::compareToIgnoreCase), false));
                newPartNames.add(hotKeyPartitioName);
            } else {
                assert false;
            }
            for (String partName : newPartNames) {
                SqlIdentifier name = new SqlIdentifier(partName, SqlParserPos.ZERO);
                SqlPartition sqlPartition = new SqlPartition(name, null, SqlParserPos.ZERO);
                sqlAlterTableExtractPartition.getNewPartitions().add(sqlPartition);
            }
        }
        return getSplitPointsForHotValue(atVal, flag);
    }

    /**
     * i.e. p=[1,9), if atVal=1, extractPosition = -1, [1,2),[2,9)
     * if atval=3, extractPosition = 0, [1,3),[3,4),[4,9)
     * if atVal=8, extractPosition = 1, [1,8),[8,9)
     * if p=[1,2) atVal=1, extractPosition=-2 not need to split any more
     */
    protected List<Long[]> getSplitPointsForHotValue(Long[] atValHashCode,
                                                     int extractPosition) {
        List<Long[]> splitPoints = new ArrayList<>();
        if (extractPosition == -1) {
            splitPoints.add(getThexNextHashPoint(atValHashCode));
        } else if (extractPosition == 0) {
            splitPoints.add(atValHashCode);
            splitPoints.add(getThexNextHashPoint(atValHashCode));
        } else if (extractPosition == 1) {
            splitPoints.add(atValHashCode);
        }
        return splitPoints;
    }

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

    public AlterTableGroupExtractPartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableExtractPartition create(DDL ddl) {
        return new LogicalAlterTableExtractPartition(ddl);
    }

}
