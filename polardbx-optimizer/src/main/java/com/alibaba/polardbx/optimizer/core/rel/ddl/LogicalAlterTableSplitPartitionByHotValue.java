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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionTupleRouteInfoBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class LogicalAlterTableSplitPartitionByHotValue extends BaseDdlOperation {

    protected AlterTableGroupSplitPartitionByHotValuePreparedData preparedData;

    public LogicalAlterTableSplitPartitionByHotValue(DDL ddl) {
        super(ddl, ((SqlAlterTable) (ddl.getSqlNode())).getObjectNames());
    }

    public LogicalAlterTableSplitPartitionByHotValue(DDL ddl, boolean notIncludeGsiName) {
        super(ddl);
        assert notIncludeGsiName;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return false;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST,
            "unarchive table " + schemaName + "." + tableName);
    }

    public void preparedData(ExecutionContext executionContext) {
        AlterTable alterTable = (AlterTable) relDdl;
        SqlAlterTable sqlAlterTable = (SqlAlterTable) alterTable.getSqlNode();
        assert sqlAlterTable.getAlters().size() == 1;

        assert sqlAlterTable.getAlters().get(0) instanceof SqlAlterTableSplitPartitionByHotValue;
        SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue =
            (SqlAlterTableSplitPartitionByHotValue) sqlAlterTable.getAlters().get(0);

        String logicalTableName = Util.last(((SqlIdentifier) alterTable.getTableName()).names);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(logicalTableName);

        List<Long[]> splitPoints = new ArrayList<>();
        int[] insertPos = {1, 1};
        /**
         * flag =
         * 2: both first new part and last new part are hot value(means all new parts are hot value)
         * -1: only first new part not include hot value
         * 1: only the last new part not include hot value
         * 0: neither of first new part and last new part is hot value
         */
        int flag = normalizeSqlSplitPartitionByHotValue(sqlAlterTableSplitPartitionByHotValue, partitionInfo,
            alterTable.getAllRexExprInfo(),
            executionContext, splitPoints, insertPos);

        String hotKeyPartNamePrefix = StringUtils.EMPTY;
        if (sqlAlterTableSplitPartitionByHotValue.getHotKeyPartitionName() != null) {
            hotKeyPartNamePrefix =
                SQLUtils.normalizeNoTrim(sqlAlterTableSplitPartitionByHotValue.getHotKeyPartitionName().toString());
        }
        boolean ignoreNameAndLocality = StringUtils.isEmpty(hotKeyPartNamePrefix);
        List<String> oldPartitions = new ArrayList<>();

        int i = insertPos[1];
        Set<String> oldPartitionNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        PartitionSpec splitPartitionSpec = null;
        boolean subPartitionSplit = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();
        boolean modifyNonTemplateSubPartition =
            subPartitionSplit && (sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName() != null);
        if (sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit()) {
            if (modifyNonTemplateSubPartition) {
                String parentPartName =
                    SQLUtils.normalizeNoTrim(
                        sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());
                PartitionSpec parentPartitionSpec =
                    partitionInfo.getPartitionBy().getPartitionByPartName(parentPartName);
                do {
                    String oldPartitionName = parentPartitionSpec.getNthSubPartition(i).getName();
                    splitPartitionSpec = parentPartitionSpec.getNthSubPartition(i);
                    oldPartitions.add(oldPartitionName);
                    oldPartitionNameSet.add(oldPartitionName);
                    i--;
                } while (i > insertPos[0]);
            } else {
                PartitionSpec parentPartitionSpec = partitionInfo.getPartitionBy().getNthPartition(1);
                do {
                    String oldPartitionName = parentPartitionSpec.getNthSubPartition(i).getTemplateName();
                    oldPartitionNameSet.add(oldPartitionName);
                    oldPartitions.add(oldPartitionName);
                    i--;
                } while (i > insertPos[0]);
            }
        } else {
            do {
                String oldPartitionName = partitionInfo.getPartitionBy().getNthPartition(i).getName();
                splitPartitionSpec = partitionInfo.getPartitionBy().getNthPartition(i);
                oldPartitions.add(oldPartitionName);
                oldPartitionNameSet.add(oldPartitionName);
                i--;
            } while (i > insertPos[0]);
        }

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());

        List<String> newPartitionNames =
            generateNewPartitionNames(tableGroupConfig, subPartitionSplit, oldPartitionNameSet, hotKeyPartNamePrefix,
                splitPoints.size(),
                flag);
        PartitionByDefinition partBy = partitionInfo.getPartitionBy();
        boolean hasSubPartition = partBy.getSubPartitionBy() != null;
        boolean useTemplateSubPartition = hasSubPartition && partBy.getSubPartitionBy().isUseSubPartTemplate();
        List<String> newSubPartitionNames = new ArrayList();
        if (!subPartitionSplit && hasSubPartition) {
            assert splitPartitionSpec != null && GeneralUtil.isNotEmpty(splitPartitionSpec.getSubPartitions());
            if (!useTemplateSubPartition) {
                newSubPartitionNames.addAll(
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig.getTableGroupRecord(),
                        partitionInfo.getPartitionBy().getPartitions().stream().map(o -> o.getName()).collect(
                            Collectors.toList()),
                        null,
                        newPartitionNames.size() * splitPartitionSpec.getSubPartitions().size(),
                        new HashSet<>(), true));
            } else {
                for (PartitionSpec subPartitionSpec : splitPartitionSpec.getSubPartitions()) {
                    assert subPartitionSpec.isUseSpecTemplate();
                    newSubPartitionNames.add(subPartitionSpec.getTemplateName());
                }
            }
        }

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName,
                tableGroupConfig.getTableGroupRecord().getTg_name());
        Map<String, List<Long[]>> splitPointInfos = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        splitPointInfos.put(logicalTableName, splitPoints);

        Map<String, String> newPartitionLocalities = new HashMap<>();
        if (sqlAlterTableSplitPartitionByHotValue.getLocality() != null) {
            String locality = sqlAlterTableSplitPartitionByHotValue.getLocality().toString();
            final String finalPrefix = hotKeyPartNamePrefix;
            newPartitionNames.stream().filter(o -> o.startsWith(finalPrefix))
                .forEach(o -> newPartitionLocalities.put(o, locality));
        }
        preparedData =
            new AlterTableSplitPartitionByHotValuePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setNewPartitionLocalities(newPartitionLocalities);

        Collections.reverse(oldPartitions);
        preparedData.setOldPartitionNames(oldPartitions);
        if (!subPartitionSplit && hasSubPartition) {
            preparedData.setLogicalParts(newPartitionNames);
            preparedData.setNewPartitionNames(newSubPartitionNames);
        } else if (subPartitionSplit && !modifyNonTemplateSubPartition) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
            preparedData.setNewPartitionNames(newPartitionNames);
        } else {
            if (modifyNonTemplateSubPartition) {
                String parentPartName =
                    SQLUtils.normalizeNoTrim(
                        sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());
                preparedData.setParentPartitionName(parentPartName);
            }
            preparedData.setNewPartitionNames(newPartitionNames);
        }
        preparedData.setUseTemplatePart(useTemplateSubPartition);
        preparedData.setSplitSubPartition(subPartitionSplit);
        preparedData.setOperateOnSubPartition(subPartitionSplit);
        preparedData.setHasSubPartition(hasSubPartition);
        preparedData.setTableGroupName(tableGroupConfig.getTableGroupRecord().getTg_name());
        preparedData.setInsertPos(insertPos);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE);
        preparedData.setHotKeyPartitionName(hotKeyPartNamePrefix);
        preparedData.setSplitPointInfos(splitPointInfos);
        preparedData.prepareInvisiblePartitionGroup(hasSubPartition);
        preparedData.setSourceSql(((SqlAlterTable) alterTable.getSqlNode()).getSourceSql());
        preparedData.setTargetImplicitTableGroupName(sqlAlterTable.getTargetImplicitTableGroupName());
        if (preparedData.needFindCandidateTableGroup()) {
            List<PartitionGroupRecord> newPartitionGroups = preparedData.getInvisiblePartitionGroups();
            Map<String, Pair<String, String>> mockOrderedTargetTableLocations =
                new TreeMap<>(String::compareToIgnoreCase);

            if (ignoreNameAndLocality) {
                for (int j = 0; j < newPartitionGroups.size(); j++) {
                    Pair<String, String> pair = new Pair<>("", "");
                    mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name, pair);
                }
            } else {
                for (int j = 0; j < newPartitionGroups.size(); j++) {
                    String mockTableName = "";
                    mockOrderedTargetTableLocations.put(newPartitionGroups.get(j).partition_name,
                        new Pair<>(mockTableName,
                            GroupInfoUtil.buildGroupNameFromPhysicalDb(newPartitionGroups.get(j).partition_name)));
                }
            }

            PartitionInfo newPartInfo = AlterTableGroupSnapShotUtils
                .getNewPartitionInfo(
                    preparedData,
                    partitionInfo,
                    false,
                    sqlAlterTableSplitPartitionByHotValue,
                    preparedData.getOldPartitionNames(),
                    preparedData.getNewPartitionNames(),
                    preparedData.getTableGroupName(),
                    null,
                    preparedData.getInvisiblePartitionGroups(),
                    mockOrderedTargetTableLocations,
                    executionContext);

            preparedData.findCandidateTableGroupAndUpdatePrepareDate(tableGroupConfig, newPartInfo, null,
                hotKeyPartNamePrefix,
                PartitionInfoUtil.IGNORE_PARTNAME_LOCALITY | PartitionInfoUtil.COMPARE_EXISTS_PART_LOCATION,
                executionContext);
        }
    }

    public List<String> generateNewPartitionNames(TableGroupConfig tableGroupConfig, boolean forSubPart,
                                                  Set<String> oldPartitionNames,
                                                  String hotKeyPartNamePrefix,
                                                  int splitSize, int flag) {
        List<String> newPartitionNames = null;
        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
        String firstTb = tableGroupConfig.getTables().get(0);
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(tableGroupRecord.getSchema()).getPartitionInfoManager()
                .getPartitionInfo(firstTb);
        List<String> partNames = new ArrayList<>();
        List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
        PartitionInfoUtil.getPartitionName(partitionInfo, partNames, subPartNamePairs);
        if (hotKeyPartNamePrefix.isEmpty() || PartitionNameUtil.isDefaultPartNamePattern(hotKeyPartNamePrefix)) {
            newPartitionNames =
                PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs,
                    splitSize + 1,
                    new TreeSet<>(String::compareToIgnoreCase), forSubPart);
        } else {
            if (flag == 2) {
                newPartitionNames = PartitionNameUtil
                    .autoGeneratePartitionNamesWithUserDefPrefix(hotKeyPartNamePrefix, splitSize + 1);
            } else if (flag == -1) {
                newPartitionNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs, 1,
                        new TreeSet<>(String::compareToIgnoreCase), forSubPart);
                newPartitionNames.addAll(PartitionNameUtil
                    .autoGeneratePartitionNamesWithUserDefPrefix(hotKeyPartNamePrefix, splitSize));
            } else if (flag == 1) {
                newPartitionNames = PartitionNameUtil
                    .autoGeneratePartitionNamesWithUserDefPrefix(hotKeyPartNamePrefix, splitSize + 1);
                //newPartitionNames.addAll(PartitionNameUtil.autoGeneratePartitionNames(tableGroupConfig, 1));
            } else if (flag == 0) {
                newPartitionNames = new ArrayList<>();
                List<String> boundPartNames =
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs, 2,
                        new TreeSet<>(String::compareToIgnoreCase), forSubPart);
                newPartitionNames.add(boundPartNames.get(0));
                newPartitionNames.addAll(PartitionNameUtil
                    .autoGeneratePartitionNamesWithUserDefPrefix(hotKeyPartNamePrefix, splitSize + 1 - 2));
                newPartitionNames.add(boundPartNames.get(1));
            } else {
                assert false;
            }
        }

        for (PartitionGroupRecord record : tableGroupConfig.getPartitionGroupRecords()) {
            if (newPartitionNames.contains(record.partition_name) && !oldPartitionNames
                .contains(record.partition_name)) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format("duplicate partition name:[%s]", record.partition_name));
            }
        }
        return newPartitionNames;
    }

    protected int normalizeSqlSplitPartitionByHotValue(
        SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue,
        PartitionInfo partitionInfo,
        Map<SqlNode, RexNode> partBoundExprInfo,
        ExecutionContext executionContext,
        List<Long[]> splitPoints,
        int[] insertPos) {

        boolean splitSubPartition = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();

        List<SqlNode> hotKeys = sqlAlterTableSplitPartitionByHotValue.getHotKeys();
        List<RexNode> hotKeysRexNode = new ArrayList<>();
        for (SqlNode hotKey : hotKeys) {
            RexNode rexNode = partBoundExprInfo.get(hotKey);
            hotKeysRexNode.add(rexNode);
        }

        if (splitSubPartition && partitionInfo.getPartitionBy().getSubPartitionBy() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "subpartition is not exists");
        }
        String schemaName = partitionInfo.getTableSchema();
        String tblName = partitionInfo.getTableName();
        PartitionByDefinition partByDef =
            splitSubPartition ? partitionInfo.getPartitionBy().getSubPartitionBy() : partitionInfo.getPartitionBy();
        int partColCnt = partByDef.getPartitionColumnNameList().size();
        PartitionStrategy strategy = partByDef.getStrategy();
        int hotKeyValColCnt = hotKeys.size();
        SqlNumericLiteral splitPartCntLiteral =
            (SqlNumericLiteral) sqlAlterTableSplitPartitionByHotValue.getPartitions();
        int splitPartCnt = splitPartCntLiteral.intValue(true);
        if (!(strategy == PartitionStrategy.KEY || strategy == PartitionStrategy.RANGE_COLUMNS)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "only support for key/range column partition to split partition/subpartition by hot value");
        }

        if (hotKeyValColCnt > partColCnt) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                "the column count of hot key should less than partition columns");
        } else {
            if (hotKeyValColCnt == partColCnt && splitPartCnt != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    String.format(
                        "only one split partition is allowed when the column count of hot key is the same as the full partition columns of table[%s.%s]",
                        schemaName, tblName));
            }
        }

        return getSplitPointsForHotValue(sqlAlterTableSplitPartitionByHotValue, hotKeysRexNode,
            partitionInfo, executionContext, splitPoints, insertPos);

    }

    public AlterTableGroupSplitPartitionByHotValuePreparedData getPreparedData() {
        return preparedData;
    }

    public static LogicalAlterTableSplitPartitionByHotValue create(DDL ddl) {
        return new LogicalAlterTableSplitPartitionByHotValue(ddl);
    }


    /*原来(最大值max, 最小值:0)：
    p1:a1,max,max
    p2:a2,max,max
    p3:a3,max,max


    第1次热点分裂后(ah是热点):
    p1:a1,max,max
    p2:a2,max,max
    p_ah_low_bnd:ah-1,max,max
    p_ah_1:ah,max/4,max
    p_ah_2:ah,max/2,max
    p_ah_3:ah,3max/4,max
    p_ah_4:ah+1,max,max
    p3:a3,max,max

    p2与p_ah_low_bnd恰好相等，则合并
    p_ah_4与p3恰好相等，则合并
    如果ah=0，则p_ah_low_bnd：0，max-1，max
    ah不可能等于max，因为我们计算的MAX(hashCode)=max-1
    p_ah_4一定不大于p3*/

    /**
     * <pre>
     * case1: one-col, split 1 partition by hot val hashcode of ah:
     * p1:a1
     * p2:a2
     * p3:a3
     * =》after splitting
     * p1:a1
     * p2:a2
     * p_ah_low_bnd:ah-1
     * p_ah_1: ah+1,
     * { [ah-1, ah+1) including target range [ah,ah] ) }
     * p3:a3
     *
     * case2_1: two-col, split 1 partition by prefix one col hot val hashcode  of ah :
     * p1:a1,max
     * p2:a2,max
     * p3:a3,max
     * =》 after splitting
     * p1:a1,max
     * p2:a2,max
     * p_ah_low_bnd:ah-1,max
     * p_ah_1: ah+1,max
     *  { [(ah-1,max), (ah+1,max)) including target range [(ah,min),(ah,max)] ) }
     * p3:a3,max
     *
     * case2_2: two-col, split 4 partition by prefix 1 col hot val hashcode  of ah :
     * p1:a1,max
     * p2:a2,max
     * p3:a3,max
     * =》 after splitting
     * p1:a1,max
     * p2:a2,max
     * p_ah_low_bnd:ah-1,max
     * p_ah_1: ah,max/4
     * p_ah_2: ah,2*max/4
     * p_ah_3: ah,3*max/4
     * p_ah_4: ah+1,max
     *      { [(ah-1,max), (ah+1,max)) including target range [(ah,min),(ah,max)] ) }
     * p3:a3,max
     *
     * case2_3: two-col, split 1 partition by prefix 2 col hot val hashcode  of (ah,ah2) :
     * p1:a1,max
     * p2:a2,max
     * p3:a3,max
     * =》 after splitting
     * p1:a1,max
     * p2:a2,max
     * p_ah_ah2_low_bnd:ah,ah2-1
     * p_ah_ah2_1: ah,ah2+1
     * { [(ah,ah2-1), (ah,ah2+1)) including target range [(ah,ah2),(ah,ah2)] ) }
     * p3:a3,max
     *
     * case3_1: three-col, split 4 partition by prefix 2 col hot val hashcode  of (ah,ah2) :
     * p1:a1,max,max
     * p2:a2,max,max
     * p3:a3,max,max
     * =》 after splitting
     * p1:a1,max,max
     * p2:a2,max,max
     * p_ah_ah2_low_bnd:ah,ah2-1,max
     * p_ah_ah2_1: ah,ah2,max/4
     * p_ah_ah2_2: ah,ah2,2*max/4
     * p_ah_ah2_3: ah,ah2,3*max/4
     * p_ah_ah2_4: ah,ah2+1,max
     *  { [(ah,ah2-1,max), (ah,ah2+1,max)) including target range [(ah,ah2,min),(ah,ah2,max)] ) }
     * p3:a3,max,max
     *
     * case3_2: three-col, split 1 partition by prefix 2 col hot val hashcode  of (ah,ah2) :
     * p1:a1,max,max
     * p2:a2,max,max
     * p3:a3,max,max
     * =》 after splitting
     * p1:a1,max,max
     * p2:a2,max,max
     * p_ah_ah2_low_bnd:ah,ah2-1,max
     * p_ah_ah2_1: ah,ah2+1,max
     *  { [(ah,ah2-1,max), (ah,ah2+1,max)) including target range [(ah,ah2,min),(ah,ah2,max)] ) }
     * p3:a3,max,max
     *
     * case3_3: three-col, split 1 partition by prefix 3 col hot val hashcode  of (ah,ah2,ah3) :
     * p1:a1,max,max
     * p2:a2,max,max
     * p3:a3,max,max
     * =》 after splitting
     * p1:a1,max,max
     * p2:a2,max,max
     * p_ah_ah2_ah3_low_bnd:ah,ah2,ah3-1
     * p_ah_ah2_ah3_1: ah,ah2,ah3+1
     *  { [(ah-1,ah2,ah3-1), (ah,ah2,ah3+1)) including target range [(ah,ah2,ah3),(ah,ah2,ah3)] ) }
     * p3:a3,max,max
     *
     * case3_4: three-col, split 1 partition by prefix 1 col hot val hashcode  of (ah) :
     * p1:a1,max,max
     * p2:a2,max,max
     * p3:a3,max,max
     * =》 after splitting
     * p1:a1,max,max
     * p2:a2,max,max
     * p_ah_low_bnd:ah-1,max,max
     * p_ah_1: ah+1,max,max
     *  { [(ah-1,max,max), (a+1,max,max)) including target range [(ah,min,min),(ah,max,max)] ) }
     * p3:a3,max,max
     *
     * case3_5: three-col, split 4 partition by prefix 1 col hot val hashcode  of (ah) :
     * p1:a1,max,max
     * p2:a2,max,max
     * p3:a3,max,max
     * =》 after splitting
     * p1:a1,max,max
     * p2:a2,max,max
     * p_ah_low_bnd:ah-1,max,max
     * p_ah_1: ah,max/4,max
     * p_ah_2: ah,2*max/4,max
     * p_ah_3: ah,3*max/4,max
     * p_ah_4: ah+1,max,max
     *  { [(ah-1,max,max), (a+1,max,max)) including target range [(ah,min,min),(ah,max,max)] ) }
     * p3:a3,max,max
     *
     * ...
     *
     *
     * </pre>
     */
    private int getSplitPointsForHotValue(SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue,
                                          List<RexNode> hotKeys,
                                          PartitionInfo partitionInfo,
                                          ExecutionContext ec,
                                          List<Long[]> outputFinalSplitPoints,
                                          int[] outputInsertPos) {

        boolean splitSubPartition = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();
        SqlNode partitions = sqlAlterTableSplitPartitionByHotValue.getPartitions();
        PartitionByDefinition partByDef =
            splitSubPartition ? partitionInfo.getPartitionBy().getSubPartitionBy() : partitionInfo.getPartitionBy();

        List<RelDataType> relDataTypes = partByDef.getPartitionExprTypeList();
        PartitionIntFunction partIntFunc = partByDef.getPartIntFunc();
        PartitionStrategy strategy = partByDef.getStrategy();
        List<PartitionBoundVal> oneBndVal = new ArrayList<>();
        int partColCnt = partByDef.getPartitionColumnNameList().size();
        int hotKeyValColCnt = hotKeys.size();
        int splitIntoParts = ((SqlNumericLiteral) partitions).intValue(true);

        /**
         * Generate hash code for each col value of a hot val
         */
        for (int i = 0; i < hotKeyValColCnt; i++) {
            RexNode oneBndExpr = hotKeys.get(i);
            RelDataType bndValDt = relDataTypes.get(i);
            PartitionInfoUtil.validateBoundValueExpr(oneBndExpr, bndValDt, partIntFunc, strategy);
            PartitionBoundVal bndVal =
                PartitionPrunerUtils
                    .getBoundValByRexExpr(oneBndExpr, bndValDt, PartFieldAccessType.DDL_EXECUTION, ec);
            oneBndVal.add(bndVal);
        }
        SearchDatumInfo datum = new SearchDatumInfo(oneBndVal);
        Long[] hotValHashCodeArr = partByDef.getHasher().calcHashCodeForKeyStrategy(datum);

        /**
         * Compute the delta range for each new split partitions of hot value
         */
        long rngDelta = PartitionInfoUtil.getHashSpaceMaxValue();
        if (splitIntoParts > 0) {
            rngDelta = 2 * (PartitionInfoUtil.getHashSpaceMaxValue() / splitIntoParts);
        }

        /**
         * Generate hash code for the lower bound of all the new split partitions of hot value
         */
        Long[] lowerBndHashCodeArr = new Long[partColCnt];
        int lastHotKeyValColIdx = hotKeyValColCnt - 1;
        for (int i = 0; i < hotKeyValColCnt - 1; i++) {
            lowerBndHashCodeArr[i] = hotValHashCodeArr[i];
        }
        long lastColHashValOfHotVal = hotValHashCodeArr[lastHotKeyValColIdx];
        // the value of hashCode will be the range [Long.min+1, Long.max-1],
        // so lastColHashValOfHotVal-1 will not be low over stack
        long lastColHashValOfLowerBndVal = lastColHashValOfHotVal - 1;
        lowerBndHashCodeArr[lastHotKeyValColIdx] = lastColHashValOfLowerBndVal;
        for (int i = hotKeyValColCnt; i < partColCnt; i++) {
            lowerBndHashCodeArr[i] = PartitionInfoUtil.getHashSpaceMaxValue();
        }

        /**
         * Generate hash code for the upper bound (just bound value of the last partition)
         * of all the new split partitions of hot value
         */
        Long[] upperBndHashCodeArr = new Long[partColCnt];
        for (int i = 0; i < hotKeyValColCnt - 1; i++) {
            upperBndHashCodeArr[i] = hotValHashCodeArr[i];
        }
        // the value of hashCode will be the range [Long.min+1, Long.max-1],
        // so lastColHashValOfHotVal-1 will not be up over stack
        long lastColHashValOfUpperBndVal = lastColHashValOfHotVal + 1;
        upperBndHashCodeArr[lastHotKeyValColIdx] = lastColHashValOfUpperBndVal;
        for (int i = hotKeyValColCnt; i < partColCnt; i++) {
            upperBndHashCodeArr[i] = PartitionInfoUtil.getHashSpaceMaxValue();
        }

        /**
         * Generate bound value for each partition to be split
         */
        List<Long[]> splitPoints = new ArrayList<>();
        splitPoints.add(lowerBndHashCodeArr);
        int nextColIdxOfHotVal = hotKeyValColCnt;
        for (int i = 0; i < splitIntoParts - 1; i++) {
            Long[] newPartBndValue = new Long[partColCnt];
            for (int k = 0; k < hotKeyValColCnt; k++) {
                newPartBndValue[k] = hotValHashCodeArr[k];
            }
            if (nextColIdxOfHotVal < partColCnt) {
                newPartBndValue[nextColIdxOfHotVal] = PartitionInfoUtil.getHashSpaceMinValue() + (i + 1) * rngDelta;
            }
            for (int k = nextColIdxOfHotVal + 1; k < partColCnt; k++) {
                newPartBndValue[k] = PartitionInfoUtil.getHashSpaceMaxValue();
            }
            splitPoints.add(newPartBndValue);
        }
        splitPoints.add(upperBndHashCodeArr);
        return generateFinalSplitPoints(sqlAlterTableSplitPartitionByHotValue, partitionInfo, splitPoints, ec,
            outputFinalSplitPoints, outputInsertPos);
    }

    /**
     * @return 2: both first new part and last new part are hot value(means all new parts are hot value)
     * -1: only first new part not include hot value
     * 1: only the last new part not include hot value
     * 0: neither of first new part and last new part is hot value
     */
    private int generateFinalSplitPoints(SqlAlterTableSplitPartitionByHotValue sqlAlterTableSplitPartitionByHotValue,
                                         PartitionInfo partitionInfo, List<Long[]> splitPoints,
                                         ExecutionContext ec, List<Long[]> finalSplitPoints, int[] insertPos) {
        boolean firstPartIsNotHotValue = false;
        boolean lastPartIsNotHotValue = false;
        boolean splitSubPartition = sqlAlterTableSplitPartitionByHotValue.isSubPartitionsSplit();
        boolean isModifyPartition =
            splitSubPartition && sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName() != null;
        PartKeyLevel pkl = splitSubPartition ? PartKeyLevel.SUBPARTITION_KEY : PartKeyLevel.PARTITION_KEY;
        Integer parentPartPosi = 1;
        if (isModifyPartition) {
            String parentPartName =
                SQLUtils.normalizeNoTrim(
                    sqlAlterTableSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());
            PartitionSpec parentPartSpec = partitionInfo.getPartitionBy()
                .getPartitionByPartName(parentPartName);
            parentPartPosi = parentPartSpec.getPosition().intValue();
        }
        for (int i = 0; i < splitPoints.size(); i++) {
            SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(splitPoints.get(i));
            PartitionSpec partitionSpec =
                PartitionTupleRouteInfoBuilder.getPartitionSpecByHashCode(splitPoints.get(i),
                    pkl, parentPartPosi, partitionInfo, ec);
            if (partitionSpec == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                    "can't find the correct split point");
            }
            PartitionSpec prePartSpec = null;
            boolean isNotFirstPartition = partitionSpec.getPosition() != 1;

            if (splitSubPartition) {
                if (isNotFirstPartition) {
                    prePartSpec =
                        partitionInfo.getPartitionBy().getNthPartition(parentPartPosi)
                            .getNthSubPartition(partitionSpec.getPosition().intValue() - 1);
                }
            } else {
                if (isNotFirstPartition) {
                    prePartSpec =
                        partitionInfo.getPartitionBy().getNthPartition(partitionSpec.getPosition().intValue() - 1);
                }
            }
            if (i == 0 || i == splitPoints.size() - 1) {
                if (isNotFirstPartition) {
                    if (prePartSpec.getBoundSpaceComparator()
                        .compare(prePartSpec.getBoundSpec().getSingleDatum(), searchDatumInfo) != 0) {
                        finalSplitPoints.add(splitPoints.get(i));
                        if (i == 0) {
                            insertPos[0] = partitionSpec.getPosition().intValue() - 1;
                        } else {
                            insertPos[1] = partitionSpec.getPosition().intValue();
                        }
                    } else {//else the split point is equal to the prePartSpec, merge it
                        if (i == 0) {
                            insertPos[0] = partitionSpec.getPosition().intValue() - 1;
                            firstPartIsNotHotValue = true;
                        } else {
                            insertPos[1] = partitionSpec.getPosition().intValue() - 1;
                            lastPartIsNotHotValue = true;
                        }
                    }

                } else {
                    finalSplitPoints.add(splitPoints.get(i));
                    insertPos[0] = partitionSpec.getPosition().intValue();
                }
            } else {
                finalSplitPoints.add(splitPoints.get(i));
            }
        }
        if (firstPartIsNotHotValue && lastPartIsNotHotValue) {
            return 2;
        } else if (firstPartIsNotHotValue) {
            return -1;
        } else if (lastPartIsNotHotValue) {
            return 1;
        } else {
            return 0;
        }
    }

}
