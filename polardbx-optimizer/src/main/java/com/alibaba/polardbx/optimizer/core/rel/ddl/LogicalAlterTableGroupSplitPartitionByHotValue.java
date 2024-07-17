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
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.gms.util.TableGroupNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTablePartitionHelper;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class LogicalAlterTableGroupSplitPartitionByHotValue extends LogicalAlterTableSplitPartitionByHotValue {

    public LogicalAlterTableGroupSplitPartitionByHotValue(DDL ddl) {
        super(ddl, true);
    }

    @Override
    public void preparedData(ExecutionContext executionContext) {
        AlterTableGroupSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (AlterTableGroupSplitPartitionByHotValue) relDdl;
        String tableGroupName = alterTableGroupSplitPartitionByHotValue.getTableGroupName();
        SqlAlterTableGroup sqlAlterTableGroup = (SqlAlterTableGroup) alterTableGroupSplitPartitionByHotValue.getAst();
        assert sqlAlterTableGroup.getAlters().size() == 1;

        assert sqlAlterTableGroup.getAlters().get(0) instanceof SqlAlterTableGroupSplitPartitionByHotValue;
        SqlAlterTableGroupSplitPartitionByHotValue sqlAlterTableGroupSplitPartitionByHotValue =
            (SqlAlterTableGroupSplitPartitionByHotValue) sqlAlterTableGroup.getAlters().get(0);

        TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
            .getTableGroupConfigByName(tableGroupName);

        String firstTblNameInGroup = tableGroupConfig.getAllTables().get(0);
        Map<String, List<Long[]>> splitPointInfos = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, SplitPointContext> splitPointCtxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        List<PartitionInfo> allLogPartInfoList = new ArrayList<>();
        for (int i = 0; i < tableGroupConfig.getAllTables().size(); i++) {
            String tbNameInGrp = tableGroupConfig.getAllTables().get(i);
            PartitionInfo partInfo =
                OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tbNameInGrp);
            allLogPartInfoList.add(partInfo);
        }

        for (int i = 0; i < allLogPartInfoList.size(); i++) {
            SplitPointContext splitPointCtx = new SplitPointContext();
            PartitionInfo partInfo = allLogPartInfoList.get(i);
            String tbNameInGrp = partInfo.getTableName();

            List<Long[]> splitPoints = new ArrayList<>();
            int[] insertPos = {1, 1};

            /**
             * flag =
             * 2: both first new part and last new part are hot value(means all new parts are hot value)
             * -1: only first new part not include hot value
             * 1: only the last new part not include hot value
             * 0: neither of first new part and last new part is hot value
             */
            int flag = normalizeSqlSplitPartitionByHotValue(sqlAlterTableGroupSplitPartitionByHotValue, partInfo,
                alterTableGroupSplitPartitionByHotValue.getPartBoundExprInfo(),
                executionContext, splitPoints, insertPos);

            splitPointCtx.partInfo = partInfo;
            splitPointCtx.splitPoints = splitPoints;
            splitPointCtx.insertPos = insertPos;
            splitPointCtx.flag = flag;
            splitPointInfos.put(tbNameInGrp, splitPoints);
            splitPointCtxMap.put(tbNameInGrp, splitPointCtx);
        }

//        String tableInCurrentGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
//        PartitionInfo partitionInfo =
//            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableInCurrentGroup);
//        List<Long[]> splitPoints = new ArrayList<>();
//        int[] insertPos = {1, 1};
//
//        /**
//         * flag =
//         * 2: both first new part and last new part are hot value(means all new parts are hot value)
//         * -1: only first new part not include hot value
//         * 1: only the last new part not include hot value
//         * 0: neither of first new part and last new part is hot value
//         */
//        int flag = normalizeSqlSplitPartitionByHotValue(sqlAlterTableGroupSplitPartitionByHotValue, partitionInfo,
//            alterTableGroupSplitPartitionByHotValue.getPartBoundExprInfo(),
//            executionContext, splitPoints, insertPos);

        String hotKeyPartNamePrefix = StringUtils.EMPTY;
        if (sqlAlterTableGroupSplitPartitionByHotValue.getHotKeyPartitionName() != null) {
            hotKeyPartNamePrefix = SQLUtils.normalizeNoTrim(
                sqlAlterTableGroupSplitPartitionByHotValue.getHotKeyPartitionName().toString());
        }

        SplitPointContext firstTbInGrpSplitPointCtx = splitPointCtxMap.get(firstTblNameInGroup);
        PartitionInfo firstTblPartInfo = firstTbInGrpSplitPointCtx.partInfo;
        List<String> oldPartitions = new ArrayList<>();
        Set<String> oldPartitionNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            LocalityInfoUtils.getAllowedGroupInfoOfTableGroup(schemaName, tableGroupName);

        int[] insertPos = firstTbInGrpSplitPointCtx.insertPos;
        List<Long[]> splitPoints = firstTbInGrpSplitPointCtx.splitPoints;
        int flag = firstTbInGrpSplitPointCtx.flag;

        int i = insertPos[1];
        PartitionSpec splitPartitionSpec = null;
        boolean subPartitionSplit = sqlAlterTableGroupSplitPartitionByHotValue.isSubPartitionsSplit();
        boolean modifyNonTemplateSubPartition =
            subPartitionSplit && (sqlAlterTableGroupSplitPartitionByHotValue.getModifyPartitionName() != null);
        if (sqlAlterTableGroupSplitPartitionByHotValue.isSubPartitionsSplit()) {
            if (modifyNonTemplateSubPartition) {
                String parentPartName =
                    SQLUtils.normalizeNoTrim(
                        sqlAlterTableGroupSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());
                PartitionSpec parentPartitionSpec =
                    firstTblPartInfo.getPartitionBy().getPartitionByPartName(parentPartName);
                do {
                    String oldPartitionName = parentPartitionSpec.getNthSubPartition(i).getName();
                    splitPartitionSpec = parentPartitionSpec.getNthSubPartition(i);
                    oldPartitions.add(oldPartitionName);
                    oldPartitionNameSet.add(oldPartitionName);
                    i--;
                } while (i > insertPos[0]);
            } else {
                PartitionSpec parentPartitionSpec = firstTblPartInfo.getPartitionBy().getNthPartition(1);
                do {
                    String oldPartitionName = parentPartitionSpec.getNthSubPartition(i).getTemplateName();
                    oldPartitionNameSet.add(oldPartitionName);
                    oldPartitions.add(oldPartitionName);
                    i--;
                } while (i > insertPos[0]);
            }
        } else {
            do {
                String oldPartitionName = firstTblPartInfo.getPartitionBy().getNthPartition(i).getName();
                splitPartitionSpec = firstTblPartInfo.getPartitionBy().getNthPartition(i);
                oldPartitions.add(oldPartitionName);
                oldPartitionNameSet.add(oldPartitionName);
                i--;
            } while (i > insertPos[0]);
        }

        List<String> newPartitionNames =
            generateNewPartitionNames(tableGroupConfig, subPartitionSplit, oldPartitionNameSet, hotKeyPartNamePrefix,
                splitPoints.size(),
                flag);

        PartitionByDefinition partBy = firstTblPartInfo.getPartitionBy();
        boolean hasSubPartition = partBy.getSubPartitionBy() != null;
        boolean useTemplateSubPartition = hasSubPartition && partBy.getSubPartitionBy().isUseSubPartTemplate();
        List<String> newSubPartitionNames = new ArrayList();
        if (!subPartitionSplit && hasSubPartition) {
            assert splitPartitionSpec != null && GeneralUtil.isNotEmpty(splitPartitionSpec.getSubPartitions());
            if (!useTemplateSubPartition) {
                TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
                List<String> partNames = new ArrayList<>();
                List<Pair<String, String>> subPartNamePairs = new ArrayList<>();
                PartitionInfoUtil.getPartitionName(firstTblPartInfo, partNames, subPartNamePairs);
                newSubPartitionNames.addAll(
                    PartitionNameUtil.autoGeneratePartitionNames(tableGroupRecord, partNames, subPartNamePairs,
                        newPartitionNames.size() * splitPartitionSpec.getSubPartitions().size(),
                        new HashSet<>(), true));
            } else {
                for (PartitionSpec subPartitionSpec : splitPartitionSpec.getSubPartitions()) {
                    assert subPartitionSpec.isUseSpecTemplate();
                    newSubPartitionNames.add(subPartitionSpec.getTemplateName());
                }
            }
        }

        Map<String, String> newPartitionLocalities = new HashMap<>();
        if (sqlAlterTableGroupSplitPartitionByHotValue.getLocality() != null) {
            String locality = sqlAlterTableGroupSplitPartitionByHotValue.getLocality().toString();
            final String finalPrefix = hotKeyPartNamePrefix;
            newPartitionNames.stream().filter(o -> o.startsWith(finalPrefix))
                .forEach(o -> newPartitionLocalities.put(o, locality));
        }
        preparedData = new AlterTableGroupSplitPartitionByHotValuePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);
        preparedData.setNewPartitionLocalities(newPartitionLocalities);
        Collections.reverse(oldPartitions);
        preparedData.setOldPartitionNames(oldPartitions);

        if (!subPartitionSplit && hasSubPartition) {
            preparedData.setLogicalParts(newPartitionNames);
            preparedData.setNewPartitionNames(newSubPartitionNames);
        } else if (subPartitionSplit && !modifyNonTemplateSubPartition) {
            List<String> logicalParts = new ArrayList<>();
            for (PartitionSpec partitionSpec : firstTblPartInfo.getPartitionBy().getPartitions()) {
                logicalParts.add(partitionSpec.getName());
            }
            preparedData.setLogicalParts(logicalParts);
            preparedData.setNewPartitionNames(newPartitionNames);
        } else {
            if (modifyNonTemplateSubPartition) {
                String parentPartName =
                    SQLUtils.normalizeNoTrim(
                        sqlAlterTableGroupSplitPartitionByHotValue.getModifyPartitionName().getSimpleName());
                preparedData.setParentPartitionName(parentPartName);
            }
            preparedData.setNewPartitionNames(newPartitionNames);
        }
        preparedData.setUseTemplatePart(useTemplateSubPartition);
        preparedData.setSplitSubPartition(subPartitionSplit);
        preparedData.setOperateOnSubPartition(subPartitionSplit);

        preparedData.setHasSubPartition(hasSubPartition);

        preparedData.setTableGroupName(tableGroupName);
        preparedData.setInsertPos(insertPos);
        preparedData.setSplitPointInfos(splitPointInfos);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup(hasSubPartition);
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE);
        preparedData.setHotKeyPartitionName(hotKeyPartNamePrefix);
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        AlterTableGroupSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (AlterTableGroupSplitPartitionByHotValue) relDdl;
        String tableGroup = alterTableGroupSplitPartitionByHotValue.getTableGroupName();
        throw new TddlRuntimeException(ErrorCode.ERR_UNARCHIVE_FIRST, "unarchive tablegroup " + tableGroup);
    }

    @Override
    public boolean checkIfBindFileStorage(ExecutionContext executionContext) {
        AlterTableGroupSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (AlterTableGroupSplitPartitionByHotValue) relDdl;
        String tableGroupName = alterTableGroupSplitPartitionByHotValue.getTableGroupName();
        return !CheckOSSArchiveUtil.checkTableGroupWithoutOSS(schemaName, tableGroupName);
    }

    @Override
    public boolean checkIfFileStorage(ExecutionContext executionContext) {
        final AlterTableGroupSplitPartitionByHotValue alterTableGroupSplitPartitionByHotValue =
            (AlterTableGroupSplitPartitionByHotValue) relDdl;
        final String tableGroupName = alterTableGroupSplitPartitionByHotValue.getTableGroupName();

        return TableGroupNameUtil.isFileStorageTg(tableGroupName);
    }

    public static LogicalAlterTableGroupSplitPartitionByHotValue create(DDL ddl) {
        return new LogicalAlterTableGroupSplitPartitionByHotValue(
            AlterTablePartitionHelper.fixAlterTableGroupDdlIfNeed(ddl));
    }

    private static class SplitPointContext {
        public PartitionInfo partInfo;
        public List<Long[]> splitPoints = new ArrayList<>();
        public int[] insertPos = {1, 1};
        public int flag;

        public SplitPointContext() {
        }
    }

//    /**
//     * @return 2: both first new part and last new part are hot value(means all new parts are hot value)
//     * -1: only first new part not include hot value
//     * 1: only the last new part not include hot value
//     * 0: neither of first new part and last new part is hot value
//     */
//    private int generateFinalSplitPoints(PartitionInfo partitionInfo, List<Long[]> splitPoints,
//                                         ExecutionContext ec, List<Long[]> finalSplitPoints, int[] insertPos) {
//        boolean firstPartIsNotHotValue = false;
//        boolean lastPartIsNotHotValue = false;
//        for (int i = 0; i < splitPoints.size(); i++) {
//            SearchDatumInfo searchDatumInfo = SearchDatumInfo.createFromHashCodes(splitPoints.get(i));
//            PartitionSpec partitionSpec =
//                PartitionTupleRouteInfoBuilder.getPartitionSpecByHashCode(splitPoints.get(i),
//                    PartKeyLevel.PARTITION_KEY, null, partitionInfo, ec);
//            if (partitionSpec == null) {
//                throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
//                    "can't find the correct split point");
//            }
//            if (i == 0 || i == splitPoints.size() - 1) {
//                if (partitionSpec.getPosition() != 1) {
//                    PartitionSpec prePartSpec =
//                        partitionInfo.getPartitionBy().getNthPartition(partitionSpec.getPosition().intValue() - 1);
//                    if (prePartSpec.getBoundSpaceComparator()
//                        .compare(prePartSpec.getBoundSpec().getSingleDatum(), searchDatumInfo) != 0) {
//                        finalSplitPoints.add(splitPoints.get(i));
//                        if (i == 0) {
//                            insertPos[0] = partitionSpec.getPosition().intValue() - 1;
//                        } else {
//                            insertPos[1] = partitionSpec.getPosition().intValue();
//                        }
//                    } else {//else the split point is equal to the prePartSpec, merge it
//                        if (i == 0) {
//                            insertPos[0] = partitionSpec.getPosition().intValue() - 1;
//                            firstPartIsNotHotValue = true;
//                        } else {
//                            insertPos[1] = partitionSpec.getPosition().intValue() - 1;
//                            lastPartIsNotHotValue = true;
//                        }
//                    }
//
//                } else {
//                    finalSplitPoints.add(splitPoints.get(i));
//                    insertPos[0] = partitionSpec.getPosition().intValue();
//                }
//            } else {
//                finalSplitPoints.add(splitPoints.get(i));
//            }
//        }
//    }
}
