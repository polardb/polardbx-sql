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

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.locality.LocalityInfoUtils;
import com.alibaba.polardbx.optimizer.partition.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableGroupSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlAlterTableGroup;
import org.apache.calcite.sql.SqlAlterTableGroupSplitPartitionByHotValue;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
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

        String firstTblNameInGroup = tableGroupConfig.getAllTables().get(0).getLogTbRec().tableName;
        Map<String, List<Long[]>> splitPointInfos = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, SplitPointContext> splitPointCtxMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        List<PartitionInfo> allLogPartInfoList = new ArrayList<>();
        for (int i = 0; i < tableGroupConfig.getAllTables().size(); i++) {
            String tbNameInGrp = tableGroupConfig.getAllTables().get(i).getLogTbRec().tableName;
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
        do {
            String oldPartitionName = firstTblPartInfo.getPartitionBy().getNthPartition(i).getName();
            oldPartitions.add(oldPartitionName);
            oldPartitionNameSet.add(oldPartitionName);
            i--;
        } while (i > insertPos[0]);

        List<String> newPartitionNames =
            generateNewPartitionNames(tableGroupConfig, oldPartitionNameSet, hotKeyPartNamePrefix, splitPoints.size(),
                flag);
        preparedData = new AlterTableGroupSplitPartitionByHotValuePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setWithHint(targetTablesHintCache != null);

        Collections.reverse(oldPartitions);
        preparedData.setOldPartitionNames(oldPartitions);
        preparedData.setNewPartitionNames(newPartitionNames);
        preparedData.setTableGroupName(tableGroupName);
        preparedData.setInsertPos(insertPos);
        preparedData.setSplitPointInfos(splitPointInfos);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE);
        preparedData.setHotKeyPartitionName(hotKeyPartNamePrefix);
    }

    public static LogicalAlterTableGroupSplitPartitionByHotValue create(DDL ddl) {
        return new LogicalAlterTableGroupSplitPartitionByHotValue(ddl);
    }

    private static class SplitPointContext {
        public PartitionInfo partInfo;
        public List<Long[]> splitPoints = new ArrayList<>();
        public int[] insertPos = {1, 1};
        public int flag;

        public SplitPointContext() {
        }
    }
}
