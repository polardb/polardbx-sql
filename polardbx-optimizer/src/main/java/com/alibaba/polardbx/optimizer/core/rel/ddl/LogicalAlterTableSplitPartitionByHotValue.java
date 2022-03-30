package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupLocation;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionByHotValuePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTable;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlAlterTableSplitPartitionByHotValue;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class LogicalAlterTableSplitPartitionByHotValue extends LogicalAlterTableGroupSplitPartitionByHotValue {

    public LogicalAlterTableSplitPartitionByHotValue(DDL ddl) {
        super(ddl);
    }

    @Override
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

        List<String> oldPartitions = new ArrayList<>();
        List<GroupDetailInfoExRecord> targetGroupDetailInfoExRecords =
            TableGroupLocation.getOrderedGroupList(schemaName);

        int i = insertPos[1];
        Set<String> oldPartitionNameSet = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        do {
            String oldPartitionName = partitionInfo.getPartitionBy().getNthPartition(i).getName();
            oldPartitions.add(oldPartitionName);
            oldPartitionNameSet.add(oldPartitionName);
            i--;
        } while (i > insertPos[0]);

        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");
        TableGroupConfig tableGroupConfig =
            oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());

        List<String> newPartitionNames =
            generateNewPartitionNames(tableGroupConfig, oldPartitionNameSet, hotKeyPartNamePrefix, splitPoints.size(),
                flag);

        Map<String, List<Long[]>> splitPointInfos = new TreeMap<>();
        splitPointInfos.put(logicalTableName, splitPoints);

        preparedData = new AlterTableGroupSplitPartitionByHotValuePreparedData();

        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);
        preparedData.setWithHint(targetTablesHintCache != null);

        Collections.reverse(oldPartitions);
        preparedData.setOldPartitionNames(oldPartitions);
        preparedData.setNewPartitionNames(newPartitionNames);
        preparedData.setTableGroupName(tableGroupConfig.getTableGroupRecord().getTg_name());
        preparedData.setInsertPos(insertPos);
        preparedData.setTargetGroupDetailInfoExRecords(targetGroupDetailInfoExRecords);
        preparedData.prepareInvisiblePartitionGroup();
        preparedData.setTaskType(ComplexTaskMetaManager.ComplexTaskType.SPLIT_HOT_VALUE);
        preparedData.setHotKeyPartitionName(hotKeyPartNamePrefix);
        preparedData.setSplitPointInfos(splitPointInfos);
    }

    public static LogicalAlterTableSplitPartitionByHotValue create(DDL ddl) {
        return new LogicalAlterTableSplitPartitionByHotValue(ddl);
    }
}
