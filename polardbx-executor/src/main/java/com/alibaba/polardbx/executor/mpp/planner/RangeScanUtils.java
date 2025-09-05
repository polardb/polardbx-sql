package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.SplitInfo;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RangeScanUtils {
    public static RangeScanMode useRangeScan(LogicalView logicalView, ExecutionContext context) {
        return useRangeScan(logicalView, context, false);
    }

    /**
     * Determines whether to use the range scan mode based on the given logical view, execution context, and flag indicating whether the root node is a merge sort.
     *
     * @param logicalView The logical view representing the logical structure of tables involved in the query.
     * @param context The execution context containing various context information required during query execution.
     * @param rootIsMergeSort A flag indicating whether the root node is a merge sort.
     * @return The appropriate range scan mode if conditions are met; otherwise, returns null.
     */
    public static RangeScanMode useRangeScan(LogicalView logicalView, ExecutionContext context,
                                             boolean rootIsMergeSort) {
        if (!context.getParamManager().getBoolean(ConnectionParams.ENABLE_RANGE_SCAN)) {
            return null;
        }
        boolean isDml = SqlType.isDML(context.getSqlType());
        if (isDml && !context.getParamManager().getBoolean(ConnectionParams.ENABLE_RANGE_SCAN_FOR_DML)) {
            return null;
        }

        Set<String> allTableNames = new HashSet<>();
        for (String tableName : logicalView.getTableNames()) {
            allTableNames.add(tableName.toLowerCase());
        }
        // Check if the logical view contains multiple tables
        if (allTableNames.size() > 1) {
            return null;
        }
        // single group no need range scan
        if (logicalView.isSingleGroup()) {
            return null;
        }
        // Check if it is a new partitioned table
        if (!PartitionUtils.isNewPartShardTable(logicalView)) {
            return null;
        }

        TargetTableInfo targetTableInfo = logicalView.buildTargetTableInfosForPartitionTb(context);

//        targetTableInfo.getTargetTableInfoList()
        // contains multi table
        if (targetTableInfo.getTargetTableInfoList().size() > 1) {
            return null;
        }

        TargetTableInfoOneTable tableInfo = targetTableInfo.getTargetTableInfoList().get(0);

        // Case 1: The sort key is the level-one partition key, and the level-one partitions are ordered.
        List<String> partitionColumns = tableInfo.getPartColList();
        boolean isSortKeyPartKey = PartitionUtils.isOrderKeyMatched(logicalView, partitionColumns);
        boolean isPartSorted = tableInfo.isAllPartSorted();
        if (!tableInfo.isUseSubPart() && !(isSortKeyPartKey && isPartSorted)) {
            return null;
        }

        // Case 2: The sort key is the level-two partition key, and the level-two partitions are ordered.
        if (tableInfo.isUseSubPart()) {
            // check sub part key first.
            List<String> subPartitionColumns = tableInfo.getSubpartColList();
            boolean isSortKeySubPartKey = PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns);
            boolean isSubPartSorted = tableInfo.getPrunedFirstLevelPartCount() == 1
                && tableInfo.isAllSubPartSorted();
            if (!(isSortKeySubPartKey && isSubPartSorted)) {
                return null;
            }
        }

        return determinRangeScanMode(logicalView, context, rootIsMergeSort);
    }

    public static RangeScanMode checkSplitInfo(SplitInfo splitInfo, RangeScanMode mode) {
        for (List<Split> splitList : splitInfo.getSplits()) {
            if (!RangeScanUtils.checkSplit(splitList)) {
                return null;
            }
        }
        return mode;
    }

    public static boolean checkSplit(List<Split> splitList) {
        return splitList.stream().allMatch(split -> {
            if (!(split.getConnectorSplit() instanceof JdbcSplit)) {
                return false;
            }
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            Set<String> allTableNames = new HashSet<>();
            for (List<String> tableNames : jdbcSplit.getTableNames()) {
                for (String tableName : tableNames) {
                    allTableNames.add(tableName.toLowerCase());
                }
            }
            return allTableNames.size() == 1;
        });
    }

    /**
     * Determines the range scan mode based on the given logical view, execution context, and whether the root node is a merge sort.
     *
     * @param logicalView The logical view representing the query's logical structure.
     * @param context The execution context containing parameters and state required for executing the query.
     * @param rootIsMergeSort A flag indicating whether the current node is the root merge sort node.
     * @return The determined range scan mode.
     */
    static RangeScanMode determinRangeScanMode(LogicalView logicalView, ExecutionContext context,
                                               boolean rootIsMergeSort) {
        RangeScanMode rangeScanMode =
            RangeScanMode.getMode(context.getParamManager().getString(ConnectionParams.RANGE_SCAN_MODE));
        if (rangeScanMode != null) {
            return rangeScanMode;
        }
        if (rootIsMergeSort) {
            return RangeScanMode.ADAPTIVE;
        }
        return RangeScanMode.NORMAL;
    }

}
