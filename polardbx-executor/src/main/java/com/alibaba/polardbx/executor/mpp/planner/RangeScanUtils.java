package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import org.apache.calcite.rel.core.Sort;

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
        // Check if the logical view contains multiple tables
        if (logicalView.getTableNames().size() != 1) {
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
        // contains multi table
        if (targetTableInfo.getTargetTableInfoList().size() > 1) {
            return null;
        }

        TargetTableInfoOneTable tableInfo = targetTableInfo.getTargetTableInfoList().get(0);
        if (!PartitionUtils.isTablePartOrdered(tableInfo)) {
            return null;
        }
        if (!PartitionUtils.isOrderKeyMatched(logicalView, tableInfo)) {
            return null;
        }
        return determinRangeScanMode(logicalView, context, rootIsMergeSort);
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
