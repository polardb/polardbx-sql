package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UseRangeScanTest {
    @Test
    public void testUseRangeScanCase1() {
        try (MockedStatic<PartitionUtils> partitionUtilsMockedStatic = Mockito.mockStatic(PartitionUtils.class)) {
            LogicalView logicalView = mock(LogicalView.class);
            ExecutionContext context = mock(ExecutionContext.class);
            ParamManager paramManager = mock(ParamManager.class);
            TargetTableInfo targetTableInfo = mock(TargetTableInfo.class);
            TargetTableInfoOneTable tableInfo = mock(TargetTableInfoOneTable.class);

            when(context.getParamManager()).thenReturn(paramManager);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_RANGE_SCAN)).thenReturn(true);
            when(context.getSqlType()).thenReturn(SqlType.SELECT);
            when(logicalView.getTableNames()).thenReturn(Lists.newArrayList("table1"));
            when(logicalView.isSingleGroup()).thenReturn(false);
            when(PartitionUtils.isNewPartShardTable(logicalView)).thenReturn(true);
            when(logicalView.buildTargetTableInfosForPartitionTb(context)).thenReturn(targetTableInfo);
            when(targetTableInfo.getTargetTableInfoList()).thenReturn(Lists.newArrayList(tableInfo));
            when(tableInfo.isUseSubPart()).thenReturn(false);
            List<String> partitionColumns = Lists.newArrayList("part_col");
            when(tableInfo.getPartColList()).thenReturn(partitionColumns);

            // Case 1: Match
            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, partitionColumns))
                .thenReturn(true);
            when(tableInfo.isAllPartSorted()).thenReturn(true);
            assertEquals(RangeScanMode.NORMAL, RangeScanUtils.useRangeScan(logicalView, context, false));

            // Case 1: Not match
            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, partitionColumns))
                .thenReturn(false);
            when(tableInfo.isAllPartSorted()).thenReturn(true);
            assertNull(RangeScanUtils.useRangeScan(logicalView, context, false));

            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, partitionColumns))
                .thenReturn(true);
            when(tableInfo.isAllPartSorted()).thenReturn(false);
            assertNull(RangeScanUtils.useRangeScan(logicalView, context, false));
        }
    }

    @Test
    public void testUseRangeScanCase2() {
        try (MockedStatic<PartitionUtils> partitionUtilsMockedStatic = Mockito.mockStatic(PartitionUtils.class)) {
            LogicalView logicalView = mock(LogicalView.class);
            ExecutionContext context = mock(ExecutionContext.class);
            ParamManager paramManager = mock(ParamManager.class);
            TargetTableInfo targetTableInfo = mock(TargetTableInfo.class);
            TargetTableInfoOneTable tableInfo = mock(TargetTableInfoOneTable.class);

            when(context.getParamManager()).thenReturn(paramManager);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_RANGE_SCAN)).thenReturn(true);
            when(context.getSqlType()).thenReturn(SqlType.SELECT);
            when(logicalView.getTableNames()).thenReturn(Lists.newArrayList("table1"));
            when(logicalView.isSingleGroup()).thenReturn(false);
            when(PartitionUtils.isNewPartShardTable(logicalView)).thenReturn(true);
            when(logicalView.buildTargetTableInfosForPartitionTb(context)).thenReturn(targetTableInfo);
            when(targetTableInfo.getTargetTableInfoList()).thenReturn(Lists.newArrayList(tableInfo));
            when(tableInfo.isUseSubPart()).thenReturn(true);
            List<String> subPartitionColumns = Lists.newArrayList("sub_part_col");
            when(tableInfo.getSubpartColList()).thenReturn(subPartitionColumns);

            // Case 2: Match
            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns))
                .thenReturn(true);
            when(tableInfo.getPrunedFirstLevelPartCount()).thenReturn(1);
            when(tableInfo.isAllSubPartSorted()).thenReturn(true);
            assertEquals(RangeScanMode.NORMAL, RangeScanUtils.useRangeScan(logicalView, context, false));

            // Case 2: Not match
            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns))
                .thenReturn(false);
            when(tableInfo.getPrunedFirstLevelPartCount()).thenReturn(1);
            when(tableInfo.isAllSubPartSorted()).thenReturn(true);
            assertNull(RangeScanUtils.useRangeScan(logicalView, context, false));

            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns))
                .thenReturn(true);
            when(tableInfo.getPrunedFirstLevelPartCount()).thenReturn(2);
            when(tableInfo.isAllSubPartSorted()).thenReturn(true);
            assertNull(RangeScanUtils.useRangeScan(logicalView, context, false));

            partitionUtilsMockedStatic.when(() -> PartitionUtils.isOrderKeyMatched(logicalView, subPartitionColumns))
                .thenReturn(true);
            when(tableInfo.getPrunedFirstLevelPartCount()).thenReturn(1);
            when(tableInfo.isAllSubPartSorted()).thenReturn(false);
            assertNull(RangeScanUtils.useRangeScan(logicalView, context, false));

        }
    }

}
