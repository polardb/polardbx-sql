package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.OrcTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartRouteFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

public class InExpressionParamRewriterInPrepareModeTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    MockedStatic<DynamicParamExpression> dynamicParamExpressionMockedStatic;
    DynamicParamExpression dynamicParamExpression;

    final int columnIndex = 3;
    final int dynamicIndex = 2;
    final String phyTable = "test_phy_table";

    private OSSTableScan scan;
    private ExecutionContext context;
    private RexCall call;
    private MockedStatic<PartRouteFunction> partRouteFunctionMockedStatic;
    private MockedStatic<PartitionBoundVal> partitionBoundValMockedStatic;
    private MockedStatic<PartitionPrunerUtils> prunerUtilsMockedStatic;
    private MockedStatic<PartPrunedResult> partPrunedResultMockedStatic;

    @Before
    public void setUpDynamicExpression() {
        dynamicParamExpressionMockedStatic = Mockito.mockStatic(DynamicParamExpression.class);
        dynamicParamExpression = Mockito.mock(DynamicParamExpression.class);
        dynamicParamExpressionMockedStatic.when(
                () -> DynamicParamExpression.create(anyInt(), any(), anyInt(), anyInt()))
            .thenReturn(dynamicParamExpression);

        Mockito.when(dynamicParamExpression.eval(any(), any()))
            .thenReturn(111)
            .thenReturn(222)
            .thenReturn(333)
            .thenReturn(444)
            .thenReturn(555);
    }

    @Before
    public void setupCall() {
        // create IN expression.

        RexInputRef rexNode1 = REX_BUILDER.makeInputRef(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            columnIndex
        );

        RexDynamicParam rexDynamicParam1 = REX_BUILDER.makeDynamicParam(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            0
        );
        RexDynamicParam rexDynamicParam2 = REX_BUILDER.makeDynamicParam(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            1
        );
        RexDynamicParam rexDynamicParam3 = REX_BUILDER.makeDynamicParam(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            2
        );
        RexDynamicParam rexDynamicParam4 = REX_BUILDER.makeDynamicParam(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            3
        );
        RexDynamicParam rexDynamicParam5 = REX_BUILDER.makeDynamicParam(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            4
        );

        RexCall rexNode2 = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.ROW),
            TddlOperatorTable.ROW,
            ImmutableList.of(rexDynamicParam1, rexDynamicParam2, rexDynamicParam3, rexDynamicParam4, rexDynamicParam5)
        );

        call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.IN,
            ImmutableList.of(rexNode1, rexNode2)
        );
    }

    @Before
    public void setupPartition() {
        PartitionRouter partitionRouter = Mockito.mock(PartitionRouter.class);

        partRouteFunctionMockedStatic = Mockito.mockStatic(PartRouteFunction.class);
        partRouteFunctionMockedStatic.when(() -> PartRouteFunction.getRouterByPartInfo(any(), any(), any()))
            .thenReturn(partitionRouter);

        PartitionBoundVal partitionBoundVal = Mockito.mock(PartitionBoundVal.class);
        partitionBoundValMockedStatic = Mockito.mockStatic(PartitionBoundVal.class);
        partitionBoundValMockedStatic.when(() -> PartitionBoundVal.createPartitionBoundVal(any(), any()))
            .thenReturn(partitionBoundVal);

        PartitionRouter.RouterResult routerResult = Mockito.mock(PartitionRouter.RouterResult.class);
        Mockito.when(partitionRouter.routePartitions(any(), any(), any())).thenReturn(routerResult);

        BitSet allPartBitSet = Mockito.mock(BitSet.class);
        prunerUtilsMockedStatic = Mockito.mockStatic(PartitionPrunerUtils.class);
        prunerUtilsMockedStatic.when(() -> PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(any()))
            .thenReturn(allPartBitSet);
        prunerUtilsMockedStatic.when(
                () -> PartitionPrunerUtils.setPartBitSetByStartEnd(any(), anyInt(), anyInt(), anyBoolean()))
            .thenReturn(allPartBitSet);
        prunerUtilsMockedStatic.when(() -> PartitionPrunerUtils.setPartBitSetForPartList(any(), any(), anyBoolean()))
            .thenReturn(allPartBitSet);

        Mockito.when(routerResult.getStrategy()).thenReturn(PartitionStrategy.HASH);

        PartPrunedResult partPrunedResult = Mockito.mock(PartPrunedResult.class);
        partPrunedResultMockedStatic = Mockito.mockStatic(PartPrunedResult.class);
        partPrunedResultMockedStatic.when(
                () -> PartPrunedResult.buildPartPrunedResult(any(), any(), any(), any(), anyBoolean()))
            .thenReturn(partPrunedResult);

        PhysicalPartitionInfo physicalPartitionInfo = Mockito.mock(PhysicalPartitionInfo.class);
        Mockito.when(physicalPartitionInfo.getPhyTable())
            .thenReturn(phyTable + "1")
            .thenReturn(phyTable + "2")
            .thenReturn(phyTable + "3")
            .thenReturn(phyTable + "4")
            .thenReturn(phyTable + "5");
        Mockito.when(partPrunedResult.getPrunedPartitions()).thenReturn(ImmutableList.of(physicalPartitionInfo));

    }

    @Before
    public void setupContext() {
        scan = Mockito.mock(OSSTableScan.class);
        Mockito.when(scan.getSchemaName()).thenReturn("test_db");
        Mockito.when(scan.getLogicalTableName()).thenReturn("test_table");

        context = Mockito.mock(ExecutionContext.class);

        // table & schema mock.
        TableMeta tableMeta = Mockito.mock(TableMeta.class);
        SchemaManager schemaManager = Mockito.mock(SchemaManager.class);
        Mockito.when(tableMeta.getSchemaName()).thenReturn("test_db");
        Mockito.when(tableMeta.getTableName()).thenReturn("test_table");

        Mockito.when(context.getSchemaManager(anyString())).thenReturn(schemaManager);
        Mockito.when(schemaManager.getTable(anyString())).thenReturn(tableMeta);

        // partition mock
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);

        PartitionByDefinition partitionByDefinition = Mockito.mock(PartitionByDefinition.class);
        Mockito.when(partitionInfo.getPartitionBy()).thenReturn(partitionByDefinition);
        Mockito.when(partitionByDefinition.getSubPartitionBy()).thenReturn(null);

        // partition 4
        Mockito.when(partitionByDefinition.getPartitions()).thenReturn(ImmutableList.of(
            Mockito.mock(PartitionSpec.class),
            Mockito.mock(PartitionSpec.class),
            Mockito.mock(PartitionSpec.class),
            Mockito.mock(PartitionSpec.class)
        ));

        // column meta mock.
        ColumnMeta columnMeta = Mockito.mock(ColumnMeta.class);
        Mockito.when(columnMeta.getDataType()).thenReturn(DataTypes.LongType);
        Mockito.when(columnMeta.getName()).thenReturn("test_col");
        Mockito.when(partitionByDefinition.getFullPartitionColumnMetas()).thenReturn(ImmutableList.of(columnMeta));
        Mockito.when(tableMeta.getAllColumns()).thenReturn(ImmutableList.of(
            Mockito.mock(ColumnMeta.class),
            Mockito.mock(ColumnMeta.class),
            Mockito.mock(ColumnMeta.class),
            columnMeta, // columnIndex = 3
            Mockito.mock(ColumnMeta.class)
        ));

        // mock orc node.
        OrcTableScan orcTableScan = Mockito.mock(OrcTableScan.class);
        Mockito.when(scan.getOrcNode()).thenReturn(orcTableScan);
        Mockito.when(orcTableScan.getInProjects()).thenReturn(ImmutableList.of(0, 1, 2, 3, 4));

        Mockito.when(orcTableScan.getFilters()).thenReturn(ImmutableList.of(call));
    }

    @Test
    public void test() {

        Map<Integer, Map<String, List>> result = InExpressionParamRewriter.rewriterParams(scan, context);

        System.out.println(result);

        Assert.assertEquals(
            "{0={test_phy_table1=[111]}, 1={test_phy_table2=[222]}, 2={test_phy_table3=[333]}, 3={test_phy_table4=[444]}, 4={test_phy_table5=[555]}}",
            result.toString());
    }

    @After
    public void tearDown() {
        if (dynamicParamExpressionMockedStatic != null) {
            dynamicParamExpressionMockedStatic.close();
        }

        if (partRouteFunctionMockedStatic != null) {
            partRouteFunctionMockedStatic.close();
        }

        if (partitionBoundValMockedStatic != null) {
            partitionBoundValMockedStatic.close();
        }

        if (prunerUtilsMockedStatic != null) {
            prunerUtilsMockedStatic.close();
        }

        if (partPrunedResultMockedStatic != null) {
            partPrunedResultMockedStatic.close();
        }
    }
}
