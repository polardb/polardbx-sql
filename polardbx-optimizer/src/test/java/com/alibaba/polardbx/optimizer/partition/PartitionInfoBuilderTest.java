package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartBoundValBuilder;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartition;
import org.apache.calcite.sql.SqlSubPartition;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartitionInfoBuilderTest {

    @Mock
    private SqlPartition newPartition;

    @Mock
    private SqlSubPartition newSubPartition;

    @Mock
    private PartitionByDefinition subPartByDef;

    @Mock
    private Map<Integer, Map<SqlNode, RexNode>> partBoundExprInfoByLevel;

    @Mock
    private PartBoundValBuilder partBoundValBuilder;

    @Mock
    private PartitionSpec parentPartitionSpec;

    private AtomicInteger phySubPartCounter = new AtomicInteger(0);

    private AtomicInteger subPartPosCounter = new AtomicInteger(0);

    private boolean isColumnarIndex = true;

    private ExecutionContext context;

    @Before
    public void setUp() {
        context = new ExecutionContext();
        // Setup common mock behaviors here if any
    }

    @Test
    public void testBuildSubPartSpecForKey() {
        // Setup specific to this test
        try (MockedStatic<PartitionInfoBuilder> mockStatic = Mockito.mockStatic(PartitionInfoBuilder.class)) {
            PartitionSpec spec = new PartitionSpec();
            when(PartitionInfoBuilder.buildPartSpecByAstParamInner(any())).thenReturn(spec);

            List<ColumnMeta> partitionFiles = new ArrayList<>();
            SearchDatumComparator searchDatumComparator = mock(SearchDatumComparator.class);
            PartitionIntFunction partIntFunc = mock(PartitionIntFunction.class);
            when(newSubPartition.getName()).thenReturn(new SqlIdentifier("p0", SqlParserPos.ZERO));
            when(subPartByDef.getPartitionFieldList()).thenReturn(partitionFiles);
            when(subPartByDef.getPruningSpaceComparator()).thenReturn(searchDatumComparator);
            when(subPartByDef.getPartIntFunc()).thenReturn(partIntFunc);
            when(subPartByDef.getStrategy()).thenReturn(PartitionStrategy.valueOf("LIST"));

            mockStatic.when(
                () -> PartitionInfoBuilder.buildSubPartSpecForKey(
                    newSubPartition,
                    subPartByDef,
                    partBoundExprInfoByLevel,
                    partBoundValBuilder,
                    parentPartitionSpec,
                    phySubPartCounter,
                    subPartPosCounter,
                    isColumnarIndex,
                    context)).thenCallRealMethod();
            PartitionInfoBuilder.buildSubPartSpecForKey(
                newSubPartition,
                subPartByDef,
                partBoundExprInfoByLevel,
                partBoundValBuilder,
                parentPartitionSpec,
                phySubPartCounter,
                subPartPosCounter,
                isColumnarIndex,
                context);

            mockStatic.when(
                () -> PartitionInfoBuilder.buildSubPartSpecForRangeOrList(
                    newSubPartition, subPartByDef, null,
                    partBoundExprInfoByLevel, parentPartitionSpec, phySubPartCounter, subPartPosCounter,
                    isColumnarIndex,
                    context)).thenCallRealMethod();
            PartitionInfoBuilder.buildSubPartSpecForRangeOrList(newSubPartition, subPartByDef, null,
                partBoundExprInfoByLevel, parentPartitionSpec, phySubPartCounter, subPartPosCounter, isColumnarIndex,
                context);

            mockStatic.when(
                () -> PartitionInfoBuilder.buildPartSpec(
                    newPartition, subPartByDef, null, partBoundExprInfoByLevel,
                    phySubPartCounter, subPartPosCounter, isColumnarIndex, context)).thenCallRealMethod();
            PartitionInfoBuilder.buildPartSpec(newPartition, subPartByDef, null, partBoundExprInfoByLevel,
                phySubPartCounter, subPartPosCounter, isColumnarIndex, context);
        }
    }

}
