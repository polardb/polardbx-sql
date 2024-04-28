package com.alibaba.polardbx.optimizer.core.rule;

import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlValuesTableSource;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class COLLogicalViewConvertRuleTest {
    Method method;

    class MockSource {
        Boolean enableTest;
        PartitionStrategy strategy;
        List<String> columns;
        List<SqlNode> partitionExprList;

        public MockSource(Boolean enableTest, PartitionStrategy strategy, List<String> columns,
                          List<SqlNode> partitionExprList) {
            this.enableTest = enableTest;
            this.strategy = strategy;
            this.columns = columns;
            this.partitionExprList = partitionExprList;
        }
    }

    @Test
    public void testCanPartitionWise() throws Exception {
        method = COLLogicalViewConvertRule.class.getDeclaredMethod("canPartitionWise", TableMeta.class, boolean.class);
        method.setAccessible(true);
        testFrame(new MockSource(
                true,
                PartitionStrategy.LIST,
                Lists.newArrayList("a"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO))),
            false);
        testFrame(new MockSource(
                true,
                PartitionStrategy.KEY,
                Lists.newArrayList("a"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO))),
            true);
        testFrame(new MockSource(
                false,
                PartitionStrategy.LIST,
                Lists.newArrayList("a"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO))),
            false);
        testFrame(new MockSource(
                false,
                PartitionStrategy.DIRECT_HASH,
                Lists.newArrayList("a", "b"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO))),
            false);
        testFrame(new MockSource(
                false,
                PartitionStrategy.DIRECT_HASH,
                Lists.newArrayList("a", "b"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO), new SqlIdentifier("b", SqlParserPos.ZERO))),
            false);
        testFrame(new MockSource(
                false,
                PartitionStrategy.DIRECT_HASH,
                Lists.newArrayList("a", "b"),
                Lists.newArrayList(new SqlValuesTableSource(SqlParserPos.ZERO, null))),
            false);
        testFrame(new MockSource(
                false,
                PartitionStrategy.DIRECT_HASH,
                Lists.newArrayList("a"),
                Lists.newArrayList(new SqlIdentifier("a", SqlParserPos.ZERO))),
            true);
    }

    public void testFrame(MockSource mockSource, boolean result) throws Exception {
        TableMeta tm = Mockito.mock(TableMeta.class);

        Mockito.when(tm.getPartitionInfo()).thenAnswer(
            (Answer<PartitionInfo>) invocation -> {
                PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
                Mockito.when(partitionInfo.getPartitionBy()).thenAnswer(
                    (Answer<PartitionByDefinition>) invocation1 -> {
                        PartitionByDefinition partitionByDefinition = Mockito.mock(PartitionByDefinition.class);
                        Mockito.when(partitionByDefinition.getStrategy()).thenAnswer(
                            (Answer<PartitionStrategy>) invocation3 -> mockSource.strategy
                        );
                        Mockito.when(partitionByDefinition.getPartitionExprList()).thenAnswer(
                            (Answer<List<SqlNode>>) invocation4 -> mockSource.partitionExprList
                        );
                        return partitionByDefinition;
                    }
                );
                Mockito.when(partitionInfo.getPartitionColumns()).thenAnswer(
                    (Answer<List<String>>) invocation2 -> mockSource.columns
                );
                return partitionInfo;
            }
        );

        Assert.assertEquals(method.invoke(COLLogicalViewConvertRule.INSTANCE, tm, mockSource.enableTest), result);
    }
}
