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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS_OF_57;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class LogicalViewExternalizeTest extends BaseRuleTest {
    /**
     * test logical tablescan in logicalview.
     */
    @Test
    public void testWriter() {

        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        final RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        LogicalFilter filter = LogicalFilter.create(logicalView,
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "operation", true),
                rexBuilder.makeExactLiteral(BigDecimal.TEN)));
        final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        final RelDataType bigIntType = relOptCluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        HashAgg aggregate = HashAgg.create(
            relOptCluster.traitSet().replace(DrdsConvention.INSTANCE),
            filter,
            ImmutableBitSet.of(0),
            null,
            ImmutableList.of(AggregateCall.create(SqlStdOperatorTable.COUNT,
                    true,
                    false,
                    ImmutableList.of(1),
                    -1,
                    bigIntType,
                    "c"),
                AggregateCall.create(SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    ImmutableList.<Integer>of(),
                    -1,
                    bigIntType,
                    "d")));
        aggregate.explain(writer);

        String s = writer.asString();

        assertThat(s.replaceAll("\n", "").replaceAll("[\t ]", ""), is(XX));

    }

    /**
     * Unit test for {@link DRDSRelJsonReader}.
     */
    @Test
    public void testReader() {
        final DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
        RelNode node;
        try {
            node = reader.read(EXPECTED);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        writer.setPlannerContext(PlannerContext.getPlannerContext(node));
        node.explain(writer);
        String s = writer.asString();

        PlannerContext plannerContext = PlannerContext.getPlannerContext(node);
        assertTrue(plannerContext.isUseColumnar() && plannerContext.getColumnarMaxShardCnt() == 40);
        assertThat(
            Util.toLinux(s).replaceAll("\n", "").replaceAll("\\\\n", "").replaceAll("[\t ]", ""),
            is(EXPECTED));
    }

    private final String EXPECTED =
        "{\"rels\":[{\"id\":\"0\",\"relOp\":\"LogicalView\",\"table\":[\"optest\",\"emp\"],\"tableNames\":[\"emp\"],\"pushDownOpt\":{\"pushrels\":[{\"id\":\"0\",\"relOp\":\"LogicalTableScan\",\"table\":[\"optest\",\"emp\"],\"flashback\":null,\"inputs\":[]}]},\"schemaName\":\"optest\",\"partitions\":[],\"flashback\":null},{\"id\":\"1\",\"relOp\":\"LogicalFilter\",\"condition\":{\"op\":\"SqlBinaryOperator=\",\"operands\":[{\"input\":2,\"name\":\"$2\",\"type\":{\"type\":\"TINYINT\",\"nullable\":true,\"precision\":1}},10],\"type\":{\"type\":\"BIGINT\",\"nullable\":true}}},{\"id\":\"2\",\"relOp\":\"HashAgg\",\"group\":[0],\"aggs\":[{\"agg\":\"SqlCountAggFunctionCOUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":true,\"operands\":[1],\"filter\":-1},{\"agg\":\"SqlCountAggFunctionCOUNT\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":false,\"operands\":[],\"filter\":-1}]}],\"args\":\"{\\\"columnarMaxShardCnt\\\":40,\\\"useColumnar\\\":true}\"}";

    private final String AS_OF_TSO_WRITER =
        "{  \"rels\": [    {      \"id\": \"0\",      \"relOp\": \"LogicalView\",      \"table\": [        \"optest\",        \"emp\"      ],      \"tableNames\": [        \"emp\"      ],      \"pushDownOpt\": {        \"pushrels\": [          {            \"id\": \"0\",            \"relOp\": \"LogicalTableScan\",            \"table\": [              \"optest\",              \"emp\"            ],            \"flashback\": {              \"index\": 0,              \"skindex\": -1,              \"subindex\": -1,              \"reltype\": {                \"type\": \"BIGINT\",                \"nullable\": true              },              \"type\": \"DYNAMIC\"            },            \"flashbackOperator\": \"SqlAsOf57OperatorAS OF TSO\",            \"inputs\": []          }        ]      },      \"schemaName\": \"optest\",      \"partitions\": [],      \"flashback\": {        \"index\": 0,        \"skindex\": -1,        \"subindex\": -1,        \"reltype\": {          \"type\": \"BIGINT\",          \"nullable\": true        },        \"type\": \"DYNAMIC\"      },      \"flashbackOperator\": \"SqlAsOf57OperatorAS OF TSO\"    }  ]}";

    @Test
    public void testAsOfTsoWriter() {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        final RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        final RelDataType bigIntType = relOptCluster.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        scan.setFlashback(rexBuilder.makeDynamicParam(bigIntType, 0));
        scan.setFlashbackOperator(AS_OF_57);
        logicalView.setFlashback(rexBuilder.makeDynamicParam(bigIntType, 0));
        logicalView.setFlashbackOperator(AS_OF_57);

        final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        logicalView.explain(writer);

        String s = writer.asString();

        assertThat(s.replaceAll("\n", "").replaceAll("\t", ""), is(AS_OF_TSO_WRITER));
    }

    @Test
    public void testAsOfTsoReader() {
        final DRDSRelJsonReader reader = new DRDSRelJsonReader(relOptCluster, schema, null, false);
        RelNode node;
        try {
            node = reader.read(AS_OF_TSO_WRITER);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Assert.assertTrue(node instanceof LogicalView);
        Assert.assertNotNull(((LogicalView) node).getFlashback());
        Assert.assertSame(((LogicalView) node).getFlashbackOperator(), AS_OF_57);
    }
}
