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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
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
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
            node = reader.read(XX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        final DRDSRelJsonWriter writer = new DRDSRelJsonWriter(false);
        node.explain(writer);
        String s = writer.asString();

        assertThat(Util.toLinux(s).replaceAll("\n", "").replaceAll("[\t ]", ""), is(XX));
    }
}
