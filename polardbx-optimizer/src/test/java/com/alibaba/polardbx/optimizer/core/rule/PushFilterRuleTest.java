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

package com.alibaba.polardbx.optimizer.core.rule;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Map;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.FORBID_APPLY_CACHE;
import static com.alibaba.polardbx.common.properties.ConnectionProperties.FORCE_APPLY_CACHE;

public class PushFilterRuleTest extends BaseRuleTest {
    @Test
    public void testDonotPushCondition() {
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        LogicalView logicalView2 = LogicalView.create(scan, scan.getTable());
        final RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexCorrelVariable correlVariable =
            (RexCorrelVariable) rexBuilder.makeCorrel(logicalView2.buildCurRowType(), new CorrelationId(1));
        RexNode corRex = rexBuilder.makeFieldAccess(correlVariable, 2);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            corRex,
            rexBuilder.makeExactLiteral(BigDecimal.TEN));
        LogicalFilter filter = LogicalFilter.create(logicalView,
            condition);

        Assert.assertTrue(PushFilterRule.doNotPush(filter, logicalView));

        setParams(filter, FORCE_APPLY_CACHE, true);
//        correlVariable.setApplyMark(true);

        Assert.assertTrue(PushFilterRule.doNotPush(filter, logicalView));

        setParams(filter, FORBID_APPLY_CACHE, true);

        Assert.assertTrue(!PushFilterRule.doNotPush(filter, logicalView));
    }

    private void setParams(RelNode relNode, String key, Object o) {
        Map<String, Object> params = PlannerContext.getPlannerContext(relNode).getExtraCmds();
        params.put(key, o);
        PlannerContext.getPlannerContext(relNode).setExtraCmds(params);
    }

}
