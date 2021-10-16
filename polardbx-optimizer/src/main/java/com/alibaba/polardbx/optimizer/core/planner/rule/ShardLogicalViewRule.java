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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

/**
 * @author chenmo.cm
 */
public class ShardLogicalViewRule extends RelOptRule {

    public ShardLogicalViewRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final ShardLogicalViewRule INSTANCE = new ShardLogicalViewRule(operand(LogicalView.class, none()),
        "ShardLogicalViewRule");

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalView logicalView = (LogicalView) call.rels[0];
        /**
         * 根据数据展开 logicalView 节点
         * <p>
         * 需要知道： 1. 缓存的逻辑执行计划 2. 当前 SQL 拆分键的取值
         * </p>
         */
        if (logicalView.getFinishShard()) {
            return;
        }

        if (needGather(logicalView)) {
            call.transformTo(Gather.create(logicalView));
        } else {
            call.transformTo(logicalView);
        }
        logicalView.setFinishShard(true);
    }

    /**
     * 是否需要添加Union节点，以下情况无需添加:
     *
     * <pre>
     *   1. LogicalView仅在一个Group上执行，即单库扫描
     *   2. 有OrderBy下推，上层应该是MergeSort节点
     * </pre>
     */
    private boolean needGather(LogicalView logicalView) {
        if (logicalView.isSingleGroup()) {
            return false;
        }
        return true;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalView logicalView = (LogicalView) call.rels[0];
        if (logicalView instanceof LogicalModifyView) {
            return false;
        }
        return !logicalView.getFinishShard();
    }
}
