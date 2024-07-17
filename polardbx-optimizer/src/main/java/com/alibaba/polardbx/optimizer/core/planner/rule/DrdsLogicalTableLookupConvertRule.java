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

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableLookup;

/**
 * @author dylan
 */
public class DrdsLogicalTableLookupConvertRule extends ConverterRule {
    public static final DrdsLogicalTableLookupConvertRule SMP_INSTANCE =
        new DrdsLogicalTableLookupConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsLogicalTableLookupConvertRule COL_INSTANCE =
        new DrdsLogicalTableLookupConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsLogicalTableLookupConvertRule(Convention outConvention) {
        super(LogicalTableLookup.class, Convention.NONE, outConvention,
            "DrdsLogicalTableLookupConvertRule");
        this.outConvention = outConvention;

    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalTableLookup logicalTableLookup = (LogicalTableLookup) rel;

        LogicalTableLookup newLogicalTableLookup =
            logicalTableLookup.copy(
                logicalTableLookup.getTraitSet().simplify().replace(outConvention),
                convert(logicalTableLookup.getJoin().getLeft(),
                    logicalTableLookup.getJoin().getLeft().getTraitSet().simplify().replace(outConvention)),
                logicalTableLookup.getJoin().getRight(),
                logicalTableLookup.getIndexTable(),
                logicalTableLookup.getPrimaryTable(),
                logicalTableLookup.getProject(),
                logicalTableLookup.getJoin(),
                logicalTableLookup.isRelPushedToPrimary(),
                logicalTableLookup.getHints());

        RelOptCost fixedCost = CheckJoinHint.check(logicalTableLookup.getJoin(), HintType.CMD_BKA_JOIN);
        if (fixedCost != null) {
            newLogicalTableLookup.setFixedCost(fixedCost);
        }
        return newLogicalTableLookup;
    }
}

