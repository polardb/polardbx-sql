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

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RecursiveCTE;

public class DrdsRecursiveCTEConvertRule extends ConverterRule {
    public static final DrdsRecursiveCTEConvertRule SMP_INSTANCE =
        new DrdsRecursiveCTEConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsRecursiveCTEConvertRule COL_INSTANCE =
        new DrdsRecursiveCTEConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsRecursiveCTEConvertRule(Convention outConvention) {
        super(RecursiveCTE.class, Convention.NONE, outConvention, "DrdsRecursiveCTEConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final RecursiveCTE recursiveCTE = (RecursiveCTE) rel;
        rel.getCluster().getPlanner().getContext().unwrap(PlannerContext.class).setHasRecursiveCte(true);
        return new RecursiveCTE(recursiveCTE.getCluster(),
            recursiveCTE.getTraitSet().simplify().replace(outConvention),
            convert(recursiveCTE.getLeft(), recursiveCTE.getLeft().getTraitSet().simplify()
                .replace(outConvention)),
            convert(recursiveCTE.getRight(), recursiveCTE.getRight().getTraitSet().simplify()
                .replace(outConvention)),
            recursiveCTE.getCteName(),
            recursiveCTE.getOffset(),
            recursiveCTE.getFetch());
    }
}
