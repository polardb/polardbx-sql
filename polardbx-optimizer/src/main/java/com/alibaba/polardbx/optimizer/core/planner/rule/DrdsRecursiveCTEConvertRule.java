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
import com.google.common.base.Predicates;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.core.RelFactories;

public class DrdsRecursiveCTEConvertRule extends ConverterRule {
    public static final DrdsRecursiveCTEConvertRule INSTANCE = new DrdsRecursiveCTEConvertRule();

    DrdsRecursiveCTEConvertRule() {
        super(RecursiveCTE.class, Convention.NONE, DrdsConvention.INSTANCE, "DrdsRecursiveCTEConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final RecursiveCTE recursiveCTE = (RecursiveCTE) rel;
        rel.getCluster().getPlanner().getContext().unwrap(PlannerContext.class).setHasRecursiveCte(true);
        return new RecursiveCTE(recursiveCTE.getCluster(),
            recursiveCTE.getTraitSet().simplify().replace(DrdsConvention.INSTANCE),
            convert(recursiveCTE.getLeft(), recursiveCTE.getLeft().getTraitSet().simplify()
                .replace(DrdsConvention.INSTANCE)),
            convert(recursiveCTE.getRight(), recursiveCTE.getRight().getTraitSet().simplify()
                .replace(DrdsConvention.INSTANCE)),
            recursiveCTE.getCteName(),
            recursiveCTE.getOffset(),
            recursiveCTE.getFetch());
    }
}
