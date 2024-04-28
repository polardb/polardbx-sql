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
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RecursiveCTEAnchor;

public class DrdsRecursiveCTEAnchorConvertRule extends ConverterRule {
    public static final DrdsRecursiveCTEAnchorConvertRule SMP_INSTANCE =
        new DrdsRecursiveCTEAnchorConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsRecursiveCTEAnchorConvertRule COL_INSTANCE =
        new DrdsRecursiveCTEAnchorConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsRecursiveCTEAnchorConvertRule(Convention outConvention) {
        super(RecursiveCTEAnchor.class, Convention.NONE, outConvention, "DrdsRecursiveCTEAnchorConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final RecursiveCTEAnchor recursiveCTEAnchor = (RecursiveCTEAnchor) rel;
        return new RecursiveCTEAnchor(recursiveCTEAnchor.getCluster(),
            recursiveCTEAnchor.getTraitSet().simplify().replace(outConvention),
            recursiveCTEAnchor.getCteName(), recursiveCTEAnchor.getRowType());
    }
}
