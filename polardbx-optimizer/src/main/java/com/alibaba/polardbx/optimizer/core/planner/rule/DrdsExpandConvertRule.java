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
import org.apache.calcite.rel.logical.LogicalExpand;

public class DrdsExpandConvertRule extends ConverterRule {

    public static final DrdsExpandConvertRule SMP_INSTANCE = new DrdsExpandConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsExpandConvertRule COL_INSTANCE = new DrdsExpandConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsExpandConvertRule(Convention outConvention) {
        super(LogicalExpand.class, Convention.NONE, outConvention, "DrdsExpandConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalExpand expand = (LogicalExpand) rel;
        return expand.copy(expand.getTraitSet().simplify().replace(outConvention), convert(expand.getInput(),
            expand.getInput().getTraitSet().simplify()
                .replace(outConvention)), expand.getProjects(), expand.getRowType());
    }
}
