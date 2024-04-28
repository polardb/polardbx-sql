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
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * @author dylan
 */
public class DrdsVirtualViewConvertRule extends ConverterRule {
    public static final DrdsVirtualViewConvertRule SMP_INSTANCE =
        new DrdsVirtualViewConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsVirtualViewConvertRule COL_INSTANCE =
        new DrdsVirtualViewConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsVirtualViewConvertRule(Convention outConvention) {
        super(VirtualView.class, Convention.NONE, outConvention, "DrdsVirtualViewConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final VirtualView virtualView = (VirtualView) rel;
        RelTraitSet relTraitSet = virtualView.getTraitSet().simplify();
        VirtualView newVirtualView = virtualView.copy(relTraitSet.replace(outConvention));
        return newVirtualView;
    }
}
