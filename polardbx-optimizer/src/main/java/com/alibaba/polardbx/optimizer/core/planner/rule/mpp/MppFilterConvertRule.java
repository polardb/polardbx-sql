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

package com.alibaba.polardbx.optimizer.core.planner.rule.mpp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;

/**
 * @author dylan
 */
public class MppFilterConvertRule extends ConverterRule {
    public static final MppFilterConvertRule INSTANCE = new MppFilterConvertRule();

    MppFilterConvertRule() {
        super(PhysicalFilter.class, RelOptUtil.FILTER_PREDICATE,
            DrdsConvention.INSTANCE, MppConvention.INSTANCE, RelFactories.LOGICAL_BUILDER,
            "MppFilterConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final PhysicalFilter filter = (PhysicalFilter) rel;
        return filter.copy(filter.getTraitSet().replace(MppConvention.INSTANCE), convert(filter.getInput(),
            filter.getInput().getTraitSet()
                .replace(MppConvention.INSTANCE)), filter.getCondition());
    }
}

