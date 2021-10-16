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

package com.alibaba.polardbx.optimizer.core;

import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppExpandConversionRule;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * @author dylan
 */
public class MppConvention extends Convention.Impl {
    public static final MppConvention INSTANCE = new MppConvention();

    private MppConvention() {
        super("MPP", RelNode.class);
    }

    @Override
    public boolean useAbstractConvertersForConversion(RelTraitSet fromTraits,
                                                      RelTraitSet toTraits) {
        return true;
    }

    @Override
    public RelNode enforce(final RelNode input, final RelTraitSet required) {
        if (input.getConvention() == DrdsConvention.INSTANCE) {
            // left Convert Rule to deal with
            return null;
        } else if (input.getConvention() == MppConvention.INSTANCE) {
            return MppExpandConversionRule.enforce(input, required);
        } else {
            throw new AssertionError("Unable to convert input to " + INSTANCE + ", input = " + input);
        }
    }
}

