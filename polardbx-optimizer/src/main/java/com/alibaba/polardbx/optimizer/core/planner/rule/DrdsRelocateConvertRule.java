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
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * @author chenmo.cm
 */
public class DrdsRelocateConvertRule extends ConverterRule {
    public static final DrdsRelocateConvertRule INSTANCE = new DrdsRelocateConvertRule();

    DrdsRelocateConvertRule() {
        super(LogicalRelocate.class, Convention.NONE, DrdsConvention.INSTANCE, "DrdsRelocateConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return DrdsConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalRelocate logicalRelocate = (LogicalRelocate) rel;
        RelTraitSet relTraitSet = logicalRelocate.getTraitSet().simplify();
        return logicalRelocate.copy(relTraitSet.replace(DrdsConvention.INSTANCE),
            convertList(logicalRelocate.getInputs(), DrdsConvention.INSTANCE));
    }

}
