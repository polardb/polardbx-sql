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
import org.apache.calcite.rel.logical.LogicalCorrelate;

public class DrdsCorrelateConvertRule extends ConverterRule {
    public static final DrdsCorrelateConvertRule SMP_INSTANCE = new DrdsCorrelateConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsCorrelateConvertRule COL_INSTANCE =
        new DrdsCorrelateConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsCorrelateConvertRule(Convention outConvention) {
        super(LogicalCorrelate.class, Convention.NONE, outConvention, "DrdsCorrelateConvertRule");
        this.outConvention = outConvention;
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final LogicalCorrelate correlate = (LogicalCorrelate) rel;
        return correlate
            .copy(correlate.getTraitSet().simplify().replace(outConvention), convert(correlate.getLeft(),
                    correlate.getLeft().getTraitSet().simplify()
                        .replace(outConvention)), convert(correlate.getRight(),
                    correlate.getRight().getTraitSet().simplify()
                        .replace(outConvention)), correlate.getCorrelationId(), correlate.getRequiredColumns(),
                correlate.getJoinType());
    }
}
