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
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinRelType;

/**
 * @author dylan
 */
public class MppMaterializedSemiJoinConvertRule extends ConverterRule {

    public static final MppMaterializedSemiJoinConvertRule INSTANCE =
        new MppMaterializedSemiJoinConvertRule("MppMaterializedSemiJoinConvertRule");

    MppMaterializedSemiJoinConvertRule(String desc) {
        super(MaterializedSemiJoin.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, desc);
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final MaterializedSemiJoin materializedSemiJoin = (MaterializedSemiJoin) rel;

        RelNode left = materializedSemiJoin.getLeft();
        RelNode right = materializedSemiJoin.getRight();

        RelTraitSet emptyTraitSet = rel.getCluster().getPlanner().emptyTraitSet();
        left = convert(left, emptyTraitSet.replace(MppConvention.INSTANCE));
        right = convert(right, emptyTraitSet.replace(MppConvention.INSTANCE));

        //FIXME 目前MaterializedSemiJoin一定要求是单线程的, 所以inner一边可以多线程读，push给join节点，但是probe端一定要求是
        //单线程的，因为MaterializedSemiJoin 的probe一定和scan chained在一起
        if (materializedSemiJoin.getJoinType().equals(JoinRelType.RIGHT)) {
            //left is inner
            left = convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
        } else {
            //righter is inner
            right = convert(right, right.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
        }

        MaterializedSemiJoin newMaterializedSemiJoin = materializedSemiJoin.copy(
            materializedSemiJoin.getTraitSet().replace(MppConvention.INSTANCE),
            materializedSemiJoin.getCondition(),
            left, right,
            materializedSemiJoin.getJoinType(),
            materializedSemiJoin.isSemiJoinDone());

        return newMaterializedSemiJoin;
    }
}
