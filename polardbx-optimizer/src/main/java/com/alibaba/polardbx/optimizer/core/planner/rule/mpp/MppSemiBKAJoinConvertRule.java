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
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

/**
 * @author dylan
 */
public class MppSemiBKAJoinConvertRule extends ConverterRule {

    public static final MppSemiBKAJoinConvertRule INSTANCE = new MppSemiBKAJoinConvertRule();

    MppSemiBKAJoinConvertRule() {
        super(SemiBKAJoin.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppSemiBKAJoinConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        final SemiBKAJoin semiBKAJoin = (SemiBKAJoin) rel;

        boolean preserveInputCollation = !semiBKAJoin.getTraitSet().simplify().getCollation().isTop();
        final RelCollation leftCollation;
        final RelCollation rightCollation;
        if (preserveInputCollation) {
            leftCollation = semiBKAJoin.getLeft().getTraitSet().getCollation();
            rightCollation = semiBKAJoin.getRight().getTraitSet().getCollation();
        } else {
            leftCollation = RelCollations.EMPTY;
            rightCollation = RelCollations.EMPTY;
        }

        RelTraitSet emptyTraitSet = rel.getCluster().getPlanner().emptyTraitSet();
        RelNode left =
            convert(semiBKAJoin.getLeft(), emptyTraitSet.replace(leftCollation).replace(MppConvention.INSTANCE));
        RelNode right =
            convert(semiBKAJoin.getRight(), emptyTraitSet.replace(rightCollation).replace(MppConvention.INSTANCE));

        SemiBKAJoin newSemiBKAJoin = semiBKAJoin.copy(
            semiBKAJoin.getTraitSet().replace(MppConvention.INSTANCE).replace(RelDistributions.ANY),
            semiBKAJoin.getCondition(),
            left,
            right,
            semiBKAJoin.getJoinType(),
            semiBKAJoin.isSemiJoinDone());

        if (semiBKAJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
            return convert(newSemiBKAJoin, newSemiBKAJoin.getTraitSet().replace(RelDistributions.SINGLETON));
        } else {
            return newSemiBKAJoin;
        }
    }
}
