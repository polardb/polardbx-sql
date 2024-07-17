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
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexNode;

/**
 * @author dylan
 */
public class MppTopNConvertRule extends ConverterRule {

    public static final MppTopNConvertRule INSTANCE = new MppTopNConvertRule();

    private MppTopNConvertRule() {
        super(TopN.class, DrdsConvention.INSTANCE, MppConvention.INSTANCE, "MppTopNConvertRule");
    }

    @Override
    public Convention getOutConvention() {
        return MppConvention.INSTANCE;
    }

    @Override
    public RelNode convert(RelNode rel) {
        TopN topN = (TopN) rel;
        RelNode topNInput = topN.getInput();
        RelTraitSet emptyTraitSet = topN.getCluster().getPlanner().emptyTraitSet();
        RelNode input = convert(topNInput, emptyTraitSet.replace(MppConvention.INSTANCE));
        RexNode fetch = RuleUtils.getPartialFetch(topN);

        if (topN.getTraitSet().contains(RelDistributions.SINGLETON)
            && topNInput.getTraitSet().contains(RelDistributions.SINGLETON)) {
            return topN.copy(topN.getTraitSet().replace(MppConvention.INSTANCE),
                input, topN.getCollation(), topN.offset, topN.fetch);
        }

        TopN partialTopN = topN.copy(
            topNInput.getTraitSet().replace(MppConvention.INSTANCE).replace(topN.getCollation()),
            input, topN.getCollation(), null, fetch);

        RelNode ensureNode = convert(partialTopN,
            partialTopN.getTraitSet().replace(RelDistributions.SINGLETON).replace(partialTopN.getCollation()));

        TopN globalTopN = topN.copy(
            topN.getTraitSet().replace(MppConvention.INSTANCE).replace(RelDistributions.SINGLETON),
            ensureNode, topN.getCollation(), topN.offset, topN.fetch);

        return globalTopN;
    }
}
