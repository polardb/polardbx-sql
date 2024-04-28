package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;

public class COLLogicalSortToSortRule extends LogicalSortToSortRule {
    public static final COLLogicalSortToSortRule INSTANCE = new COLLogicalSortToSortRule(false, "INSTANCE");

    public static final COLLogicalSortToSortRule TOPN = new COLLogicalSortToSortRule(true, "TOPN");

    private COLLogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(isTopNRule, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createSort(RelOptRuleCall call, LogicalSort sort) {
        RelTraitSet emptyTrait = call.getPlanner().emptyTraitSet();
        if (isTopNRule) {
            // convert sort to topN
            if (sort.withLimit() && sort.withOrderBy()) {
                RelNode input = convert(sort.getInput(),
                    sort.getInput().getTraitSet().replace(outConvention));
                RexNode fetch = RuleUtils.getPartialFetch(sort);

                TopN partialTopN = TopN.create(
                    emptyTrait.replace(outConvention).replace(sort.getCollation()),
                    input, sort.getCollation(), null, fetch);

                RelNode ensureNode = convert(partialTopN,
                    partialTopN.getTraitSet().replace(RelDistributions.SINGLETON));

                call.transformTo(TopN.create(
                    emptyTrait
                        .replace(outConvention)
                        .replace(RelDistributions.SINGLETON)
                        .replace(sort.getCollation()),
                    ensureNode, sort.getCollation(), sort.offset, sort.fetch));
                return;
            } else {
                return;
            }
        }

        RelNode input =
            convert(sort.getInput(), emptyTrait.replace(outConvention));
        final boolean hasOrdering = sort.withOrderBy();
        final boolean hasLimit = sort.withLimit();

        if (hasOrdering && !hasLimit) {
            call.transformTo(convert(input,
                emptyTrait
                    .replace(outConvention)
                    .replace(RelDistributions.SINGLETON)
                    .replace(sort.getCollation())));
            return;
        }

        if (hasOrdering) {
            Limit limit = Limit.create(
                emptyTrait.replace(outConvention).replace(RelDistributions.SINGLETON),
                convert(input,
                    input.getTraitSet()
                        .replace(outConvention)
                        .replace(RelDistributions.SINGLETON)
                        .replace(sort.getCollation())), sort.offset, sort.fetch);
            call.transformTo(limit);
            return;
        }

        // now sort has no order
        // one phase limit
        Limit limit = Limit.create(
            emptyTrait.replace(outConvention).replace(RelDistributions.SINGLETON),
            convert(input,
                input.getTraitSet()
                    .replace(outConvention)
                    .replace(RelDistributions.SINGLETON)), sort.offset, sort.fetch);
        call.transformTo(limit);

        // tow phase limit
        Limit localLimit = Limit.create(
            emptyTrait.replace(outConvention),
            convert(input, emptyTrait.replace(outConvention)),
            sort.offset,
            sort.fetch);
        Limit globalLimit = Limit.create(
            emptyTrait.replace(outConvention).replace(RelDistributions.SINGLETON),
            convert(localLimit, emptyTrait.replace(outConvention).replace(RelDistributions.SINGLETON)),
            sort.offset,
            sort.fetch);
        call.transformTo(globalLimit);
    }
}

