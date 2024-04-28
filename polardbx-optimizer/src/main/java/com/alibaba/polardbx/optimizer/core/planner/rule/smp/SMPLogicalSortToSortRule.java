package com.alibaba.polardbx.optimizer.core.planner.rule.smp;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

public class SMPLogicalSortToSortRule extends LogicalSortToSortRule {
    public static final SMPLogicalSortToSortRule INSTANCE = new SMPLogicalSortToSortRule(false, "INSTANCE");

    public static final SMPLogicalSortToSortRule TOPN = new SMPLogicalSortToSortRule(true, "TOPN");

    private SMPLogicalSortToSortRule(boolean isTopNRule, String desc) {
        super(isTopNRule, "SMP_" + desc);
    }

    @Override
    protected void createSort(RelOptRuleCall call, LogicalSort sort) {
        if (isTopNRule) {
            if (sort.withLimit() && sort.withOrderBy()) {
                RelNode input =
                    convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
                call.transformTo(TopN.create(
                    sort.getTraitSet().replace(DrdsConvention.INSTANCE),
                    input, sort.getCollation(), sort.offset, sort.fetch));
            }
        } else {
            RelNode input =
                convert(sort.getInput(), sort.getInput().getTraitSet().replace(DrdsConvention.INSTANCE));
            call.transformTo(convertLogicalSort(sort, input));
        }
    }

    public static RelNode convertLogicalSort(LogicalSort sort, RelNode input) {
        final boolean hasOrdering = sort.withOrderBy();
        final boolean hasLimit = sort.withLimit();

        if (hasOrdering && !hasLimit) {
            RelDistribution relDistribution = sort.getTraitSet().getDistribution();
            return convert(input,
                input.getTraitSet()
                    .replace(DrdsConvention.INSTANCE)
                    .replace(relDistribution)
                    .replace(sort.getCollation()));
        } else if (hasOrdering && hasLimit) {
            return Limit.create(
                sort.getTraitSet().replace(DrdsConvention.INSTANCE),
                convert(input,
                    input.getTraitSet()
                        .replace(DrdsConvention.INSTANCE)
                        .replace(RelDistributions.SINGLETON)
                        .replace(sort.getCollation())), sort.offset, sort.fetch);
        } else { // !hasOrdering
            return Limit.create(
                sort.getTraitSet().replace(DrdsConvention.INSTANCE).replace(RelDistributions.SINGLETON),
                input, sort.offset, sort.fetch);
        }
    }
}

