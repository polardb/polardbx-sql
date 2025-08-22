package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.google.common.base.Predicate;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.commons.collections.CollectionUtils;

import java.util.Optional;

public class PagingForceRemoveRule extends RelOptRule {

    public static final Predicate<TableScan> PAGING = new PredicateImpl<TableScan>() {
        @Override
        public boolean test(TableScan tableScan) {
            return tableScan != null &&
                CollectionUtils.isNotEmpty(IndexUtil.getPagingForceIndex(tableScan.getIndexNode()));
        }
    };

    public static final Predicate<TableScan> INDEX = new PredicateImpl<TableScan>() {
        @Override
        public boolean test(TableScan tableScan) {
            return tableScan != null &&
                CollectionUtils.isNotEmpty(IndexUtil.getForceIndex(tableScan.getIndexNode()));
        }
    };

    public static final PagingForceRemoveRule INSTANCE = new PagingForceRemoveRule(
        operand(TableScan.class, null, PAGING, none()),
        RelFactories.LOGICAL_BUILDER, "PagingForceRemoveRule"
    );

    public PagingForceRemoveRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        Optional<String> indexName = IndexUtil.getPagingForceIndex(scan.getIndexNode()).stream().findFirst();
        if (!indexName.isPresent()) {
            return;
        }
        SqlNode sqlNode = ForceIndexUtil.genForceSqlNode(indexName.get());
        LogicalTableScan newScan =
            LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                sqlNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
        call.transformTo(newScan);
    }
}