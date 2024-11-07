package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.planner.rule.util.ForceIndexUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

public class AutoForceIndexRule extends RelOptRule {

    String indexName;

    public AutoForceIndexRule(String indexName) {
        super(operand(TableScan.class, RelOptRule.none()),
            "AutoForceIndexRule");
        this.indexName = indexName;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        return !ForceIndexUtil.hasIndexHint(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        TableScan scan = call.rel(0);
        SqlNode sqlNode = new SqlNodeList(ImmutableList.of(
            new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO), null,
                new SqlNodeList(ImmutableList.of(SqlLiteral.createCharString(
                    SqlIdentifier.surroundWithBacktick(indexName), SqlParserPos.ZERO)),
                    SqlParserPos.ZERO), SqlParserPos.ZERO)), SqlParserPos.ZERO);
        LogicalTableScan newScan =
            LogicalTableScan.create(scan.getCluster(), scan.getTable(), scan.getHints(),
                sqlNode, scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
        call.transformTo(newScan);
    }
}
