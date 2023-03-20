package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * @author yaozhili
 */
public class ForceIndexVisitor extends RelShuttleImpl {
    /**
     * Whether add FORCE INDEX PRIMARY.
     * If true, a new rel-node with FORCE INDEX PRIMARY will be generated.
     */
    private final boolean addForcePrimary;

    /**
     * Whether we should add FORCE INDEX PRIMARY for count(*) under TSO.
     * If true, this rel-node can be optimize by adding FORCE INDEX PRIMARY, (but we do not optimize it.)
     */
    private boolean canOptByForcePrimary = false;

    private final ExecutionContext ec;

    public ForceIndexVisitor(boolean addForcePrimary, ExecutionContext ec) {
        this.addForcePrimary = addForcePrimary;
        this.ec = ec;
    }

    public boolean isCanOptByForcePrimary() {
        return canOptByForcePrimary;
    }

    @Override
    public RelNode visit(TableScan scan) {
        // UGSI may have no primary key, so just disable this optimization for UGSI.
        if (scan instanceof LogicalIndexScan && ((LogicalIndexScan) scan).isUniqueGsi()) {
            return scan;
        }

        if (scan instanceof LogicalView) {
            final RelNode pushedDownNode = ((LogicalView) scan).getPushedRelNode();
            final AggVisitor aggVisitor = new AggVisitor();
            pushedDownNode.accept(aggVisitor);
            this.canOptByForcePrimary = aggVisitor.isCanOptByForcePrimary();
        }
        return scan;
    }

    private class AggVisitor extends RelShuttleImpl {
        private boolean canOptByForcePrimary;

        private AggVisitor() {
            this.canOptByForcePrimary = false;
        }

        public boolean isCanOptByForcePrimary() {
            return canOptByForcePrimary;
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RelNode input = filter.getInput();

            if (input instanceof Aggregate) {
                // I am a having clause.
                if (null == ForceIndexVisitor.this.ec || !ForceIndexVisitor.this.ec.enableForcePrimaryForFilter()) {
                    // Do not optimize for filter.
                    return filter;
                }
            }

            return super.visit(filter);
        }

        /**
         * This method may in place change the node of the Rel-Tree.
         */
        @Override
        public RelNode visit(LogicalAggregate agg) {
            RelNode input = agg.getInput();
            if (agg.getGroupCount() > 0) {
                if (null == ForceIndexVisitor.this.ec || !ForceIndexVisitor.this.ec.enableForcePrimaryForGroupBy()) {
                    // Do not optimize for group by.
                    return agg;
                }
            }

            // Only consider three simple cases:
            // Case 1: agg -> table scan.
            // Case 2: agg -> filter -> table scan.
            // Case 3: agg -> project -> (possible filter) -> table scan.
            boolean hasProject = false;
            if (input instanceof Project) {
                input = ((Project) input).getInput();
                hasProject = true;
            }
            if (input instanceof Filter) {
                if (null == ForceIndexVisitor.this.ec || !ForceIndexVisitor.this.ec.enableForcePrimaryForFilter()) {
                    // Do not optimize for filter.
                    return agg;
                }
                input = ((Filter) input).getInput();
            }
            if (input instanceof TableScan && findTargetAgg(agg, hasProject, (TableScan) input)) {
                this.canOptByForcePrimary = true;
                if (ForceIndexVisitor.this.addForcePrimary && ((TableScan) input).getIndexNode() == null) {
                    // Add "FORCE INDEX(PRIMARY)".
                    final SqlCharStringLiteral indexKind =
                        SqlLiteral.createCharString("FORCE INDEX", SqlParserPos.ZERO);
                    final SqlNodeList indexList = new SqlNodeList(
                        ImmutableList.of(SqlLiteral.createCharString("PRIMARY", SqlParserPos.ZERO)),
                        SqlParserPos.ZERO);
                    final SqlIndexHint hint = new SqlIndexHint(indexKind, null, indexList, SqlParserPos.ZERO);
                    ((TableScan) input).setIndexNode(hint);
                }
            }
            return agg;
        }

        private boolean findTargetAgg(LogicalAggregate agg, boolean hasProject, TableScan tableScan) {
            for (AggregateCall call : agg.getAggCallList()) {
                SqlKind kind = call.getAggregation().getKind();
                if (kind == SqlKind.COUNT || kind == SqlKind.SUM || SqlKind.AVG_AGG_FUNCTIONS.contains(kind)) {
                    return true;
                }

                // Only consider such case: agg -> (possible filter) -> table scan.
                if (!hasProject && SqlKind.MIN_MAX_AGG.contains(kind)) {
                    // Min/Max should have one and only one argument.
                    if (call.getArgList().size() != 1) {
                        // Unexpected.
                        return false;
                    }
                    final int arg = call.getArgList().get(0);
                    // Get the target column.
                    final String columnName = tableScan.getRowType().getFieldNames().get(arg);
                    // Get the column meta.
                    final TableMeta tableMeta = RelUtils.getTableMeta(tableScan);

                    if (RelUtils.canOptMinMax(tableMeta, columnName)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }
}
