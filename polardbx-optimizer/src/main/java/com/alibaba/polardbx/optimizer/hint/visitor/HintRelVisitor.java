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

package com.alibaba.polardbx.optimizer.hint.visitor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MergeSort;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.mpp.MppExchange;
import com.alibaba.polardbx.optimizer.hint.HintPlanner;
import com.alibaba.polardbx.optimizer.hint.operator.HintPushdownOperator;
import com.alibaba.polardbx.optimizer.hint.util.HintConverter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class HintRelVisitor extends RelShuttleImpl {

    protected List<LogicalView> logicalViews = new LinkedList<>();
    protected LogicalJoin logicalJoin = null;
    protected Map<RelNode, SqlNodeList> relNodeHints = new HashMap<>();

    public List<LogicalView> getLogicalViews() {
        return logicalViews;
    }

    public LogicalJoin getLogicalJoin() {
        return logicalJoin;
    }

    public Map<RelNode, SqlNodeList> getRelNodeHints() {
        return relNodeHints;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            if (null != scan.getHints()) {
                relNodeHints.put(scan, scan.getHints());
            }
            logicalViews.add((LogicalView) scan);
        }
        return scan;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        if (null == this.logicalJoin) {
            this.logicalJoin = join;
        }

        if (null != join.getHints()) {
            relNodeHints.put(join, join.getHints());
        }

        return super.visit(join);
    }

    @Override
    public RelNode visit(LogicalValues values) {
        // TODO: support logical values
        return super.visit(values);
    }

    /**
     * push plan tree into logicalView from top to bottom
     */
    public static class PushdownHandlerVisitor extends RelShuttleImpl {

        private final Map<RelNode, RelNode> rootRelMap;
        private final Map<Integer, ParameterContext> param;
        private final ExecutionContext ec;

        public PushdownHandlerVisitor(Map<RelNode, RelNode> rootRelMap, Map<Integer, ParameterContext> param,
                                      ExecutionContext ec) {
            this.rootRelMap = rootRelMap;
            this.param = param;
            this.ec = ec;
        }

        private RelNode pushdown(RelNode root) {
            RelNode relNode = rootRelMap.get(root);

            if (relNode == null) {
                return super.visit(root);
            }

            SqlNodeList hints = null;
            RelOptTable table = null;
            final List<String> tableNames = new LinkedList<>();
            if (relNode instanceof TableScan) {
                table = relNode.getTable();
                hints = ((TableScan) relNode).getHints();
                if (relNode instanceof LogicalView) {
                    tableNames.addAll(((LogicalView) relNode).getTableNames());
                } else {
                    tableNames.add(Util.last(table.getQualifiedName()));
                }
            } else if (relNode instanceof Join) {
                HintTableFinder tableScanFinder = new HintTableFinder();
                RelOptUtil.go(tableScanFinder, relNode);
                table = tableScanFinder.tableScan.getTable();
                hints = ((Join) relNode).getHints();
                tableNames.addAll(tableScanFinder.tableNames);
            }

            final RelNode expanded = root.accept(new HintExpandLogicalViewVisitor());

            final LogicalView logicalView = new LogicalView(expanded, table, hints);
            logicalView.setSqlTemplate(logicalView.getNativeSqlNode());
            logicalView.setTableName(tableNames);

            List<HintPushdownOperator> pushdowns = new LinkedList<>();
            HintConverter.convertPushdown(hints, pushdowns, ec);

            if (pushdowns.size() > 0) {
                pushdowns.get(0).handle(logicalView, param, ec);
            } else {
                HintPlanner.rebuildComparative(logicalView, param, ec);
            }

            logicalView.setFinishShard(true);

            if (!logicalView.isSingleGroup()) {
                if (expanded instanceof Sort) {
                    Sort sort = (Sort) expanded;
                    return MergeSort.create(logicalView, sort.getCollation(), sort.offset, sort.fetch);
                } else if (expanded instanceof MppExchange) {
                    return MppExchange.create(relNode, ((MppExchange) expanded).getCollation(),
                        ((MppExchange) expanded).getDistribution());
                } else {
                    return Gather.create(logicalView);
                }
            }

            return logicalView;
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            return pushdown(aggregate);
        }

        @Override
        public RelNode visit(LogicalMatch match) {
            return pushdown(match);
        }

        @Override
        public RelNode visit(TableScan scan) {
            return pushdown(scan);
        }

        @Override
        public RelNode visit(TableFunctionScan scan) {
            return pushdown(scan);
        }

        @Override
        public RelNode visit(LogicalValues values) {
            return pushdown(values);
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            return pushdown(filter);
        }

        @Override
        public RelNode visit(LogicalProject project) {
            return pushdown(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            return pushdown(join);
        }

        @Override
        public RelNode visit(LogicalCorrelate correlate) {
            return pushdown(correlate);
        }

        @Override
        public RelNode visit(LogicalUnion union) {
            return pushdown(union);
        }

        @Override
        public RelNode visit(LogicalIntersect intersect) {
            return pushdown(intersect);
        }

        @Override
        public RelNode visit(LogicalMinus minus) {
            return pushdown(minus);
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            return pushdown(sort);
        }

        @Override
        public RelNode visit(LogicalExchange exchange) {
            return pushdown(exchange);
        }

        @Override
        public RelNode visit(LogicalTableLookup tableLookup) {
            final RelNode visited = pushdown(tableLookup.getProject());

            if (visited == tableLookup.getProject()) {
                return tableLookup;
            } else {
                final Project project = (Project) visited;
                final Join join = (Join) visited.getInput(0);
                return tableLookup.copy(
                    join.getJoinType(),
                    join.getCondition(),
                    project.getProjects(),
                    project.getRowType(),
                    join.getLeft(),
                    join.getRight(),
                    tableLookup.isRelPushedToPrimary());
            }
        }

        @Override
        public RelNode visit(RelNode other) {
            return pushdown(other);
        }
    }

    public static class HintExpandLogicalViewVisitor extends RelShuttleImpl {

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Gather && other.getInput(0) instanceof LogicalView) {
                return visit(other.getInput(0));
            }

            if (other instanceof MergeSort && other.getInput(0) instanceof LogicalView) {
                return visit(other.getInput(0));
            }

            if (other instanceof LogicalView) {
                final LogicalView lv = (LogicalView) other;
                return lv.getPushedRelNode();
            }
            return super.visit(other);
        }
    }

    public static class HintRexVisitor extends RexVisitorImpl<Void> {

        private final RelShuttle relVisitor;

        protected HintRexVisitor(RelShuttle relVisitor) {
            super(true);
            this.relVisitor = relVisitor;
        }

        @Override
        public Void visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(relVisitor);
            return super.visitSubQuery(subQuery);
        }
    }

    public static class HintTableFinder extends RelVisitor {

        public TableScan tableScan;
        public LogicalInsert tableModify;
        public LogicalModifyView tableModifyView;
        public BaseDdlOperation ddlOperation;
        public final List<String> tableNames = new LinkedList<>();

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof TableScan) {
                if (null == tableScan) {
                    this.tableScan = (TableScan) node;
                }
                if (node instanceof LogicalView) {
                    tableNames.addAll(((LogicalView) node).getTableNames());
                } else {
                    tableNames.add(Util.last(tableScan.getTable().getQualifiedName()));
                }
                return;
            } else if (node instanceof LogicalInsert && null == tableModify) {
                this.tableModify = (LogicalInsert) node;
                return;
            } else if (node instanceof LogicalModifyView && null == tableModifyView) {
                this.tableModifyView = (LogicalModifyView) node;
                return;
            } else if (node instanceof BaseDdlOperation && null == ddlOperation) {
                this.ddlOperation = (BaseDdlOperation) node;
                return;
            }
            super.visit(node, ordinal, parent);
        }

        public RelOptTable getTable() {
            if (null != tableScan) {
                return tableScan.getTable();
            } else if (null != tableModify) {
                return tableModify.getTable();
            } else if (null != tableModifyView) {
                return tableModifyView.getTable();
            }

            return null;
        }

        public boolean noTable() {
            if (ddlOperation != null) {
                return false;
            } else {
                return getTable() == null;
            }
        }
    }
}
