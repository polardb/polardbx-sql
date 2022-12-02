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

package com.alibaba.polardbx.optimizer.sharding;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.sharding.label.JoinCondition;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorType;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class RelToLabelConverter extends RelShuttleImpl {

    private final Blackboard blackboard;

    public RelToLabelConverter(ExtractorType extractorType, ExtractorContext context) {
        this.blackboard = new Blackboard(extractorType, context);
    }

    public RelToLabelConverter(RelToLabelConverter that) {
        this.blackboard = that.blackboard;
    }

    public Label getRoot() {
        return blackboard.getRoot();
    }

    public SubqueryWrapperLabel relWithSubquery(RelNode relNode, Set<CorrelationId> newCorIds) {
        if (relNode instanceof LogicalFilter) {
            return blackboard.filterSubqueryWrapper((LogicalFilter) relNode, newCorIds);
        } else {
            throw new UnsupportedOperationException("Do not support subquery in " + relNode.getClass().getName());
        }
    }

    public void subQuery(RexSubQuery subQuery, List<RexNode> inferred) {
        blackboard.subquery(subQuery, inferred);
    }

    private class Blackboard {

        private Label root;
        private final ExtractorType extractorType;
        private final LabelBuilder labelBuilder;
        private final ExtractorContext context;

        private Blackboard(ExtractorType extractorType, ExtractorContext context) {
            this.extractorType = extractorType;
            this.labelBuilder = context.labelBuilder();
            this.context = context;
        }

        public Label getRoot() {
            return root;
        }

        public void values(LogicalValues values) {
            root(labelBuilder.values(values));
        }

        public void values(LogicalDynamicValues values) {
            root(labelBuilder.values(values));
        }

        public void values(DynamicValues values) {
            root(labelBuilder.values(values));
        }

        public void tableScan(TableScan scan) {
            root(labelBuilder.tableScan(scan));
        }

        public void aggregate(Aggregate aggregate) {
            root(labelBuilder.aggregate(aggregate));
        }

        public void groupJoin(HashGroupJoin groupJoin) {
            root(labelBuilder.aggregate(groupJoin));
        }

        public void project(Project project) {
            root(labelBuilder.project(project));
        }

        public void join(Join join) {
            RexNode joinCondition = join.getCondition();

            assert joinCondition != null;

            final RexExtractorContext rexExtractorContext = context.createRexExtractor(RelToLabelConverter.this, join);
            joinCondition = joinCondition.accept(rexExtractorContext.getSubqueryExtractor());

            // Classify predicates from ON clause
            final JoinCondition onCondition = JoinCondition
                .create(join, RelOptUtil.conjunctions(joinCondition), context);

            root(labelBuilder.join(join, onCondition));
        }

        public void correlate(Correlate correlate) {
            root(labelBuilder.correlate((LogicalCorrelate) correlate));
        }

        public void filter(Filter filter) {
            final RexExtractorContext rexContext = context.createRexExtractor(RelToLabelConverter.this, filter);
            final SubqueryRexExtractor rexVisitor = rexContext.getSubqueryExtractor();
            final RexNode predicate = filter.getCondition().accept(rexVisitor);

            if (null != predicate && (predicate.isAlwaysTrue() || predicate.isAlwaysFalse()
                || predicate instanceof RexLiteral)) {
                // Skip literal condition
                return;
            }

            root(labelBuilder.filter(filter, predicate, rexVisitor.withCorrelateSubquery()));
        }

        public void union(Union union) {
            root(labelBuilder.union(union));
        }

        public SubqueryWrapperLabel filterSubqueryWrapper(LogicalFilter filter, Set<CorrelationId> newCorIds) {
            return root(labelBuilder.filterSubqueryWrapper(filter, newCorIds));
        }

        public void subquery(RexSubQuery subQuery, List<RexNode> inferred) {
            labelBuilder.subquery(subQuery, inferred);
            // Set root to SubqueryWrapper
            root(labelBuilder.top());
        }

        private <T extends Label> T root(Label newRoot) {
            if (null != newRoot) {
                this.root = newRoot;
            }

            return (T) this.root;
        }

        public void window(Window window) {
            root(labelBuilder.window(window));
        }

        public void expand(LogicalExpand logicalExpand) {
            root(labelBuilder.expand(logicalExpand));
        }
    }

    @Override
    public RelNode visit(LogicalValues values) {
        blackboard.values(values);
        return values;
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            (((LogicalView) scan).getSemiJoinRemovedRelNode()).accept(this);
        } else {
            blackboard.tableScan(scan);
        }
        return scan;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        RelNode result = super.visit(aggregate);

        blackboard.aggregate(aggregate);

        return result;
    }

    public RelNode visit(Aggregate aggregate) {
        RelNode result = super.visit(aggregate);

        blackboard.aggregate(aggregate);

        return result;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        final RelNode result = super.visit(project);

        blackboard.project(project);

        return result;
    }

    public RelNode visit(PhysicalProject project) {
        final RelNode result = super.visit(project);

        blackboard.project(project);

        return result;
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        super.visit(join);

        blackboard.join(join);

        return join;
    }

    public RelNode visit(HashGroupJoin join) {
        visit(join.copyAsJoin(join.getTraitSet(), join.getCondition()));
        blackboard.groupJoin(join);
        return join;
    }

    public RelNode visit(Join join) {
        if (join instanceof HashGroupJoin) {
            return visit((HashGroupJoin) join);
        }
        super.visit(join);

        blackboard.join(join);

        return join;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        final RelNode result = super.visit(filter);

        blackboard.filter(filter);

        return result;
    }

    public RelNode visit(PhysicalFilter filter) {
        final RelNode result = super.visit(filter);

        blackboard.filter(filter);

        return result;
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        super.visit(union);

        blackboard.union(union);

        return union;
    }

    @Override
    public RelNode visit(LogicalTableLookup tableLookup) {
        this.visit(tableLookup.getProject());

        return tableLookup;
    }

    public RelNode visit(TableLookup tableLookup) {
        this.visit(tableLookup.getProject());

        return tableLookup;
    }

    public RelNode visit(LogicalWindow logicalWindow) {
        super.visit(logicalWindow);

        blackboard.window(logicalWindow);

        return logicalWindow;
    }

    public RelNode visit(Window window) {
        super.visit(window);

        blackboard.window(window);

        return window;
    }

    public RelNode visit(LogicalExpand logicalExpand) {
        super.visit(logicalExpand);

        blackboard.expand(logicalExpand);

        return logicalExpand;
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        super.visit(correlate);

        blackboard.correlate(correlate);

        return correlate;
    }

    public RelNode visit(LogicalOutFile outFile) {
        super.visit(outFile);
        return outFile;
    }

    public RelNode visit(LogicalDynamicValues dynamicValues) {
        blackboard.values(dynamicValues);
        return dynamicValues;
    }

    public RelNode visit(DynamicValues dynamicValues) {
        blackboard.values(dynamicValues);
        return dynamicValues;
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof HepRelVertex) {
            RelNode relNode = ((HepRelVertex) other).getCurrentRel();
            return relNode.accept(this);
        }
        if (other instanceof RelSubset) {
            RelNode relNode = Util.first(((RelSubset) other).getBest(), ((RelSubset) other).getOriginal());
            return relNode.accept(this);
        }

        if (other instanceof LogicalJoin) {
            return visit((LogicalJoin) other);
        }
        if (other instanceof Join) {
            return visit((Join) other);
        }
        if (other instanceof LogicalAggregate) {
            return visit((LogicalAggregate) other);
        }
        if (other instanceof Aggregate) {
            return visit((Aggregate) other);
        }
        if (other instanceof LogicalProject) {
            return visit((LogicalProject) other);
        }
        if (other instanceof PhysicalProject) {
            return visit((PhysicalProject) other);
        }

        if (other instanceof TableScan) {
            return visit((TableScan) other);
        }
        if (other instanceof TableFunctionScan) {
            return visit((TableFunctionScan) other);
        }
        if (other instanceof LogicalValues) {
            return visit((LogicalValues) other);
        }
        if (other instanceof LogicalFilter) {
            return visit((LogicalFilter) other);
        }
        if (other instanceof PhysicalFilter) {
            return visit((PhysicalFilter) other);
        }
        if (other instanceof LogicalCorrelate) {
            return visit((LogicalCorrelate) other);
        }
        if (other instanceof LogicalUnion) {
            return visit((LogicalUnion) other);
        }
        if (other instanceof LogicalIntersect) {
            return visit((LogicalIntersect) other);
        }
        if (other instanceof LogicalMinus) {
            return visit((LogicalMinus) other);
        }
        if (other instanceof LogicalMatch) {
            return visit((LogicalMatch) other);
        }
        if (other instanceof LogicalSort) {
            return visit((LogicalSort) other);
        }
        if (other instanceof LogicalExchange) {
            return visit((LogicalExchange) other);
        }
        if (other instanceof Sort) {
            return super.visit(other);
        }
        if (other instanceof Exchange) {
            return visitChildren(other);
        }
        if (other instanceof DirectTableOperation) {
            return visitChildren(other);
        }
        if (other instanceof TableModify) {
            return visitChildren(other);
        }
        if (other instanceof LogicalTableLookup) {
            return visit((LogicalTableLookup) other);
        }
        if (other instanceof TableLookup) {
            return visit((TableLookup) other);
        }
        if (other instanceof LogicalWindow) {
            return visit((LogicalWindow) other);
        }
        if (other instanceof Window) {
            return visit((Window) other);
        }
        if (other instanceof LogicalDynamicValues) {
            return visit((LogicalDynamicValues) other);
        }

        if (other instanceof DynamicValues) {
            return visit((DynamicValues) other);
        }

        if (other instanceof LogicalExchange) {
            return visit((LogicalExchange) other);
        }
        if (other instanceof LogicalExpand) {
            return visit((LogicalExpand) other);
        }
        if (other instanceof Correlate) {
            return visit((LogicalCorrelate) other);
        }

        if (other instanceof RuntimeFilterBuilder) {
            return visitChildren(other);
        }

        if (other instanceof BroadcastTableModify) {
            return other;
        }

        if (other instanceof LogicalExpand) {
            return visit((LogicalExpand) other);
        }

        if (other instanceof VirtualView) {
            return other;
        }

        if (other instanceof LogicalOutFile) {
            return visit((LogicalOutFile) other);
        }

        throw new NotSupportException("Unhandled rel node " + other.getClass().getName());
    }
}
