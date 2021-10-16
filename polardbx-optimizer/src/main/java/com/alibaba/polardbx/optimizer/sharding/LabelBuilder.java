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

import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.sharding.label.AggregateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.FilterSubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.JoinCondition;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.LabelType;
import com.alibaba.polardbx.optimizer.sharding.label.ProjectLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.label.UnionLabel;
import com.alibaba.polardbx.optimizer.sharding.label.ValuesLabel;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

/**
 * @author chenmo.cm
 */
public class LabelBuilder {

    private final Deque<Label> inputStack = new ArrayDeque<>();
    private final ExtractorContext context;

    public static LabelBuilder create(ExtractorContext context) {
        return new LabelBuilder(context);
    }

    private LabelBuilder(ExtractorContext context) {
        this.context = context;
    }

    public Label build() {
        return inputStack.pop();
    }

    public Label values(LogicalValues values) {
        final ValuesLabel valuesLabel = ValuesLabel.create(values);
        inputStack.push(valuesLabel);
        return valuesLabel;
    }

    public Label values(LogicalDynamicValues values) {
        final ValuesLabel valuesLabel = ValuesLabel.create(values);
        inputStack.push(valuesLabel);
        return valuesLabel;
    }

    public Label tableScan(TableScan scan) {
        final TableScanLabel tableScanLabel = TableScanLabel.create(scan);
        inputStack.push(tableScanLabel);
        return tableScanLabel;
    }

    public Label aggregate(Aggregate aggregate) {
        Preconditions.checkState(!inputStack.isEmpty());

        final boolean withAggCall = LabelUtil.withAggCall(aggregate);

        final Label input = inputStack.peek();

        if (withAggCall && input.withExpressionColumn()) {
            // build snapshot of current label for following predicate inference
            final AggregateLabel aggLabel = AggregateLabel.create(aggregate, inputStack.pop(), context);

            inputStack.push(aggLabel);
            return aggLabel;
        }

        return input.aggregate(aggregate, context);
    }

    public Label aggregate(HashGroupJoin groupJoin) {
        Preconditions.checkState(!inputStack.isEmpty());

        final boolean withAggCall = !GeneralUtil.isEmpty(groupJoin.getAggCallList());

        final Label input = inputStack.peek();

        if (withAggCall && input.withExpressionColumn()) {
            // build snapshot of current label for following predicate inference
            final AggregateLabel aggLabel = AggregateLabel.create(groupJoin, inputStack.pop(), context);

            inputStack.push(aggLabel);
            return aggLabel;
        }

        return input.aggregate(groupJoin, context);
    }

    public Label project(Project project) {
        Preconditions.checkState(!inputStack.isEmpty());

        final boolean isNotPermutation = LabelUtil.isNotPermutation(project);

        final Label input = inputStack.peek();

        if (isNotPermutation && input.withExpressionColumn()) {
            // build snapshot of current label for following predicate inference
            final ProjectLabel projectLabel = ProjectLabel.create(project, inputStack.pop(), context);

            inputStack.push(projectLabel);
            return projectLabel;
        }

        return input.project(project, context);
    }

    public Label join(Join join, JoinCondition joinCondition) {
        Preconditions.checkPositionIndex(1, inputStack.size());

        final Label right = inputStack.pop();
        final Label left = inputStack.pop();
        final JoinLabel joinLabel = JoinLabel.create(join, joinCondition, left, right, context);
        inputStack.push(joinLabel);

        return joinLabel;
    }

    public Label filter(Filter filter, RexNode predicate, boolean withCorrelateSubquery) {
        Preconditions.checkState(!inputStack.isEmpty());

        final Label input = inputStack.peek();

        return input.filter(predicate, filter, context);
    }

    public Label correlate(LogicalCorrelate correlate) {
        Preconditions.checkState(!inputStack.isEmpty());

        // Generate inferred correlate equality
        final Label right = inputStack.pop();
        final Label left = inputStack.pop();
        final Label correlateLabel = CorrelateLabel.create(correlate, left, right, context);
        inputStack.push(correlateLabel);
        return correlateLabel;
    }

    public Label union(Union union) {
        final int inputCount = union.getInputs().size();
        final int lastInputRef = inputCount - 1;

        Preconditions.checkPositionIndex(lastInputRef, inputStack.size());

        final Label[] inputs = new Label[inputCount];
        IntStream.range(0, inputCount).forEach(i -> inputs[lastInputRef - i] = inputStack.pop());

        final UnionLabel unionLabel = UnionLabel.create(union, ImmutableList.copyOf(inputs));
        inputStack.push(unionLabel);
        return unionLabel;
    }

    public SubqueryLabel subquery(RexSubQuery subQuery, List<RexNode> inferredCorrelateCondition) {
        final Label child = inputStack.pop();
        final Label parent = inputStack.peek();

        assert parent != null;
        if (parent.getType().isSubqueryWrapper()) {
            ((SubqueryWrapperLabel) parent).addSubQuery(subQuery,
                SubqueryLabel.create(child, subQuery, inferredCorrelateCondition, context));
        } else {
            throw new UnsupportedOperationException("Do not support subquery in " + parent.getType());
        }
        return null;
    }

    public FilterSubqueryWrapperLabel filterSubqueryWrapper(LogicalFilter filter, Set<CorrelationId> corIds) {
        final Label input = inputStack.pop();

        final FilterSubqueryWrapperLabel filterSubqueryWrapperLabel = FilterSubqueryWrapperLabel
            .create(filter, input, null, corIds);
        inputStack.push(filterSubqueryWrapperLabel);

        return filterSubqueryWrapperLabel;
    }

    public Label top() {
        return inputStack.peek();
    }

    public Label window(Window window) {
        Preconditions.checkState(!inputStack.isEmpty());

        final Label input = inputStack.peek();

        return input.window(window, context);
    }

    public Label expand(LogicalExpand logicalExpand) {
        Preconditions.checkState(!inputStack.isEmpty());

        final Label input = inputStack.peek();

        return input.expand(logicalExpand, context);
    }

    public Label baseOf(Label current) {
        return label(current.getRel(), current.getType(), current.getInputs());
    }

    /**
     * Build default base label for PredicateNode pushdown.
     *
     * @param rel Base rel node
     * @param labelType Label type
     * @param inputs Inputs
     * @return New label
     */
    public Label label(RelNode rel, LabelType labelType, List<Label> inputs) {
        switch (labelType) {
        case TABLE_SCAN:
            return tableScan((TableScan) rel);
        case JOIN:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputStack.addAll(inputs);
            }
            // Do not set any join condition, in case of unintentionally use
            return join((Join) rel, JoinCondition.EMPTY);
        case VALUES:
            if (rel instanceof LogicalDynamicValues) {
                return values((LogicalDynamicValues) rel);
            } else {
                return values((LogicalValues) rel);
            }
        case UNION:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputStack.addAll(inputs);
            }
            return union((Union) rel);
        case PROJECT:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputStack.addAll(inputs);
            }
            return project((Project) rel);
        case AGGREGATE:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputStack.addAll(inputs);
            }
            return aggregate((Aggregate) rel);
        case FILTER_SUBQUERY:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputStack.addAll(inputs);
            }
            return filterSubqueryWrapper((LogicalFilter) rel, ImmutableSet.of());
        case CORRELATE:
            if (GeneralUtil.isNotEmpty(inputs)) {
                inputs.stream().forEach(i -> inputStack.push(i));
            }
            return correlate((LogicalCorrelate) rel);
        default:
            throw new NotSupportException("Unhandled label type: " + labelType);
        }

    }
}
