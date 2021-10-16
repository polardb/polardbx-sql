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
import com.alibaba.polardbx.optimizer.sharding.label.AggregateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.FullRowType;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.ProjectLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.label.UnionLabel;
import com.alibaba.polardbx.optimizer.sharding.label.ValuesLabel;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.RelOptUtil.RexInputConverter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdPredicates.SargableConditionInference;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil.deduplicate;

/**
 * @author chenmo.cm
 */
public class PredicatePushDownVisitor extends LabelShuttleImpl {

    private final ExtractorContext context;

    private Map<RelNode, Label> relLabelMap = new HashMap<>();
    private Map<RelOptTable, Map<RelNode, Label>> tableLabelMap = new HashMap<>();

    public PredicatePushDownVisitor(ExtractorContext context) {
        this.context = context;
    }

    @Override
    public Label visit(TableScanLabel tableScanLabel) {
        pushDown(tableScanLabel);

        final Label visited = super.visit(tableScanLabel);
        final TableScan tableScan = tableScanLabel.getRel();
        relLabelMap.put(tableScan, visited);
        tableLabelMap.computeIfAbsent(tableScan.getTable(), k -> new HashMap<>()).put(tableScan, visited);
        return visited;
    }

    @Override
    public Label visit(ProjectLabel projectLabel) {
        pushDown(projectLabel);

        // TODO do infer

        final Label visited = super.visit(projectLabel);
        relLabelMap.put(visited.getRel(), visited);
        return visited;
    }

    @Override
    public Label visit(JoinLabel joinLabel) {
        pushDown(joinLabel);

        final FullRowType fullRowType = joinLabel.getFullRowType();
        final boolean columnEquivalenceOnly = context.getType().columnEquivalenceOnly();

        // Push down column equality predicates
        pushDownColumnEquality(joinLabel, fullRowType.getColumnEqualities());

        if (columnEquivalenceOnly) {
            final Label visited = super.visit(joinLabel);
            relLabelMap.put(joinLabel.getRel(), visited);
            return visited;
        }

        final List<RexNode> leftPreds = new ArrayList<>();
        final List<RexNode> rightPreds = new ArrayList<>();

        PredicateUtil.mergePushdownPredicates(joinLabel, leftPreds, rightPreds);

        /*
         * Infer between left and right
         */
        final RelOptPredicateList inferred = PredicateUtil.infer(joinLabel.getRel(), leftPreds, rightPreds);

        if (!inferred.leftInferredPredicates.isEmpty()) {
            leftPreds.addAll(inferred.leftInferredPredicates);
        }
        if (!inferred.rightInferredPredicates.isEmpty()) {
            rightPreds.addAll(inferred.rightInferredPredicates);
        }

        // Pushdown to children label
        if (!leftPreds.isEmpty()) {
            final Label left = joinLabel.left();
            left.setPushdown(new PredicateNode(left, null, PredicateUtil.deduplicate(leftPreds, context), context));
        }

        if (!rightPreds.isEmpty()) {
            final Label right = joinLabel.right();
            right.setPushdown(new PredicateNode(right, null, PredicateUtil.deduplicate(rightPreds, context), context));
        }

        final Label visited = super.visit(joinLabel);
        relLabelMap.put(joinLabel.getRel(), visited);
        return visited;
    }

    @Override
    public Label visit(AggregateLabel aggregateLabel) {
        pushDown(aggregateLabel);

        // TODO do infer

        final Label visited = super.visit(aggregateLabel);
        relLabelMap.put(visited.getRel(), visited);
        return visited;
    }

    @Override
    public Label visit(UnionLabel unionLabel) {
        pushDown(unionLabel);

        // TODO do infer

        final Label visited = super.visit(unionLabel);
        relLabelMap.put(visited.getRel(), visited);
        return visited;
    }

    @Override
    public Label visit(ValuesLabel valuesLabel) {
        pushDown(valuesLabel);

        // TODO do infer

        final Label visited = super.visit(valuesLabel);
        relLabelMap.put(visited.getRel(), visited);
        return visited;
    }

    @Override
    public Label visit(SubqueryLabel subqueryLabel) {
        final Label input = subqueryLabel.getInput(0);

        if (null != subqueryLabel.getCorrelatePushdown()) {
            input.setCorrelatePushdown(
                new PredicateNode(input, null, subqueryLabel.getCorrelatePushdown().getDigests(), context));
        }

        final Label visited = super.visit(subqueryLabel);
        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(SubqueryWrapperLabel subqueryWrapperLabel) {
        final SubqueryWrapperLabel wrapperLabel = pushDown(subqueryWrapperLabel);

        final List<RelDataTypeField> fields = new ArrayList<>();
        final List<Pair<Label, ImmutableBitSet>> inputBitSets = new ArrayList<>();
        final List<RexNode> predicates = new ArrayList<>();

        final Map<? extends Label, RexPermuteInputsShuttle> inputPermuteMap = PredicateUtil
            .mergePushdownPredicates(wrapperLabel, fields, inputBitSets, predicates);

        final SargableConditionInference inference = PredicateUtil.infer(wrapperLabel,
            predicates,
            fields,
            context);

        final Map<String, RexNode> valuePredicates = inference.allValuePredicate();

        // Classify and permute inferred predicates
        final Map<Label, List<RexNode>> classified = PredicateUtil
            .dispatchAndPermute(valuePredicates, inputBitSets, inputPermuteMap, l -> l instanceof SubqueryLabel);

        // Push down predicates to children
        classified.forEach((label, preds) -> {
            if (label instanceof SubqueryLabel && !preds.isEmpty()) {
                label.setCorrelatePushdown(new PredicateNode(label, null, preds, context));
            } else if (label.getType().isSubqueryWrapper() && !preds.isEmpty()) {
                final Label input = label.getInput(0);
                input.setPushdown(new PredicateNode(input, null, preds, context));
            }
        });

        final Label visited = ((SubqueryWrapperLabel) super.visit(wrapperLabel)).subqueryAccept(this);
        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(CorrelateLabel wrapperLabel) {
//        final CorrelateLabel wrapperLabel = pushDown(subqueryWrapperLabel);

        final List<RelDataTypeField> fields = new ArrayList<>();
        final List<Pair<Label, ImmutableBitSet>> inputBitSets = new ArrayList<>();
        final List<RexNode> predicates = new ArrayList<>();

        final Map<? extends Label, RexPermuteInputsShuttle> inputPermuteMap = PredicateUtil
            .mergePushdownPredicates(wrapperLabel, fields, inputBitSets, predicates);

        final SargableConditionInference inference = PredicateUtil.infer(wrapperLabel,
            predicates,
            fields,
            context);

        final Map<String, RexNode> valuePredicates = inference.allValuePredicate();

        // Classify and permute inferred predicates
        final Map<Label, List<RexNode>> classified = PredicateUtil
            .dispatchAndPermute(valuePredicates, inputBitSets, inputPermuteMap, l -> true);

        // Push down predicates to children
        classified.forEach((label, preds) -> {
            if (!preds.isEmpty()) {
                label.setPushdown(new PredicateNode(label, null, preds, context));
            }
        });

        final Label visited = super.visit(wrapperLabel);
        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(Label other) {
        throw new NotSupportException("Unhandled label " + other.getClass().getName());
    }

    /**
     * Push down predicates belongs current label or pushed from top label to relNode of current label
     *
     * @param current current label
     */
    private <R extends Label> R pushDown(R current) {
        final Map<String, RexNode> pushdown = PredicateUtil.pushDown(current.getPredicates());

        Optional.ofNullable(current.getPullUp())
            .map(p -> p.pushDown(null))
            .ifPresent(p -> p.forEach(pushdown::putIfAbsent));

        Optional.ofNullable(current.getPushdown())
            .map(p -> p.pushDown(null))
            .ifPresent(p -> p.forEach(pushdown::putIfAbsent));

        Optional.ofNullable(current.getCorrelatePushdown())
            .ifPresent(p -> pushdown.putAll(p.getDigests()));

        if (!pushdown.isEmpty()) {
            current
                .setPushdown(new PredicateNode(context.labelBuilder().baseOf(current), null, pushdown, context));
        } else {
            current.setPushdown(null);
        }

        return current;
    }

    private void pushDownColumnEquality(JoinLabel joinLabel, Map<String, RexNode> inferred) {
        final Label left = joinLabel.left();
        final Label right = joinLabel.right();
        final Join join = joinLabel.getRel();
        final FullRowType fullRowType = joinLabel.getFullRowType();

        final Map<String, RexNode> leftEqualities = new HashMap<>();
        final Map<String, RexNode> rightEqualities = new HashMap<>();

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final int nFieldsTotal = fullRowType.fullRowType.getFieldCount();
        final int nFieldsLeft = left.getFullRowType().fullRowType.getFieldCount();

        final ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, nFieldsLeft);
        final ImmutableBitSet rightBitmap = ImmutableBitSet.range(nFieldsLeft, nFieldsTotal);

        final int[] rightAdjustments = new int[nFieldsTotal];
        IntStream.range(nFieldsLeft, nFieldsTotal).forEach(i -> rightAdjustments[i] = -nFieldsLeft);

        inferred.forEach((digest, filter) -> {
            final InputFinder inputFinder = InputFinder.analyze(filter);
            final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

            if (leftBitmap.contains(inputBits)) {
                leftEqualities.put(digest, filter);
            } else if (rightBitmap.contains(inputBits)) {
                final RexNode rex = filter.accept(new RexInputConverter(rexBuilder,
                    fullRowType.fullRowType.getFieldList(),
                    right.getFullRowType().fullRowType.getFieldList(),
                    rightAdjustments));
                rightEqualities.put(context.getDigestCache().digest(rex), rex);
            }
        });

        // Push down
        left.getFullRowType().getColumnEqualities().putAll(leftEqualities);
        right.getFullRowType().getColumnEqualities().putAll(rightEqualities);
    }

    public Map<RelOptTable, Map<RelNode, Label>> getTableLabelMap() {
        return tableLabelMap;
    }
}
