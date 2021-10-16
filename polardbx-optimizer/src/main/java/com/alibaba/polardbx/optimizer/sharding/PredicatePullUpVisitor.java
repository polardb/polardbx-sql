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

import com.alibaba.polardbx.optimizer.sharding.label.AbstractLabelOptNode;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.optimizer.sharding.label.AggregateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.FullRowType;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import com.alibaba.polardbx.optimizer.sharding.label.ProjectLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.label.UnionLabel;
import com.alibaba.polardbx.optimizer.sharding.label.ValuesLabel;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMdPredicates.SargableConditionInference;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil.deduplicate;

/**
 * @author chenmo.cm
 */
public class PredicatePullUpVisitor extends LabelShuttleImpl {

    private final ExtractorContext context;

    private Map<RelNode, Label> relLabelMap = new HashMap<>();
    private Map<RelOptTable, Map<RelNode, Label>> tableLabelMap = new HashMap<>();

    public PredicatePullUpVisitor(ExtractorContext context) {
        this.context = context;
    }

    @Override
    public Label visit(TableScanLabel tableScanLabel) {
        final Label visited = super.visit(tableScanLabel);
        final TableScan tableScan = tableScanLabel.getRel();

        pullUp(visited);

        relLabelMap.put(tableScan, visited);
        tableLabelMap.computeIfAbsent(tableScan.getTable(), k -> new HashMap<>()).put(tableScan, visited);

        return visited;
    }

    @Override
    public Label visit(ProjectLabel projectLabel) {
        final Label visited = super.visit(projectLabel);

        pullUp(visited);

        // TODO do infer

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(JoinLabel joinLabel) {
        final JoinLabel visited = (JoinLabel) super.visit(joinLabel);
        final Join join = joinLabel.getRel();
        final JoinRelType joinType = join.getJoinType();
        final FullRowType fullRowType = visited.getFullRowType();
        final boolean columnEqualityOnly = context.getType().columnEquivalenceOnly();

        /*
         * Pull up column equalities and merge with equalities from ON clause
         */
        final List<RexNode> onValuePreds = new ArrayList<>();
        final List<RexNode> topValuePreds = new ArrayList<>();
        final Map<String, RexNode> columnEqualities = PredicateUtil.mergePullUpColumnEquality(visited,
            context,
            columnEqualityOnly ? null : onValuePreds,
            columnEqualityOnly ? null : topValuePreds);

        // Accept all inferred column equality
        fullRowType.getColumnEqualities().putAll(columnEqualities);

        if (columnEqualityOnly) {
            relLabelMap.put(visited.getRel(), visited);
            return visited;
        }

        /*
         * Inference value predicates from ON clause. Base on pull-up column equalities
         */
        final Map<String, RexNode> inferredValuePreds = new HashMap<>();
        if (!onValuePreds.isEmpty()) {
            final SargableConditionInference eI = SargableConditionInference.create(join.getCluster(),
                columnEqualities,
                deduplicate(onValuePreds, context),
                fullRowType.fullRowType,
                joinLabel.left().getFullRowType().fullRowType.getFieldCount());

            inferredValuePreds.putAll(eI.valuePredicateForMoveRound(join));
        }

        /*
         * Inference value predicates from Filter on top of this label. Base on pull-up column equalities
         */
        if (!topValuePreds.isEmpty()) {
            final SargableConditionInference eI = SargableConditionInference.create(join.getCluster(),
                columnEqualities,
                deduplicate(topValuePreds, context),
                fullRowType.fullRowType,
                joinLabel.left().getFullRowType().fullRowType.getFieldCount());

            inferredValuePreds.putAll(eI.allValuePredicate());
        }

        // Accept inferred value predicates
        if (!inferredValuePreds.isEmpty()) {
            joinLabel.setValuePredicates(new PredicateNode(joinLabel,
                null,
                PredicateUtil.permutePredicateOnFullColumn(fullRowType.getFullColumnMapping(), inferredValuePreds),
                context));
        }

        final List<RexNode> leftPreds = new ArrayList<>();
        final List<RexNode> rightPreds = new ArrayList<>();

        PredicateUtil.mergePullUpPredicates(visited, leftPreds, rightPreds);

        /*
         * Infer between left and right
         */
        final RelOptPredicateList inferred = PredicateUtil.infer(join, leftPreds, rightPreds);

        final List<RexNode> inferredPreds =
            PredicateUtil.mergeInferredPredicateForPullUp(join, leftPreds, rightPreds, inferred);

        // Accept inferred predicates
        final List<PredicateNode> resultPreds = new ArrayList<>();
        resultPreds.add(new PredicateNode(context.labelBuilder().baseOf(joinLabel),
            null,
            inferredPreds,
            context));

        // Update value predicates for push down
        if (!inferredPreds.isEmpty()) {
            joinLabel.setValuePredicates(new PredicateNode(visited, null, inferredPreds, context));
        }

        // Accept predicates belongs to this label
        resultPreds.addAll(visited.getPredicates());

        // Set pull up predicates
        if (!resultPreds.isEmpty()) {
            visited.setPullUp(new PredicateNode(visited,
                null,
                deduplicate(PredicateUtil.pullUp(resultPreds, visited).values(), context),
                context));
        }

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(AggregateLabel aggregateLabel) {
        final Label visited = super.visit(aggregateLabel);

        pullUp(visited);

        // TODO do infer

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(UnionLabel unionLabel) {
        final Label visited = super.visit(unionLabel);

        // TODO do infer

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(ValuesLabel valuesLabel) {
        final Label visited = super.visit(valuesLabel);

        pullUp(visited);

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(SubqueryLabel subqueryLabel) {
        final Label visited = super.visit(subqueryLabel);

        pullUp(visited);

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(SubqueryWrapperLabel subqueryWrapperLabel) {
        // Visit all subquery
        final SubqueryWrapperLabel visited = ((SubqueryWrapperLabel) super.visit(subqueryWrapperLabel))
            .subqueryAccept(this);

        final List<RelDataTypeField> fields = new ArrayList<>();
        final List<Pair<Label, ImmutableBitSet>> inputBitSets = new ArrayList<>();
        final List<RexNode> predicates = new ArrayList<>();

        final Map<? extends Label, RexPermuteInputsShuttle> inputPermuteMap = PredicateUtil
            .mergePullUpPredicates(visited, fields, inputBitSets, predicates);

        final SargableConditionInference inference = PredicateUtil.infer(visited,
            predicates,
            fields,
            context);

        final Map<String, RexNode> valuePredicates = inference.allValuePredicate();

        // Classify and permute inferred predicates
        final Map<Label, List<RexNode>> classified = PredicateUtil
            .dispatchAndPermute(valuePredicates, inputBitSets, inputPermuteMap, l -> l instanceof SubqueryLabel);

        // Merge pull up predicates
        final List<PredicateNode> pullUp = new ArrayList<>(visited.getPredicates());
        final List<RexNode> inferredPullUp = classified.get(visited);
        if (null != inferredPullUp && !inferredPullUp.isEmpty()) {
            pullUp.add(new PredicateNode(context.labelBuilder().baseOf(visited), null, inferredPullUp, context));
        }

        final Map<String, RexNode> pullUps = PredicateUtil.pullUp(pullUp, visited);
        if (!pullUps.isEmpty()) {
            visited.setPullUp(new PredicateNode(visited, null, pullUps, context));
        }

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(CorrelateLabel correlate) {
        // Visit all subquery
        final CorrelateLabel visited = ((CorrelateLabel) super.visit(correlate));

        final List<RelDataTypeField> fields = new ArrayList<>();
        final List<Pair<Label, ImmutableBitSet>> inputBitSets = new ArrayList<>();
        final List<RexNode> predicates = new ArrayList<>();

        final Map<? extends Label, RexPermuteInputsShuttle> inputPermuteMap = PredicateUtil
            .mergePullUpPredicates(visited, fields, inputBitSets, predicates);

        final SargableConditionInference inference = PredicateUtil.infer(visited,
            predicates,
            fields,
            context);

        final Map<String, RexNode> valuePredicates = inference.allValuePredicate();

        // Classify and permute inferred predicates
        final Map<Label, List<RexNode>> classified = PredicateUtil
            .dispatchAndPermute(valuePredicates, inputBitSets, inputPermuteMap, l -> true);

        // Merge pull up predicates
        final List<PredicateNode> pullUp = new ArrayList<>(visited.getPredicates());
        final List<RexNode> inferredPullUp = classified.get(visited);
        if (null != inferredPullUp && !inferredPullUp.isEmpty()) {
            pullUp.add(new PredicateNode(context.labelBuilder().baseOf(visited), null, inferredPullUp, context));
        }

        final Map<String, RexNode> pullUps = PredicateUtil.pullUp(pullUp, visited);
        if (!pullUps.isEmpty()) {
            visited.setPullUp(new PredicateNode(visited, null, pullUps, context));
        }

        relLabelMap.put(visited.getRel(), visited);

        return visited;
    }

    @Override
    public Label visit(Label other) {
        throw new NotSupportException("Unhandled label " + other.getClass().getName());
    }

    /**
     * Pull up predicates for single or none input label
     *
     * @param current Current label
     * @return Current label with pull up predicates in
     * {@link AbstractLabelOptNode#pullUp}
     */
    private <R extends Label> R pullUp(R current) {
        final Map<String, RexNode> pullUps = PredicateUtil.pullUp(current.getPredicates(), current);

        if (null != current.getInputs() && !current.getInputs().isEmpty()) {
            Preconditions.checkArgument(current.getInputs().size() == 1);
            Optional.ofNullable(current.getInput(0).getPullUp())
                .map(p -> p.rebaseTo(current, null))
                .ifPresent(p -> p.forEach(pullUps::putIfAbsent));
        }

        if (!pullUps.isEmpty()) {
            current.setPullUp(new PredicateNode(current, null, pullUps, context));
        } else {
            current.setPullUp(null);
        }

        return current;
    }

    public Map<RelNode, Label> getRelLabelMap() {
        return relLabelMap;
    }

    public Map<RelOptTable, Map<RelNode, Label>> getTableLabelMap() {
        return tableLabelMap;
    }
}
