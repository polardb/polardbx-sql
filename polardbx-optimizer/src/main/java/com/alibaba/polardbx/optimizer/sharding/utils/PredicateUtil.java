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

package com.alibaba.polardbx.optimizer.sharding.utils;

import com.alibaba.polardbx.optimizer.sharding.PredicatePullUpVisitor;
import com.alibaba.polardbx.optimizer.sharding.label.AbstractLabelOptNode;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.FullRowType;
import com.alibaba.polardbx.optimizer.sharding.label.JoinCondition;
import com.alibaba.polardbx.optimizer.sharding.label.JoinCondition.PredicateType;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.metadata.RelMdPredicates.JoinConditionBasedPredicateInference;
import org.apache.calcite.rel.metadata.RelMdPredicates.SargableConditionInference;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class PredicateUtil {

    public static EnumSet<JoinRelType> INFER_VALUE_PRED_FROM_ON_CLAUSE = EnumSet
        .of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT);

    /**
     * <pre>
     * Merge column equality related to join label, predicates comes from
     *  1. Predicates pulled up from input label by {@link PredicatePullUpVisitor}
     *  2. Predicates inferred from ON clause by {@link PredicatePullUpVisitor}
     * </pre>
     *
     * @param joinLabel Join label
     * @return Column equality
     */

    public static Map<String, RexNode> mergePullUpColumnEquality(JoinLabel joinLabel, ExtractorContext context,
                                                                 List<RexNode> onValuePredicates,
                                                                 List<RexNode> topValuePredicates) {
        final Join join = joinLabel.getRel();
        final JoinRelType joinType = join.getJoinType();
        final Label left = joinLabel.left();
        final Label right = joinLabel.right();
        final FullRowType fullRowType = joinLabel.getFullRowType();
        final Mapping fullColumnMapping = fullRowType.getFullColumnMapping();

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        /*
         * Column equality from ON clause
         */
        final RexPermuteInputsShuttle onPermute = RexPermuteInputsShuttle.of(fullColumnMapping);
        final ImmutableBitSet columnInputSet = fullRowType.getColumnInputSet();

        final JoinCondition joinCondition = joinLabel.getJoinCondition();
        final List<RexNode> onCondition = joinCondition.forMoveAround(join, PredicateType.COLUMN_EQUALITY);

        final List<RexNode> permutedOnCondition = onCondition
            .stream()
            .filter(p -> columnInputSet.contains(InputFinder.bits(p)))
            .map(p -> p.accept(onPermute))
            .collect(Collectors.toList());

        final List<RexNode> permutedOuterSideCondition = joinCondition
            .fromNonNullGenerateSide(joinType, PredicateType.COLUMN_EQUALITY)
            .stream()
            .filter(p -> columnInputSet.contains(InputFinder.bits(p)))
            .map(p -> p.accept(onPermute))
            .collect(Collectors.toList());

        if (null != onValuePredicates) {
            joinCondition.all(PredicateType.SARGABLE, context).values().stream()
                .filter(p -> columnInputSet.contains(InputFinder.bits(p)))
                .map(p -> p.accept(onPermute))
                .forEach(onValuePredicates::add);
        }

        /*
         * Pull up column equality
         */
        int nFieldsLeft = left.getFullRowType().fullRowType.getFieldCount();
        int nFieldsRight = right.getFullRowType().fullRowType.getFieldCount();

        final Map<String, RexNode> lPullUpCe = left.getFullRowType().getColumnEqualities();
        final Map<String, RexNode> rPullUpCe = deduplicate(
            PredicateUtil
                .shiftRightPredicates(right.getFullRowType().getColumnEqualities().values(), nFieldsLeft, nFieldsRight),
            context);

        // Infer among pull-up equalities and merged equality group
        final List<RexNode> pullUps = new ArrayList<>(lPullUpCe.values());
        pullUps.addAll(rPullUpCe.values());
        pullUps.addAll(permutedOuterSideCondition);

        final List<RelDataTypeField> fieldList = fullRowType.fullRowType.getFieldList();

        final JoinCondition inferredWithPullUps = inferColumnEquality(permutedOnCondition,
            pullUps,
            nFieldsLeft,
            fieldList.size(),
            (l, r) -> buildEquality(l, r, fieldList, rexBuilder, context));

        // Add pull-up predicates again. Because for LEFT/RIGHT JOIN predicates on non-null-generate side is
        // not included in inferred column equalities
        final Map<String, RexNode> result = new HashMap<>(lPullUpCe);
        result.putAll(rPullUpCe);
        result.putAll(inferredWithPullUps.forMoveAround(join, PredicateType.COLUMN_EQUALITY, context));


        /*
         * Add column equalities on top of join
         */
        final Map<String, RexNode> topPredicates = pushDown(joinLabel.getPredicates());
        final List<RexNode> topEqualities = new ArrayList<>();
        final List<RexNode> topValuePreds = new ArrayList<>();
        classify(topPredicates.values()
            .stream()
            .filter(p -> columnInputSet.contains(InputFinder.bits(p)))
            .collect(Collectors.toList()), topEqualities, topValuePreds);

        if (!topEqualities.isEmpty()) {
            final JoinCondition inferredWithTop = inferColumnEquality(
                topEqualities.stream().map(p -> p.accept(onPermute)).collect(Collectors.toList()),
                result.values(),
                nFieldsLeft,
                fieldList.size(),
                (l, r) -> buildEquality(l, r, fieldList, rexBuilder, context));

            result.putAll(inferredWithTop.all(PredicateType.COLUMN_EQUALITY, context));
        }

        if (null != topValuePredicates) {
            pushThroughJoin(topValuePreds.stream().map(p -> p.accept(onPermute)).collect(Collectors.toList()), join,
                topValuePredicates);
        }

        return result;
    }

    /**
     * Inference all column equalities from current and pull-up predicates
     *
     * @param base Current column equality predicates
     * @param pullUps Column equalities pulled up from input labels
     * @param nFieldsLeft Field count of left table
     * @param nFields Field count of join
     * @param equalityBuilder Function for build RexNode
     * @return Map of RexNode and its digest
     */
    public static JoinCondition inferColumnEquality(List<RexNode> base, Collection<RexNode> pullUps, int nFieldsLeft,
                                                    int nFields,
                                                    BiFunction<Integer, Integer, Pair<String, RexNode>> equalityBuilder) {
        final List<BitSet> groups = base.stream()
            .map(PredicateUtil::rexToBitSet)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        // Merge equality group
        final List<BitSet> merged = Util.mergeIntersectedSets(groups);

        /*
         * Column equalities pulled up from input label cannot be put together for
         * inference again, because we do not know the direction of inference now. For
         * example, column equalities from LEFT JOIN can only infer from left to right.
         * But, at here, we have no idea whether any of pullUps are pulled up from the ON
         * clause of a LEFT JOIN
         */
        final List<List<BitSet>> groupLists = new ArrayList<>();
        pullUps.stream()
            .map(PredicateUtil::rexToBitSet)
            .filter(Objects::nonNull)
            .map(b -> ImmutableList.<BitSet>builder().add(b).addAll(merged).build())
            .map(Util::mergeIntersectedSets)
            .forEach(groupLists::add);

        if (groupLists.isEmpty()) {
            groupLists.add(merged);
        }

        final List<BitSet> resultGroups = Util.unionSets(groupLists, nFields);

        return convertEqualityToRex(resultGroups,
            nFieldsLeft,
            nFields,
            equalityBuilder);
    }

    /**
     * Convert column equalities to RexNode
     *
     * @param equalities Example: BitSet g = equalities.get(i) means column index i is equality to all column index in g
     * @param leftFieldCount Field count of left table
     * @param fieldCount Field count of join
     * @param equalityBuilder Function for build RexNode
     * @return Map of RexNode and its digest
     */
    public static JoinCondition convertEqualityToRex(List<BitSet> equalities, int leftFieldCount, int fieldCount,
                                                     BiFunction<Integer, Integer, Pair<String, RexNode>> equalityBuilder) {
        final ImmutableBitSet leftFieldsBitSet = ImmutableBitSet.range(0, leftFieldCount);
        final ImmutableBitSet rightFieldsBitSet = ImmutableBitSet.range(leftFieldCount, fieldCount);

        final JoinCondition result = new JoinCondition();

        // Update equivalence
        final Set<Pair<Integer, Integer>> exists = new HashSet<>();
        for (int l = 0; l < equalities.size(); l++) {
            final List<Integer> rList = equalities.get(l).stream().boxed().collect(Collectors.toList());

            if (rList.isEmpty()) {
                continue;
            }

            for (int i = 0; i < rList.size(); i++) {
                final int r = rList.get(i);
                if (l == r || exists.contains(Pair.of(l, r))) {
                    continue;
                }

                exists.add(Pair.of(l, r));
                exists.add(Pair.of(r, l));

                final Pair<String, RexNode> e = equalityBuilder.apply(l, r);
                final String digest = e.left;
                final RexNode call = e.right;

                final ImmutableBitSet bits = ImmutableBitSet.of(l, r);

                if (leftFieldsBitSet.contains(bits)) {
                    result.getLeftEqualityMap().put(digest, call);
                } else if (rightFieldsBitSet.contains(bits)) {
                    result.getRightEqualityMap().put(digest, call);
                } else if (leftFieldsBitSet.intersects(bits) && rightFieldsBitSet.intersects(bits)) {
                    result.getCrossEqualityMap().put(digest, call);
                } else {
                    result.getOtherMap().put(digest, call);
                }
            }
        }

        return result;
    }

    /**
     * Inference all column equalities from current predicates
     *
     * @param base Current column equality predicates
     * @param equalityBuilder Function for build RexNode
     * @return Map of RexNode and its digest
     */
    public static Map<String, RexNode> inferColumnEquality(List<RexNode> base,
                                                           BiFunction<Integer, Integer, Pair<String, RexNode>> equalityBuilder) {
        final List<BitSet> groups = base.stream()
            .map(PredicateUtil::rexToBitSet)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        // Merge equality group
        final List<BitSet> merged = Util.mergeIntersectedSets(groups);

        return convertEqualityGroupToRex(merged, equalityBuilder);
    }

    /**
     * Convert equality groups to RexNode
     *
     * @param groups Group list, column index in one group are equivalence
     * @param equalityBuilder Function for build RexNode
     * @return Map of RexNode and its digest
     */
    public static Map<String, RexNode> convertEqualityGroupToRex(List<BitSet> groups,
                                                                 BiFunction<Integer, Integer, Pair<String, RexNode>> equalityBuilder) {
        final Map<String, RexNode> result = new HashMap<>();

        // Update equivalence
        groups.forEach(g -> {
            final Set<Pair<Integer, Integer>> exists = new HashSet<>();
            final List<Integer> equals = g.stream().boxed().collect(Collectors.toList());
            for (int i = 0; i < equals.size(); i++) {
                for (int j = i + 1; j < equals.size(); j++) {
                    final Integer l = equals.get(i);
                    final Integer r = equals.get(j);

                    if (exists.contains(Pair.of(l, r))) {
                        continue;
                    }

                    exists.add(Pair.of(l, r));
                    exists.add(Pair.of(r, l));

                    final Pair<String, RexNode> equality = equalityBuilder.apply(l, r);

                    result.put(equality.left, equality.right);
                }
            }
        });

        return result;
    }

    /**
     * <pre>
     * Merge predicates related to join label, predicates comes from
     *  1. Predicates belongs or push down to current label
     *  2. Predicates pull up from input label by {@link PredicatePullUpVisitor}
     *  3. Predicates inferred from ON clause by {@link PredicatePullUpVisitor}
     * </pre>
     *
     * @param joinLabel Join label
     * @param leftPreds Result left predicates
     * @param rightPreds Result right predicates
     */
    public static void mergePushdownPredicates(JoinLabel joinLabel, List<RexNode> leftPreds, List<RexNode> rightPreds) {
        final Join join = joinLabel.getRel();
        JoinRelType joinType = join.getJoinType();

        // Predicates belongs or push down to this label
        final Map<String, RexNode> top = Optional.ofNullable(joinLabel.getPushdown())
            .map(PredicateNode::getDigests)
            .orElseGet(HashMap::new);

        // Predicates inferred by PredicatePullUpVisitor
        Optional.ofNullable(joinLabel.getValuePredicates()).ifPresent(p -> top.putAll(p.getDigests()));

        // Check null accept predicates
        final List<RexNode> topPredicates = pushThroughJoin(top.values(), join, new ArrayList<>());

        if (!topPredicates.isEmpty() && joinType != JoinRelType.INNER && !(join instanceof SemiJoin)) {
            ImmutableList<RexNode> topPredicatesList = ImmutableList.copyOf(topPredicates);
            joinType = RelOptUtil.simplifyJoin(join, topPredicatesList, joinType);
        }

        // Split top predicates to left and right
        RelOptUtil.classifyFilters(
            join, topPredicates, joinType,
            false,
            true,
            true,
            null, leftPreds, rightPreds);

        // Predicates pulled up from input labels
        if (null != leftPreds) {
            Optional.ofNullable(joinLabel.left().getPullUp())
                .map(PredicateNode::getPredicates)
                .ifPresent(leftPreds::addAll);
        }
        if (null != rightPreds && !(join.getRight() instanceof Project)) {
            Optional.ofNullable(joinLabel.right().getPullUp())
                .map(PredicateNode::getPredicates)
                .ifPresent(rightPreds::addAll);
        }
    }

    /**
     * <pre>
     * Merge predicates related to join label, predicates comes from
     *  1. Predicates belongs to current label
     *  2. Predicates pulled up from input label by {@link PredicatePullUpVisitor}
     *  3. Predicates inferred from ON clause by {@link PredicatePullUpVisitor}
     * </pre>
     *
     * @param joinLabel Join label
     * @param leftPreds Result left predicates
     * @param rightPreds Result right predicates
     */
    public static void mergePullUpPredicates(JoinLabel joinLabel, List<RexNode> leftPreds, List<RexNode> rightPreds) {
        final Join join = joinLabel.getRel();
        JoinRelType joinType = join.getJoinType();

        // Predicates belongs to this label
        final Map<String, RexNode> topPredicates = pushDown(joinLabel.getPredicates());

        // Predicates inferred from ON clause
        if (null != joinLabel.getValuePredicates()) {
            topPredicates.putAll(joinLabel.getValuePredicates().getDigests());
        }

        if (!topPredicates.isEmpty() && joinType != JoinRelType.INNER && !(join instanceof SemiJoin)) {
            ImmutableList<RexNode> topPredicatesList = ImmutableList.copyOf(topPredicates.values());
            joinType = RelOptUtil.simplifyJoin(join, topPredicatesList, joinType);
        }

        // Split predicates to left and right
        RelOptUtil.classifyFilters(join,
            new ArrayList<>(topPredicates.values()),
            joinType,
            false,
            true,
            true,
            null,
            leftPreds,
            rightPreds);

        // Predicates pulled up from input labels
        if (null != leftPreds) {
            Optional.ofNullable(joinLabel.left().getPullUp())
                .map(PredicateNode::getPredicates)
                .ifPresent(leftPreds::addAll);
        }
        if (null != rightPreds && !(join.getRight() instanceof Project)) {
            Optional.ofNullable(joinLabel.right().getPullUp())
                .map(PredicateNode::getPredicates)
                .ifPresent(rightPreds::addAll);
        }
    }

    public static Map<String, RexNode> pushDown(List<PredicateNode> predicates) {
        return predicates.stream()
            // pushdown predicates
            .map(p -> p.pushDown(null))
            .filter(Objects::nonNull)
            .flatMap(p -> p.getDigests().entrySet().stream())
            // deduplicate
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (rex1, rex2) -> rex1));
    }

    public static Map<String, RexNode> pullUp(List<PredicateNode> predicates, Label current) {
        return predicates.stream()
            // pull up predicates
            .map(p -> p.rebaseTo(current))
            .filter(Objects::nonNull)
            .flatMap(p -> p.getDigests().entrySet().stream())
            // deduplicate
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (rex1, rex2) -> rex1));
    }

    /**
     * Find out which condition can be pushed through join
     *
     * @param predicates Predicates
     * @param join Join rel node
     * @param pushed Predicates can be pushed
     * @return @param pushed
     */
    public static List<RexNode> pushThroughJoin(Collection<RexNode> predicates, Join join, List<RexNode> pushed) {
        final JoinRelType joinType = join.getJoinType();

        final int nSysFields = 0;
        final int nFieldsLeft =
            join.getInputs().get(0).getRowType().getFieldCount();
        final int nFieldsRight =
            join.getInputs().get(1).getRowType().getFieldCount();
        final int nTotalFields = nFieldsLeft + nFieldsRight;

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap =
            ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
        ImmutableBitSet rightBitmap =
            ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

        predicates.stream()
            // Do not use "IS NULL" for partition pruning in "SELECT FROM a LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL"
            .filter(predicate -> !joinType.generatesNullsOnRight() || !containsColumn(predicate, rightBitmap) || Strong
                .isNotTrue(predicate, rightBitmap))
            // Do not use "IS NULL" for partition pruning in "SELECT FROM a RIGHT JOIN b ON a.id = b.id WHERE a.id IS NULL"
            .filter(predicate -> !joinType.generatesNullsOnLeft() || !containsColumn(predicate, leftBitmap) || Strong
                .isNotTrue(predicate, leftBitmap))
            .forEachOrdered(pushed::add);

        return pushed;
    }

    public static List<RexNode> pushThroughJoin(RexNode filterAboveJoin, Join join, List<RexNode> pushed) {
        final List<RexNode> conjunctions = RelOptUtil.conjunctions(filterAboveJoin);
        return pushThroughJoin(conjunctions, join, pushed);
    }

    /**
     * Infer predicates between left and right
     *
     * @param join Join relNode
     * @param leftPredicates Left predicates
     * @param rightPredicates Right predicates
     * @return Inferred
     */
    public static RelOptPredicateList infer(Join join, List<RexNode> leftPredicates, List<RexNode> rightPredicates) {
        final RexBuilder builder = join.getCluster().getRexBuilder();

        final JoinConditionBasedPredicateInference jI = new JoinConditionBasedPredicateInference(join,
            RexUtil.composeConjunction(builder, leftPredicates, true),
            RexUtil.composeConjunction(builder, rightPredicates, true));

        return jI.inferPredicates(false);
    }

    private static boolean containsColumn(RexNode condition, ImmutableBitSet inputSet) {
        final InputFinder inputFinder = InputFinder.analyze(condition);
        final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

        return inputSet.intersects(inputBits);
    }

    public static Map<String, RexNode> deduplicate(RexNode predicate, ExtractorContext context) {
        final List<RexNode> decomposed = RelOptUtil.conjunctions(predicate);
        return deduplicate(decomposed, context);
    }

    public static Map<String, RexNode> deduplicate(Collection<RexNode> decomposed, ExtractorContext context) {
        final DigestCache digestCache = context.getDigestCache();

        // Remove duplicated condition
        return decomposed.stream().collect(Collectors.toMap(digestCache::digest, rex -> rex, (rex1, rex2) -> rex1));
    }

    /**
     * Convert predicates base on
     * {@link FullRowType} back
     * to predicates base on {@link AbstractLabelOptNode#getRel()}
     *
     * @param fullColumnMapping Full column mapping of label currently predicates
     * based on
     * @param predicates Predicates
     * @return Converted predicates
     */
    public static Map<String, RexNode> permutePredicateOnFullColumn(Mapping fullColumnMapping,
                                                                    Map<String, RexNode> predicates) {
        final Mapping inverted = fullColumnMapping.inverse();
        final List<Integer> sourceList = new ArrayList<>();
        for (IntPair intPair : inverted) {
            sourceList.add(intPair.source);
        }

        final RexPermuteInputsShuttle permute = RexPermuteInputsShuttle.of(inverted);
        final ImmutableBitSet inputSet = ImmutableBitSet.of(sourceList);

        Map<String, RexNode> result = new HashMap<>();
        for (Entry<String, RexNode> entry : predicates.entrySet()) {
            final RexNode rexNode = entry.getValue();
            if (inputSet.contains(InputFinder.bits(rexNode))) {
                RexNode call = rexNode.accept(permute);
                result.put(call.toString(), call);
            }
        }
        return result;
    }

    /**
     * Merge predicates from subquery and wrapper for push down. Different from
     * {@link #mergePushdownPredicates(CorrelateLabel, List, List, List)},
     * this method uses the current rowType of subquery label
     *
     * @param wrapperLabel SubqueryWrapperLabel
     * @param fields Concat of all subquery fields, for output
     * @param inputBitSets Relation of column index and subquery, for output
     * @param predicates Merged predicates, for output
     * @return Permutation for each subquery for further pull up
     */
    public static Map<Label, RexPermuteInputsShuttle> mergePullUpPredicates(CorrelateLabel wrapperLabel,
                                                                            List<RelDataTypeField> fields,
                                                                            List<Pair<Label, ImmutableBitSet>> inputBitSets,
                                                                            List<RexNode> predicates) {
        // Add predicates of current label
        fields.addAll(wrapperLabel.getRel().getRowType().getFieldList());
        inputBitSets.add(Pair.of(wrapperLabel.left(),
            ImmutableBitSet.range(0, wrapperLabel.getRel().getInput(0).getRowType().getFieldCount())));
        wrapperLabel.getPredicates()
            .stream()
            .map(p -> p.pushDown(null))
            .filter(Objects::nonNull)
            .flatMap(p -> p.getPredicates().stream())
            .forEach(predicates::add);
        predicates.addAll(Optional.ofNullable(wrapperLabel.getInputs().get(0).getPullUp())
            .filter(Objects::nonNull)
            .map(PredicateNode::getPredicates)
            .orElseGet(ImmutableList::of));

        final List<RexNode> toRemove = Lists.newArrayList();
        String correlateTargetName = wrapperLabel.getRel().getCorrelVariable();
        for (RexNode rex : predicates) {
            List<RexFieldAccess> rexFieldAccesses = RexUtil.findFieldAccessesDeep(rex);
            if (rexFieldAccesses.stream().anyMatch(
                r -> !((RexCorrelVariable) r.getReferenceExpr()).getId().getName().equals(correlateTargetName))) {
                toRemove.add(rex);
            }
        }
        predicates.removeAll(toRemove);

        return mergePullUpSubqueryPredicates(wrapperLabel, fields, inputBitSets, predicates);
    }

    /**
     * Merge predicates from subquery and wrapper for push down. Different from
     * {@link #mergePullUpPredicates(CorrelateLabel, List, List, List)}, this
     * method uses the base rowType of subquery label
     *
     * @param correlateLabel CorrelateLabel
     * @param fields Concat of all subquery fields, for output
     * @param inputBitSets Relation of column index and subquery, for output
     * @param predicates Merged predicates, for output
     * @return Permutation for each subquery for further pull up
     */
    public static Map<? extends Label, RexPermuteInputsShuttle> mergePushdownPredicates(CorrelateLabel correlateLabel,
                                                                                        List<RelDataTypeField> fields,
                                                                                        List<Pair<Label, ImmutableBitSet>> inputBitSets,
                                                                                        List<RexNode> predicates) {
        // Add predicates of current label
        fields.addAll(correlateLabel.getRel().getRowType().getFieldList());
        LogicalCorrelate correlate = correlateLabel.getRel();
        if (correlate.getJoinType() != SemiJoinType.LEFT) {
            fields.addAll(correlateLabel.getRel().getInput(1).getRowType().getFieldList());
        }
        inputBitSets.add(Pair.of(correlateLabel.left(),
            ImmutableBitSet.range(0, correlateLabel.getRel().getInput(0).getRowType().getFieldCount())));
        if (null != correlateLabel.getPushdown()) {
            predicates.addAll(correlateLabel.getPushdown().getPredicates());
        }

        return mergePushdownSubqueryPredicates(correlateLabel, fields, inputBitSets, predicates);
    }

    /**
     * <pre>
     *     1. shift and merge the pull-up predicates of subquery
     *     2. Concat rowType of each subquery
     *     3. Generate permutation for each subquery for further push up
     * </pre>
     *
     * @param correlateLabel Subquery label
     * @param outFields Concat of all subquery fields
     * @param outInputBitSets Relation of column index and subquery
     * @param outPredicates Merged predicates
     * @return Permutation for each subquery for further pull up
     */
    public static Map<Label, RexPermuteInputsShuttle> mergePullUpSubqueryPredicates(
        CorrelateLabel correlateLabel,
        List<RelDataTypeField> outFields,
        List<Pair<Label, ImmutableBitSet>> outInputBitSets,
        List<RexNode> outPredicates) {
        // Put all columns and predicates together
        final Map<Label, RexPermuteInputsShuttle> inputPermuteMap = new HashMap<>();
        final RelDataType subqueryRowType = correlateLabel.getRel().getInput(1).getRowType();
        final int leftCount = correlateLabel.getRel().getInput(0).getRowType().getFieldCount();
        final int rightCount = subqueryRowType.getFieldCount();

        outFields.addAll(correlateLabel.getRel().getInput(1).getRowType().getFieldList());
        final RexPermuteInputsShuttle permute = RexPermuteInputsShuttle
            .of(Mappings.createShiftMapping(rightCount, leftCount + 1, 0, rightCount));

        // Add inferred correlation conditions
        Optional.ofNullable(correlateLabel.getInferredCorrelateCondition())
            .ifPresent(preds -> preds.getPredicates().forEach(p -> outPredicates.add(p.accept(permute))));

        // Pull up predicates from subquery
        Optional.ofNullable(correlateLabel.right().getPullUp())
            .filter(Objects::nonNull)
            .ifPresent(p -> p.forEach(
                (digest, pullUp) -> RelOptUtil.decomposeConjunction(pullUp, outPredicates)));

        // Generate permutations for pull up inferred predicates
        outInputBitSets
            .add(Pair.of(correlateLabel.right(), ImmutableBitSet.range(leftCount + 1, 1 + leftCount + rightCount)));
        inputPermuteMap.put(correlateLabel.right(),
            RexPermuteInputsShuttle
                .of(Mappings.createShiftMapping(outFields.size(), 0, leftCount + 1, rightCount)));
        inputPermuteMap.put(correlateLabel.left(),
            RexPermuteInputsShuttle
                .of(Mappings.createShiftMapping(outFields.size(), 0, 0, leftCount)));
        return inputPermuteMap;
    }

    /**
     * Merge predicates from subquery and wrapper for push down. Different from
     * {@link #mergePushdownPredicates(SubqueryWrapperLabel, List, List, List)},
     * this method uses the current rowType of subquery label
     *
     * @param wrapperLabel SubqueryWrapperLabel
     * @param fields Concat of all subquery fields, for output
     * @param inputBitSets Relation of column index and subquery, for output
     * @param predicates Merged predicates, for output
     * @return Permutation for each subquery for further pull up
     */
    public static Map<SubqueryLabel, RexPermuteInputsShuttle> mergePullUpPredicates(SubqueryWrapperLabel wrapperLabel,
                                                                                    List<RelDataTypeField> fields,
                                                                                    List<Pair<Label, ImmutableBitSet>> inputBitSets,
                                                                                    List<RexNode> predicates) {
        // Add predicates of current label
        fields.addAll(wrapperLabel.getRel().getRowType().getFieldList());
        inputBitSets.add(Pair.of(wrapperLabel, ImmutableBitSet.range(0, fields.size())));
        wrapperLabel.getPredicates()
            .stream()
            .map(p -> p.pushDown(null))
            .filter(Objects::nonNull)
            .flatMap(p -> p.getPredicates().stream())
            .forEach(predicates::add);
        predicates.addAll(Optional.ofNullable(wrapperLabel.getInputs().get(0).getPullUp())
            .map(PredicateNode::getPredicates)
            .orElseGet(ImmutableList::of));

        return mergePullUpSubqueryPredicates(wrapperLabel.getInferableSubqueryLabelMap(), fields, inputBitSets,
            predicates);
    }

    /**
     * Merge predicates from subquery and wrapper for push down. Different from
     * {@link #mergePullUpPredicates(SubqueryWrapperLabel, List, List, List)}, this
     * method uses the base rowType of subquery label
     *
     * @param wrapperLabel SubqueryWrapperLabel
     * @param fields Concat of all subquery fields, for output
     * @param inputBitSets Relation of column index and subquery, for output
     * @param predicates Merged predicates, for output
     * @return Permutation for each subquery for further pull up
     */
    public static Map<SubqueryLabel, RexPermuteInputsShuttle> mergePushdownPredicates(SubqueryWrapperLabel wrapperLabel,
                                                                                      List<RelDataTypeField> fields,
                                                                                      List<Pair<Label, ImmutableBitSet>> inputBitSets,
                                                                                      List<RexNode> predicates) {
        // Add predicates of current label
        fields.addAll(wrapperLabel.getRel().getRowType().getFieldList());
        inputBitSets.add(Pair.of(wrapperLabel, ImmutableBitSet.range(0, fields.size())));
        if (null != wrapperLabel.getPushdown()) {
            predicates.addAll(wrapperLabel.getPushdown().getPredicates());
        }

        return mergePushdownSubqueryPredicates(wrapperLabel.getInferableSubqueryLabelMap(), fields, inputBitSets,
            predicates);
    }

    /**
     * <pre>
     *     1. shift and merge the pull-up predicates of subquery
     *     2. Concat rowType of each subquery
     *     3. Generate permutation for each subquery for further push up
     * </pre>
     *
     * @param subqueryLabelMap Subquery labels
     * @param outFields Concat of all subquery fields
     * @param outInputBitSets Relation of column index and subquery
     * @param outPredicates Merged predicates
     * @return Permutation for each subquery for further pull up
     */
    public static Map<SubqueryLabel, RexPermuteInputsShuttle> mergePullUpSubqueryPredicates(
        Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
        List<RelDataTypeField> outFields,
        List<Pair<Label, ImmutableBitSet>> outInputBitSets,
        List<RexNode> outPredicates) {
        // Put all columns and predicates together
        final Map<SubqueryLabel, RexPermuteInputsShuttle> inputPermuteMap = new HashMap<>();
        for (Entry<RexSubQuery, SubqueryLabel> entry : subqueryLabelMap.entrySet()) {
            final SubqueryLabel subquery = entry.getValue();
            final RelDataType subqueryRowType = subquery.getRowType();
            final int leftCount = outFields.size();
            final int rightCount = subqueryRowType.getFieldCount();

            final RexPermuteInputsShuttle permute = RexPermuteInputsShuttle
                .of(Mappings.createShiftMapping(rightCount, leftCount, 0, rightCount));

            // Add inferred correlation conditions
            Optional.ofNullable(subquery.getInferredCorrelateCondition())
                .ifPresent(preds -> preds.getPredicates().forEach(p -> outPredicates.add(p.accept(permute))));

            // Pull up predicates from subquery
            Optional.ofNullable(subquery.getPullUp())
                .ifPresent(p -> p.forEach(
                    (digest, pullUp) -> RelOptUtil.decomposeConjunction(pullUp.accept(permute), outPredicates)));

            // Generate permutations for pull up inferred predicates
            outFields.addAll(subqueryRowType.getFieldList());
            outInputBitSets.add(Pair.of(subquery, ImmutableBitSet.range(leftCount, leftCount + rightCount)));
            inputPermuteMap.put(subquery,
                RexPermuteInputsShuttle
                    .of(Mappings.createShiftMapping(leftCount + rightCount, 0, leftCount, rightCount)));
        }
        return inputPermuteMap;
    }

    /**
     * <pre>
     *     1. Push down(to base rowType of its input rel), shift and merge predicates of subquery
     *     2. Concat rowType of each subquery
     *     3. Generate permutation for each subquery for further push down
     * </pre>
     *
     * @param subqueryLabelMap Subquery labels
     * @param outFields Concat of all subquery fields
     * @param outInputBitSets Relation of column index and subquery
     * @param outPredicates Merged predicates
     * @return Permutation for each subquery for further push down
     */
    public static Map<SubqueryLabel, RexPermuteInputsShuttle> mergePushdownSubqueryPredicates(
        Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
        List<RelDataTypeField> outFields,
        List<Pair<Label, ImmutableBitSet>> outInputBitSets,
        List<RexNode> outPredicates) {
        // Put all columns and predicates together
        final Map<SubqueryLabel, RexPermuteInputsShuttle> inputPermuteMap = new HashMap<>();
        for (Entry<RexSubQuery, SubqueryLabel> entry : subqueryLabelMap.entrySet()) {
            final SubqueryLabel subquery = entry.getValue();
            // Use the base rowType of subquery label
            final RelDataType subqueryRowType = subquery.getInput(0).getRel().getRowType();
            final int leftCount = outFields.size();
            final int rightCount = subqueryRowType.getFieldCount();

            final RexPermuteInputsShuttle permute = RexPermuteInputsShuttle
                .of(Mappings.createShiftMapping(rightCount, leftCount, 0, rightCount));

            // Add inferred correlation conditions. Push down to base of its input label
            Optional.ofNullable(subquery.getInferredCorrelateCondition())
                .map(p -> p.pushDown(subquery.getInput(0)))
                .ifPresent(preds -> preds.getPredicates().forEach(p -> outPredicates.add(p.accept(permute))));

            // Add pull up predicates from subquery. Push down to base of its input label
            Optional.ofNullable(subquery.getPullUp())
                .map(p -> p.pushDown(subquery.getInput(0)))
                .ifPresent(preds -> preds.forEach(
                    (digest, pullUp) -> RelOptUtil.decomposeConjunction(pullUp.accept(permute), outPredicates)));

            // Generate permutations for pull up inferred predicates
            outFields.addAll(subqueryRowType.getFieldList());
            outInputBitSets.add(Pair.of(subquery, ImmutableBitSet.range(leftCount, leftCount + rightCount)));
            inputPermuteMap.put(subquery,
                RexPermuteInputsShuttle
                    .of(Mappings.createShiftMapping(leftCount + rightCount, 0, leftCount, rightCount)));
        }
        return inputPermuteMap;
    }

    /**
     * Prepare predicate inference for subquery
     *
     * @param wrapperLabel Subquery wrapper
     * @param predicates Original predicates
     * @param fields Column information of input labels
     * @param context ExtractorContext
     * @return SargableConditionInference object
     */
    public static SargableConditionInference infer(SubqueryWrapperLabel wrapperLabel, List<RexNode> predicates,
                                                   List<RelDataTypeField> fields, ExtractorContext context) {
        final RelOptCluster cluster = wrapperLabel.getRel().getCluster();
        final RelDataType rowType = RelUtils.buildRowType(fields, cluster);
        final DecorrelationShuttle decorrelationShuttle = new DecorrelationShuttle(wrapperLabel.getRel().getRowType());
        // Meaningless for subquery inference, use field count of row type of subquery wrapper
        final int leftCount = wrapperLabel.getRel().getRowType().getFieldCount();

        // Replace correlate id with column ref
        final List<RexNode> decorrelated = predicates.stream()
            .map(s -> s.accept(decorrelationShuttle))
            .collect(Collectors.toList());

        final List<RexNode> equalityPreds = new ArrayList<>();
        final List<RexNode> valuePreds = new ArrayList<>();
        classify(decorrelated, equalityPreds, valuePreds);

        final Map<String, RexNode> inferredEqualities = inferColumnEquality(equalityPreds,
            (l, r) -> buildEquality(l, r, fields, cluster.getRexBuilder(), context));

        // Infer
        return SargableConditionInference
            .create(cluster, inferredEqualities, deduplicate(valuePreds, context), rowType, leftCount);
    }

    /**
     * Prepare predicate inference for subquery
     *
     * @param wrapperLabel Subquery wrapper
     * @param predicates Original predicates
     * @param fields Column information of input labels
     * @param context ExtractorContext
     * @return SargableConditionInference object
     */
    public static SargableConditionInference infer(CorrelateLabel wrapperLabel, List<RexNode> predicates,
                                                   List<RelDataTypeField> fields, ExtractorContext context) {
        final RelOptCluster cluster = wrapperLabel.getRel().getCluster();
        final RelDataType rowType = RelUtils.buildRowType(fields, cluster);
        final DecorrelationShuttle decorrelationShuttle = new DecorrelationShuttle(wrapperLabel.left().getRowType());
        // Meaningless for subquery inference, use field count of row type of subquery wrapper
        final int leftCount = wrapperLabel.getRowType().getFieldCount() - 1;

        // Replace correlate id with column ref
        List<RexNode> toRemoved = predicates.stream().filter(rex -> RexUtil.findFieldAccessesDeep(rex).stream()
            .anyMatch(acc -> !((RexCorrelVariable) acc.getReferenceExpr()).getId().getName()
                .equals(wrapperLabel.getRel().getCorrelVariable()))).collect(Collectors.toList());
        predicates.removeAll(toRemoved);
        final List<RexNode> decorrelated = predicates.stream()
            .map(s -> s.accept(decorrelationShuttle))
            .collect(Collectors.toList());

        final List<RexNode> equalityPreds = new ArrayList<>();
        final List<RexNode> valuePreds = new ArrayList<>();
        classify(decorrelated, equalityPreds, valuePreds);

        final Map<String, RexNode> inferredEqualities = inferColumnEquality(equalityPreds,
            (l, r) -> buildEquality(l, r, fields, cluster.getRexBuilder(), context));

        // Infer
        return SargableConditionInference
            .create(cluster, inferredEqualities, deduplicate(valuePreds, context), rowType, leftCount);
    }

    /**
     * <pre>
     *     1. Push down(to base rowType of its input rel), shift and merge predicates of subquery
     *     2. Concat rowType of each subquery
     *     3. Generate permutation for each subquery for further push down
     * </pre>
     *
     * @param correlateLabel Subquery label
     * @param outFields Concat of all subquery fields
     * @param outInputBitSets Relation of column index and subquery
     * @param outPredicates Merged predicates
     * @return Permutation for each subquery for further push down
     */
    public static Map<? extends Label, RexPermuteInputsShuttle> mergePushdownSubqueryPredicates(
        CorrelateLabel correlateLabel,
        List<RelDataTypeField> outFields,
        List<Pair<Label, ImmutableBitSet>> outInputBitSets,
        List<RexNode> outPredicates) {
        // Put all columns and predicates together
        final Map<Label, RexPermuteInputsShuttle> inputPermuteMap = new HashMap<>();
        // Use the base rowType of subquery label
        final RelDataType mainRowType = correlateLabel.getRel().getInput(0).getRowType();
        final RelDataType subqueryRowType = correlateLabel.getRel().getInput(1).getRowType();
        final int leftCount = mainRowType.getFieldCount();
        final int rightCount = subqueryRowType.getFieldCount();

        boolean isLeftType = ((LogicalCorrelate) correlateLabel.getRel()).getJoinType() == SemiJoinType.LEFT;

        final RexPermuteInputsShuttle permute = RexPermuteInputsShuttle
            .of(Mappings.createShiftMapping(rightCount, isLeftType ? leftCount : leftCount + 1, 0, rightCount));

        // Add inferred correlation conditions. Push down to base of its input label
        Optional.ofNullable(correlateLabel.getInferredCorrelateCondition())
            .ifPresent(preds -> preds.getPredicates().forEach(p -> outPredicates.add(p)));

        // Add pull up predicates from subquery. Push down to base of its input label
        Optional.ofNullable(correlateLabel.right().getPullUp())
            .ifPresent(preds -> preds.forEach(
                (digest, pullUp) -> RelOptUtil.decomposeConjunction(pullUp.accept(permute), outPredicates)));

        // Generate permutations for pull up inferred predicates
        outInputBitSets.add(Pair.of(correlateLabel.right(), ImmutableBitSet
            .range(isLeftType ? (leftCount - 1 < 0 ? 0 : leftCount - 1) : leftCount,
                isLeftType ? leftCount + rightCount - 1 : leftCount + rightCount)));
        inputPermuteMap.put(correlateLabel.left(),
            RexPermuteInputsShuttle
                .of(LabelUtil.identityColumnMapping(leftCount)));
        inputPermuteMap.put(correlateLabel.right(),
            RexPermuteInputsShuttle
                .of(Mappings.createShiftMapping(isLeftType ? leftCount + rightCount : leftCount + rightCount + 1, 0,
                    isLeftType ? leftCount : leftCount + 1, rightCount)));

        return inputPermuteMap;
    }

    /**
     * Dispatch value predicates to label where its reference columns belong to
     *
     * @param valuePredicates Predicates to be dispatch
     * @param inputBitSets Column BitSet of each label
     * @param inputPermuteMap Permutation of each label
     * @param doPermute Do permutation or not
     * @return Dispatch result
     */
    public static Map<Label, List<RexNode>> dispatchAndPermute(Map<String, RexNode> valuePredicates,
                                                               List<Pair<Label, ImmutableBitSet>> inputBitSets,
                                                               Map<? extends Label, RexPermuteInputsShuttle> inputPermuteMap,
                                                               Predicate<Object> doPermute) {
        final Map<Label, List<RexNode>> classified = new HashMap<>();
        valuePredicates.forEach((key, pred) -> {
            final ImmutableBitSet inputBits = InputFinder.analyze(pred).inputBitSet.build();
            // Add predicates to label which its ref-column belongs to
            inputBitSets.stream()
                .filter(inputBitSet -> inputBitSet.getValue().contains(inputBits))
                .findAny()
                .map(u -> u.left)
                .ifPresent(label -> classified.compute(label, (k, v) -> {
                    v = Optional.ofNullable(v).orElseGet(ArrayList::new);
                    v.add(doPermute.test(label) ? pred.accept(inputPermuteMap.get(label)) : pred);
                    return v;
                }));
        });
        return classified;
    }

    public static int pos(RexNode expr) {
        if (expr instanceof RexInputRef) {
            return ((RexInputRef) expr).getIndex();
        }
        return -1;
    }

    public static List<RexNode> shiftRightPredicates(Collection<RexNode> rPreds, int nFieldsLeft, int nFieldsRight) {
        List<RexNode> result = new ArrayList<>();

        if (rPreds.isEmpty()) {
            return result;
        }

        if (rPreds.size() > 0) {
            Mappings.TargetMapping rightMapping = Mappings.createShiftMapping(nFieldsLeft + nFieldsRight,
                nFieldsLeft,
                0,
                nFieldsRight);
            RexPermuteInputsShuttle rexPermuteInputsShuttle = RexPermuteInputsShuttle.of(rightMapping);

            for (RexNode call : rPreds) {
                RexNode shiftedCall = call.accept(rexPermuteInputsShuttle);
                result.add(shiftedCall);
            } // end of for
        } // end of if

        return result;
    }

    /**
     * Merge predicate for pull up according to join type
     *
     * @param join Join rel node
     * @param leftPreds Predicates pull up from left
     * @param rightPreds Predicates pull up from right
     * @param inferred Inferred predicates
     * @return Predicates for pull up
     */
    public static List<RexNode> mergeInferredPredicateForPullUp(Join join, List<RexNode> leftPreds,
                                                                List<RexNode> rightPreds,
                                                                RelOptPredicateList inferred) {
        JoinRelType joinType = join.getJoinType();
        if (join instanceof SemiJoin && joinType == JoinRelType.LEFT) {
            joinType = JoinRelType.LEFT_SEMI;
        }

        int nFieldLeft = join.getLeft().getRowType().getFieldCount();
        int nFieldRight = join.getRight().getRowType().getFieldCount();

        final List<RexNode> inferredPreds = new ArrayList<>();

        switch (joinType) {
        case INNER:
            // Accept all pull-up predicates
            inferredPreds.addAll(leftPreds);
            inferredPreds.addAll(shiftRightPredicates(rightPreds, nFieldLeft, nFieldRight));
            // Accept all inferred predicates
            inferredPreds.addAll(inferred.leftInferredPredicates);
            inferredPreds.addAll(shiftRightPredicates(inferred.rightInferredPredicates, nFieldLeft, nFieldRight));
            break;
        case LEFT:
            // Accept all pull-up predicates
            inferredPreds.addAll(leftPreds);
            inferredPreds.addAll(shiftRightPredicates(rightPreds, nFieldLeft, nFieldRight));
            // Accept right inferred predicates
            inferredPreds.addAll(shiftRightPredicates(inferred.rightInferredPredicates, nFieldLeft, nFieldRight));
            break;
        case RIGHT:
            // Accept all pull-up predicates
            inferredPreds.addAll(leftPreds);
            inferredPreds.addAll(shiftRightPredicates(rightPreds, nFieldLeft, nFieldRight));
            // Accept left inferred predicates
            inferredPreds.addAll(inferred.leftInferredPredicates);
            break;
        case LEFT_SEMI:
        case SEMI:
            // SEMI JOIN only returns columns from left, so only predicates belong to left will be pulled up
            inferredPreds.addAll(leftPreds);
            // Accept left inferred predicates.
            inferredPreds.addAll(inferred.leftInferredPredicates);
            break;
        case ANTI:
        case FULL:
        default:
            break;
        }
        return inferredPreds;
    }

    /**
     * Convert RexFieldAccess to  RexInputRef
     */
    public static class DecorrelationShuttle extends RexShuttle {

        private final RelDataType rowType;

        public DecorrelationShuttle(RelDataType rowType) {
            this.rowType = rowType;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                final RelDataTypeField field = fieldAccess.getField();

//                if (rowType.getFieldCount() <= field.getIndex()) {
//                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "unknown column " + field.getName());
//                }
                return RexInputRef.of(field.getIndex(), fieldAccess.getReferenceExpr().getType());
            }
            return fieldAccess;
        }
    }

    public static boolean isEquality(RexNode condition) {
        if (condition.isA(SqlKind.EQUALS)) {
            final RexNode left = ((RexCall) condition).operands.get(0);
            final RexNode right = ((RexCall) condition).operands.get(1);

            if (left instanceof RexInputRef && right instanceof RexInputRef) {
                return true;
            }
        }

        return false;
    }

    public static void classify(Collection<RexNode> input, List<RexNode> outColumnEquality, List<RexNode> outOthers) {
        if (null == input) {
            return;
        }

        for (RexNode condition : input) {
            if (condition.isA(SqlKind.EQUALS)) {
                final RexNode left = ((RexCall) condition).operands.get(0);
                final RexNode right = ((RexCall) condition).operands.get(1);

                if (left instanceof RexInputRef && right instanceof RexInputRef) {
                    outColumnEquality.add(condition);
                } else {
                    outOthers.add(condition);
                }
            } else {
                outOthers.add(condition);
            }
        }
    }

    public static BitSet rexToBitSet(RexNode rex) {
        if (rex.isA(SqlKind.EQUALS)) {
            final int l = pos(((RexCall) rex).operands.get(0));
            final int r = pos(((RexCall) rex).operands.get(1));

            if (l > -1 && r > -1) {
                return BitSets.of(l, r);
            }
        }
        return null;
    }

    public static Pair<String, RexNode> buildEquality(int l, int r, List<RelDataTypeField> fieldList,
                                                      RexBuilder rexBuilder, ExtractorContext context) {
        final DigestCache digestCache = context.getDigestCache();
        final RexNode call = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            Arrays.asList(RexInputRef.of(l, fieldList), RexInputRef.of(r, fieldList)));
        return Pair.of(digestCache.digest(call), call);
    }

    public static List<RexNode> sortPredicates(List<RexNode> predicates) {
        return predicates.stream().sorted((o1, o2) -> {
            if (null == o1 && null == o2) {
                return 0;
            }

            if (null == o1 || null == o2) {
                // NULL LAST
                return null == o1 ? 1 : -1;
            }

            final boolean o1IsEquals = o1.isA(SqlKind.EQUALS);
            final boolean o2IsEquals = o2.isA(SqlKind.EQUALS);

            if (o1IsEquals && o2IsEquals) {
                return 0;
            }

            if (o1IsEquals || o2IsEquals) {
                // EQUALS FIRST
                return o1IsEquals ? -1 : 1;
            }

            final boolean o1IsGreater = o1.isA(SqlKind.GREATER_THAN) || o1.isA(SqlKind.GREATER_THAN_OR_EQUAL);
            final boolean o2IsGreater = o2.isA(SqlKind.GREATER_THAN) || o2.isA(SqlKind.GREATER_THAN_OR_EQUAL);

            if (o1IsGreater && o2IsGreater) {
                return 0;
            }

            if (o1IsGreater || o2IsGreater) {
                // GREATER FIRST
                return o1IsGreater ? -1 : 0;
            }

            final boolean o1IsLess = o1.isA(SqlKind.LESS_THAN) || o1.isA(SqlKind.LESS_THAN_OR_EQUAL);
            final boolean o2IsLess = o2.isA(SqlKind.LESS_THAN) || o2.isA(SqlKind.LESS_THAN_OR_EQUAL);

            if (o1IsLess && o2IsLess) {
                return 0;
            }

            if (o1IsLess || o2IsLess) {
                // GREATER/LESS FIRST
                return o1IsLess ? -1 : 0;
            }

            final boolean o1IsAndOr = o1.isA(SqlKind.AND) || o1.isA(SqlKind.OR);
            final boolean o2IsAndOr = o2.isA(SqlKind.AND) || o2.isA(SqlKind.OR);

            if (o1IsAndOr && o2IsAndOr) {
                return 0;
            }

            if (o1IsAndOr || o2IsAndOr) {
                // AND/OR LAST
                return o1IsAndOr ? 1 : -1;
            }

            return 0;
        }).collect(Collectors.toList());
    }
}
