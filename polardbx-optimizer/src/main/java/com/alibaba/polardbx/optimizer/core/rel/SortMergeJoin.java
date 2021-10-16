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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.MppConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.rel.RelCollations.containsOrderless;

public class SortMergeJoin extends Join implements PhysicalNode {
    //~ Instance fields --------------------------------------------------------

    private final boolean semiJoinDone;
    private final ImmutableList<RelDataTypeField> systemFieldList;

    private final List<Integer> leftColumns;

    private final List<Integer> rightColumns;

    private final RexNode otherCondition;

    private RelOptCost fixedCost;

    private final RelCollation collation;
    // only contains the equal part
    private JoinInfo joinInfo;
    //~ Constructors -----------------------------------------------------------

    public SortMergeJoin(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        RexNode condition,
        Set<CorrelationId> variablesSet,
        JoinRelType joinType,
        boolean semiJoinDone,
        ImmutableList<RelDataTypeField> systemFieldList,
        SqlNodeList hints,
        final List<Integer> leftColumns,
        final List<Integer> rightColumns,
        final RelCollation relCollation,
        final RexNode otherCondition
    ) {
        super(cluster, traitSet, left, right, condition, variablesSet, joinType, hints);
        assert traitSet.containsIfApplicable(DrdsConvention.INSTANCE)
            || traitSet.containsIfApplicable(MppConvention.INSTANCE);
        this.semiJoinDone = semiJoinDone;
        this.systemFieldList = Preconditions.checkNotNull(systemFieldList);
        this.leftColumns = leftColumns;
        this.rightColumns = rightColumns;
        this.collation = relCollation;
        this.otherCondition = otherCondition;
        this.joinInfo = JoinInfo.of(ImmutableIntList.copyOf(leftColumns), ImmutableIntList.copyOf(rightColumns));
    }

    public SortMergeJoin(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            relInput.getInputs().get(0),
            relInput.getInputs().get(1),
            relInput.getExpression("condition"),
            ImmutableSet.<CorrelationId>of(),
            JoinRelType.valueOf(relInput.getString("joinType")),
            null);
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
        this.otherCondition = relInput.getExpression("otherCondition");
        this.leftColumns = relInput.getIntegerList("leftColumns");
        this.rightColumns = relInput.getIntegerList("rightColumns");

        if (relInput.get("collation") == null) {
            this.collation = CBOUtil.createRelCollation(leftColumns);
        } else {
            this.collation = relInput.getCollation();
        }
        if (relInput.get("systemFields") == null) {
            this.systemFieldList = ImmutableList.of();
        } else {
            this.systemFieldList = (ImmutableList<RelDataTypeField>) relInput.get("systemFields");
        }
        this.semiJoinDone = relInput.getBoolean("semiJoinDone", false);
        this.joinInfo = JoinInfo.of(ImmutableIntList.copyOf(leftColumns), ImmutableIntList.copyOf(rightColumns));
    }

    public static SortMergeJoin create(RelTraitSet traitSet, RelNode left, RelNode right, RelCollation relCollation,
                                       RexNode condition,
                                       Set<CorrelationId> variablesSet,
                                       JoinRelType joinType, boolean semiJoinDone,
                                       ImmutableList<RelDataTypeField> systemFieldList, SqlNodeList hints,
                                       List<Integer> leftColumns, List<Integer> rightColumns,
                                       RexNode otherCondition) {
        final RelOptCluster cluster = left.getCluster();
        return new SortMergeJoin(cluster, traitSet, left, right, condition,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints, leftColumns, rightColumns,
            relCollation, otherCondition);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public SortMergeJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
                              RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        if (null != conditionExpr && null != this.condition) {
            final String originDigest = this.condition.toString();
            final String newDigest = conditionExpr.toString();
        }
        SortMergeJoin sortMergeJoin = new SortMergeJoin(getCluster(),
            traitSet, left, right, conditionExpr,
            variablesSet, joinType, semiJoinDone, systemFieldList, hints, leftColumns, rightColumns,
            collation, otherCondition);
        sortMergeJoin.setFixedCost(this.fixedCost);
        return sortMergeJoin;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        // Don't ever print semiJoinDone=false. This way, we
        // don't clutter things up in optimizers that don't use semi-joins.
        return super.explainTerms(pw)
            .itemIf("semiJoinDone", semiJoinDone, semiJoinDone)
            .item("leftColumns", leftColumns)
            .item("rightColumns", rightColumns)
            .itemIf("otherCondition", otherCondition, otherCondition != null)
            .item("collation", collation);
    }

    @Override
    public boolean isSemiJoinDone() {
        return semiJoinDone;
    }

    @Override
    public List<RelDataTypeField> getSystemFieldList() {
        return systemFieldList;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "SortMergeJoin");

        RexExplainVisitor visitor = new RexExplainVisitor(this);
        condition.accept(visitor);
        return pw.item("condition", visitor.toSqlString())
            .item("type", joinType.name().toLowerCase())
            .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    public List<Integer> getLeftColumns() {
        return leftColumns;
    }

    public List<Integer> getRightColumns() {
        return rightColumns;
    }

    public RexNode getOtherCondition() {
        return otherCondition;
    }

    public void setFixedCost(RelOptCost cost) {
        fixedCost = cost;
    }

    public RelOptCost getFixedCost() {
        return fixedCost;
    }

    public RelCollation getCollation() {
        return collation;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        if (fixedCost != null) {
            return fixedCost;
        }
        final double leftRowCount = mq.getRowCount(left);
        final double rightRowCount = mq.getRowCount(right);
        if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
            return planner.getCostFactory().makeHugeCost();
        }

        double mergeWeight = CostModelWeight.INSTANCE.getMergeWeight();
        double rowCount = leftRowCount + rightRowCount;
        double cpu = mergeWeight * (leftRowCount + rightRowCount);

        return planner.getCostFactory().makeCost(rowCount, cpu, 0, 0, 0);
    }

    @Override
    public RelNode passThrough(RelTraitSet required) {
        Pair<RelTraitSet, List<RelTraitSet>> p = passThroughTraits(required);
        if (p == null) {
            return null;
        }
        int size = getInputs().size();
        assert size == p.right.size();
        List<RelNode> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            RelNode n = RelOptRule.convert(getInput(i), p.right.get(i));
            list.add(n);
        }

        List<RelFieldCollation> leftRelFieldCollationList = p.right.get(0).getCollation().getFieldCollations();
        List<RelFieldCollation> rightRelFieldCollationList = p.right.get(1).getCollation().getFieldCollations();

        List<RelFieldCollation> relFieldCollationList = new ArrayList<>();

        List<Integer> newLeftColumns = new ArrayList<>();
        List<Integer> newRightColumns = new ArrayList<>();

        for (int i = 0; i < Math.min(leftRelFieldCollationList.size(), rightRelFieldCollationList.size()); i++) {
            relFieldCollationList.add(leftRelFieldCollationList.get(i));
            newLeftColumns.add(leftRelFieldCollationList.get(i).getFieldIndex());
            newRightColumns.add(rightRelFieldCollationList.get(i).getFieldIndex());
        }
        RelCollation collation = RelCollations.of(relFieldCollationList);

        return new SortMergeJoin(getCluster(), p.left, list.get(0), list.get(1), condition, variablesSet, joinType,
            semiJoinDone, systemFieldList, hints, newLeftColumns, newRightColumns, collation, otherCondition);
    }

    /**
     * Pass collations through can have three cases:
     * 1. If sort keys are equal to either left join keys, or right join keys,
     * collations can be pushed to both join sides with correct mappings.
     * For example, for the query
     * select * from foo join bar on foo.a=bar.b order by foo.a desc
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc)
     * join
     * (select * from bar order by bar.b desc)
     * <p>
     * 2. If sort keys are sub-set of either left join keys, or right join keys,
     * collations have to be extended to cover all joins keys before passing through,
     * because merge join requires all join keys are sorted.
     * For example, for the query
     * select * from foo join bar
     * on foo.a=bar.b and foo.c=bar.d
     * order by foo.a desc
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc, foo.c)
     * join
     * (select * from bar order by bar.b desc, bar.d)
     * <p>
     * 3. If sort keys are super-set of either left join keys, or right join keys,
     * but not both, collations can be completely passed to the join key whose join
     * keys match the prefix of collations. Meanwhile, partial mapped collations can
     * be passed to another join side to make sure join keys are sorted.
     * For example, for the query
     * select * from foo join bar
     * on foo.a=bar.b and foo.c=bar.d
     * order by foo.a desc, foo.c desc, foo.e
     * after traits pass through it will be equivalent to
     * select * from
     * (select * from foo order by foo.a desc, foo.c desc, foo.e)
     * join
     * (select * from bar order by bar.b desc, bar.d desc)
     */
    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
        final RelTraitSet required) {
        return sortMergeJoinPassThroughTraits(required, this, joinInfo);
    }

    public static Pair<RelTraitSet, List<RelTraitSet>> sortMergeJoinPassThroughTraits(final RelTraitSet required,
                                                                                      Join sortMergeJoin,
                                                                                      JoinInfo joinInfo) {
        if (required.getConvention() != sortMergeJoin.getConvention()) {
            return null;
        }

        if (required.getConvention() == MppConvention.INSTANCE) {
            return null;
        }

        final RelDistribution leftDistribution = RelDistributions.ANY;
        final RelDistribution rightDistribution = RelDistributions.ANY;

        // Required collation keys can be subset or superset of merge join keys.
        RelCollation collation = getCollation(required);
        int leftInputFieldCount = sortMergeJoin.getLeft().getRowType().getFieldCount();

        List<Integer> reqKeys = RelCollations.ordinals(collation);
        List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
        List<Integer> rightKeys =
            joinInfo.rightKeys.incr(leftInputFieldCount).toIntegerList();

        ImmutableBitSet reqKeySet = ImmutableBitSet.of(reqKeys);
        ImmutableBitSet leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
        ImmutableBitSet rightKeySet = ImmutableBitSet.of(joinInfo.rightKeys)
            .shift(leftInputFieldCount);

        if (reqKeySet.equals(leftKeySet)) {
            // if sort keys equal to left join keys, we can pass through all collations directly.
            Mappings.TargetMapping mapping =
                buildMapping(true, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation rightCollation = collation.apply(mapping);
            return Pair.of(
                required, ImmutableList.of(required.replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        } else if (containsOrderless(leftKeys, collation)) {
            // if sort keys are subset of left join keys, we can extend collations to make sure all join
            // keys are sorted.
            collation = extendCollation(collation, leftKeys);
            Mappings.TargetMapping mapping =
                buildMapping(true, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation rightCollation = collation.apply(mapping);
            return Pair.of(
                required, ImmutableList.of(required.replace(collation).replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        } else if (containsOrderless(collation, leftKeys)
            && reqKeys.stream().allMatch(i -> i < leftInputFieldCount)) {
            // if sort keys are superset of left join keys, and left join keys is prefix of sort keys
            // (order not matter), also sort keys are all from left join input.
            Mappings.TargetMapping mapping =
                buildMapping(true, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation rightCollation =
                RexUtil.apply(
                    mapping,
                    intersectCollationAndJoinKey(collation, joinInfo.leftKeys));
            return Pair.of(
                required, ImmutableList.of(required.replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        } else if (reqKeySet.equals(rightKeySet)) {
            // if sort keys equal to right join keys, we can pass through all collations directly.
            RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
            Mappings.TargetMapping mapping =
                buildMapping(false, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation leftCollation = rightCollation.apply(mapping);
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation).replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        } else if (containsOrderless(rightKeys, collation)) {
            // if sort keys are subset of right join keys, we can extend collations to make sure all join
            // keys are sorted.
            collation = extendCollation(collation, rightKeys);
            RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
            Mappings.TargetMapping mapping =
                buildMapping(false, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation leftCollation = RexUtil.apply(mapping, rightCollation);
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation).replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        } else if (containsOrderless(collation, rightKeys)
            && reqKeys.stream().allMatch(i -> i >= leftInputFieldCount)) {
            // if sort keys are superset of right join keys, and right join keys is prefix of sort keys
            // (order not matter), also sort keys are all from right join input.
            RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
            Mappings.TargetMapping mapping =
                buildMapping(false, joinInfo, sortMergeJoin.getLeft(), sortMergeJoin.getRight());
            RelCollation leftCollation =
                RexUtil.apply(
                    mapping,
                    intersectCollationAndJoinKey(rightCollation, joinInfo.rightKeys));
            return Pair.of(
                required, ImmutableList.of(
                    required.replace(leftCollation).replace(leftDistribution),
                    required.replace(rightCollation).replace(rightDistribution)));
        }

        return null;
    }

    private static Mappings.TargetMapping buildMapping(boolean left2Right, JoinInfo joinInfo, RelNode left,
                                                       RelNode right) {
        ImmutableIntList sourceKeys = left2Right ? joinInfo.leftKeys : joinInfo.rightKeys;
        ImmutableIntList targetKeys = left2Right ? joinInfo.rightKeys : joinInfo.leftKeys;
        Map<Integer, Integer> keyMap = new HashMap<>();
        for (int i = 0; i < joinInfo.leftKeys.size(); i++) {
            keyMap.put(sourceKeys.get(i), targetKeys.get(i));
        }

        Mappings.TargetMapping mapping = Mappings.target(keyMap,
            (left2Right ? left : right).getRowType().getFieldCount(),
            (left2Right ? right : left).getRowType().getFieldCount());
        return mapping;
    }

    private static RelCollation getCollation(RelTraitSet traits) {
        return requireNonNull(traits.getCollation(),
            () -> "no collation trait in " + traits);
    }

    /**
     * This function will remove collations that are not defined on join keys.
     * For example:
     * select * from
     * foo join bar
     * on foo.a = bar.a and foo.c=bar.c
     * order by bar.a, bar.c, bar.b;
     * <p>
     * The collation [bar.a, bar.c, bar.b] can be pushed down to bar. However, only
     * [a, c] can be pushed down to foo. This function will help create [a, c] for foo by removing
     * b from the required collation, because b is not defined on join keys.
     *
     * @param collation collation defined on the JOIN
     * @param joinKeys the join keys
     */
    private static RelCollation intersectCollationAndJoinKey(
        RelCollation collation, ImmutableIntList joinKeys) {
        List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (RelFieldCollation rf : collation.getFieldCollations()) {
            if (joinKeys.contains(rf.getFieldIndex())) {
                fieldCollations.add(rf);
            }
        }
        return RelCollations.of(fieldCollations);
    }

    /**
     * This function extends collation by appending new collation fields defined on keys.
     */
    private static RelCollation extendCollation(RelCollation collation, List<Integer> keys) {
        List<RelFieldCollation> fieldsForNewCollation = new ArrayList<>(keys.size());
        fieldsForNewCollation.addAll(collation.getFieldCollations());

        ImmutableBitSet keysBitset = ImmutableBitSet.of(keys);
        ImmutableBitSet colKeysBitset = ImmutableBitSet.of(collation.getKeys());
        ImmutableBitSet exceptBitset = keysBitset.except(colKeysBitset);
        for (Integer i : exceptBitset) {
            fieldsForNewCollation.add(new RelFieldCollation(i));
        }
        return RelCollations.of(fieldsForNewCollation);
    }

    @Override
    public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(final RelTraitSet childTraits, final int childId) {
        return sortMergeJoinDeriveTraits(childTraits, childId, joinInfo, getTraitSet(), left, right);
    }

    public static Pair<RelTraitSet, List<RelTraitSet>> sortMergeJoinDeriveTraits(final RelTraitSet childTraits,
                                                                                 final int childId, JoinInfo joinInfo,
                                                                                 final RelTraitSet joinTraits,
                                                                                 RelNode left, RelNode right) {
        if (childTraits.getConvention() == MppConvention.INSTANCE) {
            RelDistribution childDistribution = childTraits.getDistribution();
            RelCollation childCollation = childTraits.getCollation();

            final int keyCount = joinInfo.leftKeys.size();
            final int colCount = childCollation.getFieldCollations().size();
            if (colCount < keyCount || keyCount == 0) {
                return null;
            }

            if (colCount > keyCount) {
                childCollation = RelCollations.of(childCollation.getFieldCollations().subList(0, keyCount));
            }

            ImmutableIntList sourceKeys = childId == 0 ? joinInfo.leftKeys : joinInfo.rightKeys;
            ImmutableBitSet keySet = ImmutableBitSet.of(sourceKeys);
            ImmutableBitSet childCollationKeys = ImmutableBitSet.of(
                RelCollations.ordinals(childCollation));
            if (!childCollationKeys.equals(keySet)) {
                return null;
            }

            Mappings.TargetMapping mapping = buildMapping(childId == 0, joinInfo, left, right);
            RelCollation targetCollation = childCollation.apply(mapping);

            if (childDistribution == RelDistributions.ANY
                    || childDistribution == RelDistributions.BROADCAST_DISTRIBUTED) {
                return null;
            }
            if (childId == 0 && right.getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                RelTraitSet joinTraitSet = joinTraits.replace(childDistribution).replace(childCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(childTraits, right.getTraitSet().replace(targetCollation)));
            } else if (childId == 1 && left.getTraitSet().getDistribution() == RelDistributions.BROADCAST_DISTRIBUTED) {
                int leftFieldCount = left.getRowType().getFieldCount();
                int rightFiledCount = right.getRowType().getFieldCount();
                Mappings.TargetMapping mapping2 =
                    Mappings.createShiftMapping(rightFiledCount, leftFieldCount, 0, rightFiledCount);
                RelTraitSet joinTraitSet =
                    joinTraits.replace(childDistribution.apply(mapping2)).replace(targetCollation);
                return org.apache.calcite.util.Pair.of(joinTraitSet,
                    ImmutableList.of(left.getTraitSet().replace(targetCollation), childTraits.replace(childCollation)));
            }
        }
        return null;
    }

    @Override
    public DeriveMode getDeriveMode() {
        return DeriveMode.BOTH;
    }

    public JoinInfo getJoinInfo() {
        return joinInfo;
    }
}
