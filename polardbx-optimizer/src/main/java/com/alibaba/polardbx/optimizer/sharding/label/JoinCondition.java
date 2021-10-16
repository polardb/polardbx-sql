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

package com.alibaba.polardbx.optimizer.sharding.label;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

/**
 * @author chenmo.cm
 */
public class JoinCondition {

    public static final JoinCondition EMPTY = new JoinCondition();

    public final List<RexNode> left;
    public final List<RexNode> right;
    public final List<RexNode> leftEquality;
    public final List<RexNode> rightEquality;
    public final List<RexNode> crossEquality;
    public final List<RexNode> other;

    private Map<String, RexNode> leftMap = new HashMap<>();
    private Map<String, RexNode> rightMap = new HashMap<>();
    private Map<String, RexNode> leftEqualityMap = new HashMap<>();
    private Map<String, RexNode> rightEqualityMap = new HashMap<>();
    private Map<String, RexNode> crossEqualityMap = new HashMap<>();
    private Map<String, RexNode> otherMap = new HashMap<>();

    public static JoinCondition create(Join join, List<RexNode> onConditions, ExtractorContext context) {

        final Map<String, RexNode> deduplicated = PredicateUtil.deduplicate(onConditions, context);

        return classify(join, deduplicated.values(), new JoinCondition());
    }

    public static JoinCondition classify(Join join, Collection<RexNode> conditions, JoinCondition outOnCondition) {
        if (null == conditions) {
            return outOnCondition;
        }

        final int nLeft = join.getLeft().getRowType().getFieldCount();
        final int nRight = join.getRight().getRowType().getFieldCount();

        final ImmutableBitSet leftBitSet = ImmutableBitSet.builder().set(0, nLeft).build();
        final ImmutableBitSet rightBitSet = ImmutableBitSet.builder().set(nLeft, nLeft + nRight).build();

        for (RexNode condition : conditions) {
            final ImmutableBitSet inputBitSet = InputFinder.analyze(condition).inputBitSet.build();

            if (leftBitSet.contains(inputBitSet)) {
                if (PredicateUtil.isEquality(condition)) {
                    outOnCondition.leftEquality.add(condition);
                } else {
                    outOnCondition.left.add(condition);
                }

                continue;
            }

            if (rightBitSet.contains(inputBitSet)) {

                if (PredicateUtil.isEquality(condition)) {
                    outOnCondition.rightEquality.add(condition);
                } else {
                    outOnCondition.right.add(condition);
                }

                continue;
            }

            if (PredicateUtil.isEquality(condition)) {
                outOnCondition.crossEquality.add(condition);
                continue;
            }

            outOnCondition.other.add(condition);
        }

        return outOnCondition;
    }

    public JoinCondition() {
        this.left = new ArrayList<>();
        this.right = new ArrayList<>();
        this.leftEquality = new ArrayList<>();
        this.rightEquality = new ArrayList<>();
        this.crossEquality = new ArrayList<>();
        this.other = new ArrayList<>();
    }

    protected JoinCondition(List<RexNode> left, List<RexNode> right,
                            List<RexNode> leftEquality, List<RexNode> rightEquality, List<RexNode> crossEquality,
                            List<RexNode> other) {
        this.left = left;
        this.right = right;
        this.leftEquality = leftEquality;
        this.rightEquality = rightEquality;
        this.crossEquality = crossEquality;
        this.other = other;
    }

    public enum PredicateType {
        /**
         * Predicate with two column reference connected by Operator EQUAL
         */
        COLUMN_EQUALITY,
        /**
         * Predicate which is not COLUMN_EQUALITY
         */
        SARGABLE,
        ALL;

        public boolean columnEquality() {
            return this == COLUMN_EQUALITY || this == ALL;
        }

        public boolean sargable() {
            return this == SARGABLE || this == ALL;
        }
    }

    public List<RexNode> fromNonNullGenerateSide(JoinRelType joinRelType, PredicateType type) {

        final List<RexNode> result = new ArrayList<>();
        switch (joinRelType) {
        case LEFT:
            if (type.columnEquality()) {
                result.addAll(this.leftEquality);
            }
            if (type.sargable()) {
                result.addAll(this.left);
            }
            break;
        case RIGHT:
            if (type.columnEquality()) {
                result.addAll(this.rightEquality);
            }
            if (type.sargable()) {
                result.addAll(this.right);
            }
            break;
        default:
        }

        return result;
    }

    public List<RexNode> forMoveAround(Join join, PredicateType type) {
        JoinRelType joinType = join.getJoinType();

        if (join instanceof SemiJoin && joinType == JoinRelType.LEFT) {
            joinType = JoinRelType.LEFT_SEMI;
        }

        final List<RexNode> result = new ArrayList<>();
        switch (joinType) {
        case INNER:
            if (type.columnEquality()) {
                result.addAll(this.crossEquality);
                result.addAll(this.leftEquality);
                result.addAll(this.rightEquality);
            }

            if (type.sargable()) {
                result.addAll(this.left);
                result.addAll(this.right);
            }
            break;
        case LEFT:
            if (type.columnEquality()) {
                result.addAll(this.crossEquality);
                result.addAll(this.rightEquality);
            }
            if (type.sargable()) {
                result.addAll(this.right);
            }
            break;
        case RIGHT:
            if (type.columnEquality()) {
                result.addAll(this.crossEquality);
                result.addAll(this.leftEquality);
            }
            if (type.sargable()) {
                result.addAll(this.left);
            }
            break;
        case SEMI:
        case LEFT_SEMI:
            if (type.sargable()) {
                result.addAll(this.left);
            }
        case ANTI:
            if (type.columnEquality()) {
                result.addAll(this.crossEquality);
            }
            break;
        default:
            break;
        }
        return result;
    }

    public Map<String, RexNode> forMoveAround(Join join, PredicateType type, ExtractorContext context) {
        JoinRelType joinType = join.getJoinType();

        if (join instanceof SemiJoin && joinType == JoinRelType.LEFT) {
            joinType = JoinRelType.LEFT_SEMI;
        }

        final Map<String, RexNode> result = new HashMap<>();
        switch (joinType) {
        case INNER:
            if (type.columnEquality()) {
                result.putAll(this.getCrossEqualityMap(context));
                result.putAll(this.getLeftEqualityMap(context));
                result.putAll(this.getRightEqualityMap(context));
            }

            if (type.sargable()) {
                result.putAll(this.getLeftMap(context));
                result.putAll(this.getRightMap(context));
            }
            break;
        case LEFT:
            if (type.columnEquality()) {
                result.putAll(this.getCrossEqualityMap(context));
                result.putAll(this.getRightEqualityMap(context));
            }
            if (type.sargable()) {
                result.putAll(this.getRightMap(context));
            }
            break;
        case RIGHT:
            if (type.columnEquality()) {
                result.putAll(this.getCrossEqualityMap(context));
                result.putAll(this.getLeftEqualityMap(context));
            }
            if (type.sargable()) {
                result.putAll(this.getLeftMap(context));
            }
            break;
        case SEMI:
        case LEFT_SEMI:
            if (type.sargable()) {
                result.putAll(this.getLeftMap(context));
            }
        case ANTI:
            if (type.columnEquality()) {
                result.putAll(this.getCrossEqualityMap(context));
            }
            break;
        default:
            break;
        }
        return result;
    }

    public Map<String, RexNode> all(PredicateType type, ExtractorContext context) {
        final Map<String, RexNode> result = new HashMap<>();

        if (type.columnEquality()) {
            result.putAll(this.getCrossEqualityMap(context));
            result.putAll(this.getLeftEqualityMap(context));
            result.putAll(this.getRightEqualityMap(context));
        }

        if (type.sargable()) {
            result.putAll(this.getLeftMap(context));
            result.putAll(this.getRightMap(context));
        }

        return result;
    }

    public Map<String, RexNode> getLeftMap(ExtractorContext context) {
        if (this.leftMap.isEmpty() && !this.left.isEmpty()) {
            this.leftMap = new HashMap<>(PredicateUtil.deduplicate(this.left, context));
        }

        return this.leftMap;
    }

    public Map<String, RexNode> getRightMap(ExtractorContext context) {
        if (this.rightMap.isEmpty() && !this.right.isEmpty()) {
            this.rightMap = new HashMap<>(PredicateUtil.deduplicate(this.right, context));
        }

        return this.rightMap;
    }

    public Map<String, RexNode> getLeftEqualityMap(ExtractorContext context) {
        if (this.leftEqualityMap.isEmpty() && !this.leftEquality.isEmpty()) {
            this.leftEqualityMap = new HashMap<>(PredicateUtil.deduplicate(this.leftEquality, context));
        }

        return this.leftEqualityMap;
    }

    public Map<String, RexNode> getRightEqualityMap(ExtractorContext context) {
        if (this.rightEqualityMap.isEmpty() && !this.rightEquality.isEmpty()) {
            this.rightEqualityMap = new HashMap<>(PredicateUtil.deduplicate(this.rightEquality, context));
        }

        return this.rightEqualityMap;
    }

    public Map<String, RexNode> getCrossEqualityMap(ExtractorContext context) {
        if (this.crossEqualityMap.isEmpty() && !this.crossEquality.isEmpty()) {
            this.crossEqualityMap = PredicateUtil.deduplicate(this.crossEquality, context);
        }

        return this.crossEqualityMap;
    }

    public Map<String, RexNode> getOtherMap(ExtractorContext context) {
        if (this.otherMap.isEmpty() && !this.other.isEmpty()) {
            this.otherMap = new HashMap<>(PredicateUtil.deduplicate(this.other, context));
        }

        return this.otherMap;
    }

    public Map<String, RexNode> getLeftMap() {
        return leftMap;
    }

    public Map<String, RexNode> getRightMap() {
        return rightMap;
    }

    public Map<String, RexNode> getLeftEqualityMap() {
        return leftEqualityMap;
    }

    public Map<String, RexNode> getRightEqualityMap() {
        return rightEqualityMap;
    }

    public Map<String, RexNode> getCrossEqualityMap() {
        return crossEqualityMap;
    }

    public Map<String, RexNode> getOtherMap() {
        return otherMap;
    }
}
