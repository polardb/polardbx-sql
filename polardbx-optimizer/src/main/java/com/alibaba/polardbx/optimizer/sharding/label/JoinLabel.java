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

import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author chenmo.cm
 */
public class JoinLabel extends AbstractLabel {

    /**
     * Filtered predicates from ON clause
     */
    private JoinCondition joinCondition;
    /**
     * Predicates inferred from ON clause
     */
    private PredicateNode valuePredicates;

    protected JoinLabel(@Nonnull Join rel, Label left, Label right, ExtractorContext context) {
        super(LabelType.JOIN, rel, ImmutableList.of(left, right));
    }

    public JoinLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType, Mapping columnMapping,
                     RelDataType currentBaseRowType, PredicateNode pullUp, PredicateNode pushdown,
                     PredicateNode[] columnConditionMap, List<PredicateNode> predicates, JoinCondition joinCondition) {
        super(type,
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
        this.joinCondition = joinCondition;
    }

    public static JoinLabel create(@Nonnull Join join, JoinCondition joinCondition, Label left, Label right,
                                   ExtractorContext context) {
        final JoinLabel joinLabel = new JoinLabel(join, left, right, context);

        joinLabel.setJoinCondition(joinCondition);

        return joinLabel;
    }

    public Label left() {
        return getInput(0);
    }

    public Label right() {
        return getInput(1);
    }

    @Override
    public RelDataType deriveRowType() {
        final RelDataTypeFactory factory = this.rel.getCluster().getTypeFactory();

        final RelDataType leftRowType = left().deriveRowType();
        final RelDataType rightRowType = right().deriveRowType();

        final List<RelDataTypeField> base = ImmutableList.<RelDataTypeField>builder()
            .addAll(leftRowType.getFieldList())
            .addAll(rightRowType.getFieldList())
            .build();
        if (this.rel instanceof HashGroupJoin) {
            return deriveGroupRowType(factory, base, ((HashGroupJoin) this.rel).indicator,
                ((HashGroupJoin) this.rel).getGroupSet(), ((HashGroupJoin) this.rel).getGroupSets(),
                ((HashGroupJoin) this.rel).getAggCallList());
        }

        return super.deriveRowType(factory, base);
    }

    public static RelDataType deriveGroupRowType(RelDataTypeFactory typeFactory,
                                                 final List<RelDataTypeField> fieldList, boolean indicator,
                                                 ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                                 final List<AggregateCall> aggCalls) {
        final List<Integer> groupList = groupSet.asList();
        assert groupList.size() == groupSet.cardinality();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        final Set<String> containedNames = Sets.newHashSet();
        for (int groupKey : groupList) {
            final RelDataTypeField field = fieldList.get(groupKey);
            containedNames.add(field.getName());
            builder.add(field);
            if (groupSets != null && !allContain(groupSets, groupKey)) {
                builder.nullable(true);
            }
        }
        if (indicator) {
            for (int groupKey : groupList) {
                final RelDataType booleanType =
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
                final String base = "i$" + fieldList.get(groupKey).getName();
                String name = base;
                int i = 0;
                while (containedNames.contains(name)) {
                    name = base + "_" + i++;
                }
                containedNames.add(name);
                builder.add(name, booleanType);
            }
        }
        for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
            final String base;
            if (aggCall.e.name != null) {
                base = aggCall.e.name;
            } else {
                base = "$f" + (groupList.size() + aggCall.i);
            }
            String name = base;
            int i = 0;
            while (containedNames.contains(name)) {
                name = base + "_" + i++;
            }
            containedNames.add(name);
            builder.add(name, aggCall.e.type);
        }
        return builder.build();
    }

    private static boolean allContain(List<ImmutableBitSet> groupSets,
                                      int groupKey) {
        for (ImmutableBitSet groupSet : groupSets) {
            if (!groupSet.get(groupKey)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new JoinLabel(getType(),
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates,
            joinCondition);
    }

    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public String toString() {
        return super.toString() + " # " + left().toString() + " # " + right().toString();
    }

    public JoinCondition getJoinCondition() {
        return joinCondition;
    }

    private void setJoinCondition(JoinCondition joinCondition) {
        this.joinCondition = joinCondition;
    }

    public PredicateNode getValuePredicates() {
        return valuePredicates;
    }

    public void setValuePredicates(PredicateNode valuePredicates) {
        this.valuePredicates = valuePredicates;
    }

    @Override
    public Label filter(RexNode predicate, Filter filter,
                        ExtractorContext context) {

        final Join join = this.getRel();

        final List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        final List<RexNode> originFilters = RelOptUtil.conjunctions(predicate);
        final List<RexNode> aboveFilters = RelOptUtil.conjunctions(predicate);
        final ImmutableList<RexNode> origAboveFilters = ImmutableList.copyOf(aboveFilters);

        JoinRelType joinType = join.getJoinType();
        if (!origAboveFilters.isEmpty() && joinType != JoinRelType.INNER && !(join instanceof SemiJoin)) {
            joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
        }

        final List<RexNode> leftFilters = new ArrayList<>();
        final List<RexNode> rightFilters = new ArrayList<>();
        final List<RexNode> pushFilters = new ArrayList<>();
        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        if (RelOptUtil.classifyFilters(join,
            aboveFilters,
            joinType,
            true,
            !joinType.generatesNullsOnLeft(),
            !joinType.generatesNullsOnRight(),
            joinFilters,
            leftFilters,
            rightFilters)) {
            for (RexNode rexNode : originFilters) {
                if (!aboveFilters.contains(rexNode)) {
                    pushFilters.add(rexNode);
                }
            }
            final Map<String, RexNode> deduplicatedFilters = PredicateUtil.deduplicate(pushFilters, context);
            this.predicates.add(new PredicateNode(this.clone(), null, deduplicatedFilters, context));
        }
        return this;
    }
}
