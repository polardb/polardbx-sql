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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelOptUtil.EquivalenceFinder;
import org.apache.calcite.plan.RelOptUtil.InputFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.calcite.util.mapping.Mappings.NoElementException;
import org.apache.calcite.util.mapping.Mappings.TargetMapping;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.IntStream;

/**
 * Planner rule that get predicates from on a
 * {@link org.apache.calcite.rel.core.Filter} and creates
 * {@link org.apache.calcite.rel.core.Filter}s if those predicates can be pushed
 * to {@link TableLookup}'s inputs. If
 * any of predicates cannot be pushed to left input,
 * {@link TableLookup#relPushedToPrimary}
 * will be enabled
 *
 * @author chenmo.cm
 */
public class FilterTableLookupTransposeRule extends TddlFilterJoinRule {

    public static final FilterTableLookupTransposeRule INSTANCE = new FilterTableLookupTransposeRule();

    public FilterTableLookupTransposeRule() {
        super(operand(Filter.class, operand(LogicalTableLookup.class, any())),
            "FilterTableLookupTransposeRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalTableLookup tableLookup = call.rel(1);
        // this rule are used in rbo, do not specific convention
        return tableLookup.getJoin().getJoinType() == JoinRelType.INNER;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final LogicalTableLookup tableLookup = call.rel(1);
        perform(call, filter, tableLookup);
    }

    private boolean leftOnly(RexNode condition, ImmutableBitSet leftInputSet) {
        final InputFinder inputFinder = InputFinder.analyze(condition);
        final ImmutableBitSet inputBits = inputFinder.inputBitSet.build();

        return leftInputSet.contains(inputBits);
    }

    protected void perform(RelOptRuleCall call, Filter filter, LogicalTableLookup tableLookup) {
        final LogicalProject project = tableLookup.getProject();
        final LogicalJoin join = tableLookup.getJoin();

        if (RexOver.containsOver(project.getProjects(), null)) {
            // In general a filter cannot be pushed below a windowing
            // calculation.
            // Applying the filter before the aggregation function changes
            // the results of the windowing invocation.
            //
            // When the filter is on the PARTITION BY expression of the OVER
            // clause it can be pushed down. For now we don't support this.
            return;
        }

        if (RexUtil.containsCorrelation(filter.getCondition())
            || (null != filter.getVariablesSet() && filter.getVariablesSet().size() > 0)) {
            // If there is a correlation condition anywhere in the filter, don't
            // push this filter past project since in some cases it can prevent
            // a Correlate from being de-correlated.
            return;
        }

        if (checkScalarExists(filter)) {
            return;
        }

        /**
         * filter project transpose
         */

        final RelBuilder relBuilder = call.builder();
        final ImmutableBitSet leftBitmap = ImmutableBitSet.range(0, join.getLeft().getRowType().getFieldCount());

        final List<RexNode> primaryPredicates = new ArrayList<>();
        final List<RexNode> indexPredicates = new ArrayList<>();

        final List<RexNode> predicates = RelOptUtil.conjunctions(filter.getCondition());
        for (RexNode predicate : predicates) {
            // convert the filter to one that references the child of the project
            RexNode pushed = RelOptUtil.pushPastProject(predicate, project);
            RexUtil.SubQueryCounter subQueryCounter = new RexUtil.SubQueryCounter();
            pushed.accept(subQueryCounter);
            if (subQueryCounter.getSubqueryCount() > 0) {
                return;
            }

            // simplify new condition
            final RelOptPredicateList preds = RelOptPredicateList.EMPTY;
            final RexSimplify simplify = new RexSimplify(relBuilder.getRexBuilder(), preds, false, RexUtil.EXECUTOR);
            pushed = simplify.removeNullabilityCast(pushed);

            // Transpose filter with table lookup only If filter can be pushed to index scan,
            if (leftOnly(pushed, leftBitmap)) {
                indexPredicates.add(pushed);
            }

            primaryPredicates.add(pushed);
        }

        /**
         * filter join transpose
         */
        final List<RexNode> leftFilters = new ArrayList<>(indexPredicates);
        final List<RexNode> rightFilters = new ArrayList<>();
        final RelMetadataQuery mq = tableLookup.getCluster().getMetadataQuery();

        RelNode leftRel = join.getLeft();
        RelNode rightRel = join.getRight();
        final RelOptTable indexTable = tableLookup.getIndexTable();
        final RelOptTable primaryTable = tableLookup.getPrimaryTable();
        final int leftCount = leftRel.getRowType().getFieldCount();
        final int rightCount = rightRel.getRowType().getFieldCount();

        final SortedMap<Integer, BitSet> equivalence = Maps.newTreeMap();
        final EquivalenceFinder ef = new EquivalenceFinder(equivalence);
        final List<RexNode> joinConditions = RelOptUtil.conjunctions(join.getCondition());

        // get column equivalence from join condition
        joinConditions.forEach(condition -> condition.accept(ef));

        final PlannerContext context = PlannerContext.getPlannerContext(call);
        if (context.getParamManager().getBoolean(ConnectionParams.REPLICATE_FILTER_TO_PRIMARY)) {
            // get column equivalence for index columns
            buildColumnEquivalence(mq, leftRel, rightRel, indexTable, primaryTable, equivalence);
        }

        final TargetMapping primaryTarget = buildPrimaryMapping(leftCount, rightCount, equivalence);

        // copy filters to right
        for (RexNode f : primaryPredicates) {
            try {
                rightFilters.add(f.accept(new RexPermuteInputsShuttle(primaryTarget, rightRel)));
            } catch (NoElementException e) {
                // filter should not be copied
            }
        }

        if (!leftFilters.isEmpty()) {
            leftRel = relBuilder.push(join.getLeft()).filter(leftFilters).build();
        }

        if (!rightFilters.isEmpty()) {
            rightRel = relBuilder.push(join.getRight()).filter(rightFilters).build();
        }

        final LogicalTableLookup newTableLookup = tableLookup.copy(
            join.getJoinType(),
            join.getCondition(),
            project.getProjects(),
            project.getRowType(),
            leftRel,
            rightRel,
            tableLookup.isRelPushedToPrimary() || primaryPredicates.size() > indexPredicates.size());
        if (!leftFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, leftRel);
        }
        if (!rightFilters.isEmpty()) {
            call.getPlanner().onCopy(filter, rightRel);
        }

        call.transformTo(newTableLookup);
    }

    /**
     * Build TargetMapping between join and primary table as described in
     * equivalence
     *
     * @param leftCount column count of left child of table lookup
     * @param rightCount column count of right child of table lookup
     * @param equivalence column equivalence
     * @return mapping for coping predicates
     */
    private TargetMapping buildPrimaryMapping(int leftCount, int rightCount, SortedMap<Integer, BitSet> equivalence) {
        final Map<Integer, Integer> mapping = new HashMap<>();
        IntStream.range(0, leftCount + rightCount).forEach(i -> {
            if (i >= leftCount) {
                // column in primary only
                mapping.put(i, i - leftCount);
            } else {
                // column in both index and primary
                if (equivalence.containsKey(i)) {
                    final BitSet bs = equivalence.get(i);
                    for (int iEq = bs.nextSetBit(0); iEq >= 0; iEq = bs.nextSetBit(iEq + 1)) {
                        // operate on index i here
                        if (iEq == Integer.MAX_VALUE) {
                            break; // or (i+1) would overflow
                        }

                        if (iEq >= leftCount) {
                            // map to right rel
                            mapping.put(i, iEq - leftCount);
                        }
                    }
                }
            }
        });
        return Mappings.target(mapping, leftCount + rightCount, rightCount);
    }

    /**
     * Build column equivalence between identical columns of index table and
     * primary table
     *
     * @param mq meta query object
     * @param leftRel left child of TableLookup
     * @param rightRel right child of TableLookup
     * @param indexTable index table definition
     * @param primaryTable primary table definition
     * @param equivalence output column equivalence
     */
    private void buildColumnEquivalence(RelMetadataQuery mq, RelNode leftRel, RelNode rightRel, RelOptTable indexTable,
                                        RelOptTable primaryTable, SortedMap<Integer, BitSet> equivalence) {
        final int leftCount = leftRel.getRowType().getFieldCount();

        // get origin column names
        final List<Set<RelColumnOrigin>> leftOrigins = mq.getColumnOriginNames(leftRel);
        final List<Set<RelColumnOrigin>> rightOrigins = mq.getColumnOriginNames(rightRel);

        // copy filters from index table to primary table
        if (null != leftOrigins && null != rightOrigins) {
            // build column name map for primary table
            final Map<String, Integer> rightSet = TreeMaps.caseInsensitiveMap();
            for (int rightRef = 0; rightRef < rightOrigins.size(); rightRef++) {
                final Set<RelColumnOrigin> rcos = rightOrigins.get(rightRef);
                // rcos.size() > 1 means this column represents an expression
                // contains two or more other columns
                if (rcos.size() == 1) {
                    final RelColumnOrigin rco = rcos.iterator().next();
                    final RelOptTable table = rco.getOriginTable();

                    if (!rco.isDerived() && primaryTable.equals(table)) {
                        final String columnName = table.getRowType().getFieldNames().get(rco.getOriginColumnOrdinal());
                        rightSet.put(columnName, rightRef);
                    }
                }
            }

            for (int leftRef = 0; leftRef < leftOrigins.size(); leftRef++) {
                final Set<RelColumnOrigin> rcos = leftOrigins.get(leftRef);
                // rcos.size() > 1 means this column represents an expression
                // contains two or more other columns
                if (rcos.size() == 1) {
                    final RelColumnOrigin rco = rcos.iterator().next();
                    final RelOptTable table = rco.getOriginTable();

                    if (!rco.isDerived() && indexTable.equals(table)) {
                        final String leftColName = rco.getOriginTable()
                            .getRowType()
                            .getFieldNames()
                            .get(rco.getOriginColumnOrdinal());

                        final Integer rightRef = rightSet.get(leftColName);

                        if (null != rightRef) {
                            // build equivalence on top of join
                            equivalence.computeIfAbsent(leftRef, BitSet::new).set(leftCount + rightRef);
                        }
                    }
                }
            }
        }
    }

}
