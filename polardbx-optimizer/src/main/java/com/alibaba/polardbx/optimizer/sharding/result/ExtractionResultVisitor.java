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

package com.alibaba.polardbx.optimizer.sharding.result;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttleImpl;
import com.alibaba.polardbx.optimizer.sharding.label.AggregateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.CorrelateLabel;
import com.alibaba.polardbx.optimizer.sharding.label.JoinLabel;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.label.PredicateNode;
import com.alibaba.polardbx.optimizer.sharding.label.ProjectLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryLabel;
import com.alibaba.polardbx.optimizer.sharding.label.SubqueryWrapperLabel;
import com.alibaba.polardbx.optimizer.sharding.label.TableScanLabel;
import com.alibaba.polardbx.optimizer.sharding.label.UnionLabel;
import com.alibaba.polardbx.optimizer.sharding.label.ValuesLabel;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Visit label tree and merge extraction result of same table
 *
 * @author chenmo.cm
 */
public class ExtractionResultVisitor extends LabelShuttleImpl {

    private final Map<RelNode, Label> relLabelMap = new HashMap<>();
    private final Deque<ResultBean> resultStack = new ArrayDeque<>();
    private final Map<RelOptTable, List<ResultBean>> tableResults = new HashMap<>();

    public ExtractionResultVisitor() {
    }

    @Override
    public Label visit(TableScanLabel tableScanLabel) {
        super.visit(tableScanLabel);

        final ResultBean resultBean = ResultBean.create(tableScanLabel);

        relLabelMap.putIfAbsent(tableScanLabel.getRel(), tableScanLabel);
        resultStack.push(resultBean);
        tableResults.computeIfAbsent(tableScanLabel.getTable(), k -> new ArrayList<>()).add(resultBean);

        return tableScanLabel;
    }

    @Override
    public Label visit(ProjectLabel projectLabel) {
        return super.visit(projectLabel);
    }

    @Override
    public Label visit(JoinLabel joinLabel) {
        super.visit(joinLabel);

        final int size = joinLabel.getInputs().size();
        final List<ResultBean> inputs = IntStream.range(0, size)
            .mapToObj(i -> resultStack.pop())
            .collect(Collectors.toList());

        final RexBuilder rexBuilder = joinLabel.getRel().getCluster().getRexBuilder();
        resultStack.push(ResultBean.or(rexBuilder, inputs));

        return joinLabel;
    }

    @Override
    public Label visit(CorrelateLabel correlateLabel) {
        super.visit(correlateLabel);

        final int size = correlateLabel.getInputs().size();
        final List<ResultBean> inputs = IntStream.range(0, size)
            .mapToObj(i -> resultStack.pop())
            .collect(Collectors.toList());

        final RexBuilder rexBuilder = correlateLabel.getRel().getCluster().getRexBuilder();
        resultStack.push(ResultBean.or(rexBuilder, inputs));

        return correlateLabel;
    }

    @Override
    public Label visit(AggregateLabel aggregateLabel) {
        return super.visit(aggregateLabel);
    }

    @Override
    public Label visit(UnionLabel unionLabel) {
        super.visit(unionLabel);

        final int size = unionLabel.getInputs().size();
        final List<ResultBean> inputs = IntStream.range(0, size)
            .mapToObj(i -> resultStack.pop())
            .collect(Collectors.toList());

        final RexBuilder rexBuilder = unionLabel.getRel().getCluster().getRexBuilder();
        resultStack.push(ResultBean.or(rexBuilder, inputs));

        return unionLabel;
    }

    @Override
    public Label visit(ValuesLabel valuesLabel) {
        super.visit(valuesLabel);

        final ResultBean resultBean = ResultBean.create(valuesLabel);

        relLabelMap.putIfAbsent(valuesLabel.getRel(), valuesLabel);
        resultStack.push(resultBean);

        return valuesLabel;
    }

    @Override
    public Label visit(SubqueryLabel subqueryLabel) {
        return super.visit(subqueryLabel);
    }

    @Override
    public Label visit(SubqueryWrapperLabel subqueryWrapperLabel) {
        ((SubqueryWrapperLabel) super.visit(subqueryWrapperLabel)).subqueryAccept(this);

        final int size = subqueryWrapperLabel.getSubqueryLabelMap().size();
        final List<ResultBean> inputs = IntStream.range(0, size)
            .mapToObj(i -> resultStack.pop())
            .collect(Collectors.toList());
        inputs.add(0, resultStack.pop());

        final RexBuilder rexBuilder = subqueryWrapperLabel.getRel().getCluster().getRexBuilder();
        resultStack.push(ResultBean.or(rexBuilder, inputs));

        return subqueryWrapperLabel;
    }

    @Override
    public Label visit(Label other) {
        throw new AssertionError("Unhandled label " + other.getClass().getName());
    }

    /**
     * Object for condition merge
     */
    public static class ResultBean {
        public static final ResultBean EMPTY = new ResultBean(new HashSet<>(), new HashMap<>(), new HashMap<>());

        private final Set<RelOptTable> tableSet;
        private final Map<RelOptTable, List<Label>> labels;
        private final Map<RelOptTable, Map<String, RexNode>> conditions;

        private ResultBean(Set<RelOptTable> tableSet, Map<RelOptTable, List<Label>> labels,
                           Map<RelOptTable, Map<String, RexNode>> conditions) {
            this.tableSet = tableSet;
            this.labels = labels;
            this.conditions = conditions;
        }

        public static ResultBean create(TableScanLabel label) {
            final RelOptTable table = label.getTable();

            final Set<RelOptTable> tableSet = new HashSet<>();
            final Map<RelOptTable, List<Label>> labels = new HashMap<>();
            final Map<RelOptTable, Map<String, RexNode>> conditions = new HashMap<>();

            tableSet.add(table);
            labels.put(table, ImmutableList.of(label));
            conditions.put(table, Optional.ofNullable(label.getPushdown())
                .map(PredicateNode::getDigests)
                .orElseGet(ImmutableMap::of));

            return new ResultBean(tableSet, labels, conditions);
        }

        public static ResultBean create(ValuesLabel label) {
            return EMPTY;
        }

        public Set<RelOptTable> getTableSet() {
            return tableSet;
        }

        public Map<RelOptTable, List<Label>> getLabels() {
            return labels;
        }

        public Map<RelOptTable, Map<String, RexNode>> getConditions() {
            return conditions;
        }

        /**
         * Compose disjunction of conditions belongs to same table but different label
         *
         * @param rexBuilder Rex builder
         * @param inputs ResultBean of input labels
         * @return ResultBean with merged condition
         */
        public static ResultBean or(RexBuilder rexBuilder, List<ResultBean> inputs) {
            if (GeneralUtil.isEmpty(inputs)) {
                return null;
            }

            final Set<RelOptTable> tableSet = new HashSet<>();
            final Map<RelOptTable, List<Label>> labels = new HashMap<>();
            final Map<RelOptTable, Map<String, RexNode>> conditions = new HashMap<>();

            for (ResultBean input : inputs) {
                if (GeneralUtil.isEmpty(input.getTableSet())) {
                    // Skip ValueLabel
                    continue;
                }

                for (RelOptTable table : input.getTableSet()) {
                    final Map<String, RexNode> inputConditions = input.getConditions().get(table);

                    if (tableSet.contains(table)) {
                        // Table exists in more than one label
                        labels.get(table).addAll(input.getLabels().get(table));

                        // If any label belongs to same table has no partitioning condition,
                        // we should do full table scan, so clear existing partitioning condition
                        if (inputConditions.isEmpty() || conditions.get(table).isEmpty()) {
                            conditions.get(table).clear();
                            continue;
                        }

                        conditions.compute(table, (k, v) -> {
                            final RexNode current = RexUtil
                                .composeConjunction(rexBuilder, conditions.get(table).values(), true);
                            final RexNode append = RexUtil
                                .composeConjunction(rexBuilder, inputConditions.values(), true);

                            final RexNode rex = RexUtil.composeDisjunction(rexBuilder,
                                ImmutableList.of(current, append));

                            v = new HashMap<>();
                            v.put(rex.toString(), rex);

                            return v;
                        });

                    } else {
                        // First time
                        tableSet.add(table);
                        labels.put(table, new ArrayList<>(input.getLabels().get(table)));
                        conditions.put(table, new HashMap<>(inputConditions));
                    }
                }

            }

            return new ResultBean(tableSet, labels, conditions);
        }

        /**
         * Intersection of conditions belongs to same table but different label.
         * Two condition is identical only if their digest is same
         *
         * @param inputs ResultBean of input labels
         * @return ResultBean with merged condition
         */
        public static ResultBean intersect(List<ResultBean> inputs) {
            if (GeneralUtil.isEmpty(inputs)) {
                return null;
            }

            final Set<RelOptTable> tableSet = new HashSet<>();
            final Map<RelOptTable, List<Label>> labels = new HashMap<>();
            final Map<RelOptTable, Map<String, RexNode>> conditions = new HashMap<>();

            for (ResultBean input : inputs) {
                if (GeneralUtil.isEmpty(input.getTableSet())) {
                    // Skip ValueLabel
                    continue;
                }

                for (RelOptTable table : input.getTableSet()) {
                    final Map<String, RexNode> inputConditions = input.getConditions().get(table);

                    if (tableSet.contains(table)) {
                        // Table exists in more than one label
                        labels.get(table).addAll(input.getLabels().get(table));

                        if (inputConditions.isEmpty() || conditions.get(table).isEmpty()) {
                            conditions.get(table).clear();
                            continue;
                        }

                        final Map<String, RexNode> newConditions = conditions.get(table)
                            .entrySet()
                            .stream()
                            .filter(e -> inputConditions.containsKey(e.getKey()))
                            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

                        conditions.put(table, newConditions);
                    } else {
                        // First time
                        tableSet.add(table);
                        labels.put(table, new ArrayList<>(input.getLabels().get(table)));
                        conditions.put(table, new HashMap<>(inputConditions));
                    }
                }

            }

            return new ResultBean(tableSet, labels, conditions);
        }
    }

    public ResultBean getResult() {
        return resultStack.peek();
    }

    public Map<RelOptTable, List<ResultBean>> getTableResults() {
        return tableResults;
    }
}
