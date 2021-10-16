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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepMatchOrder;

import java.util.LinkedList;
import java.util.List;

public enum SQL_REWRITE_RULE_PHASE {
    EMPTY(),
    /**
     * Subquery unnesting
     */
    SUBQUERY(RuleToUse.SUBQUERY),

    /**
     * Remove trivial operator
     */
    PRE(RuleToUse.SQL_REWRITE_CALCITE_RULE_PRE),

    /**
     * The following four rules TDDL_PRE_BUSHY_JOIN_SHALLOW_PUSH_FILTER, LOGICAL_JOIN_TO_BUSHY_JOIN, BUSHY_JOIN_CLUSTERING and BUSHY_JOIN_TO_LOGICAL_JOIN should keep together
     * PRE_BUSHY_JOIN_SHALLOW_PUSH_FILTER will push filter to top join, so make join clustering work and preserve join condition as original as possible
     * PRE_PUSH_TRANSFORMER_RULE will  benefit from this four rule
     **/
    PRE_BUSHY_JOIN_SHALLOW_PUSH_FILTER(RuleToUse.TDDL_PRE_BUSHY_JOIN_SHALLOW_PUSH_FILTER, HepMatchOrder.BOTTOM_UP),
    PRE_LOGICAL_JOIN_TO_BUSHY_JOIN(RuleToUse.LOGICAL_JOIN_TO_BUSHY_JOIN, HepMatchOrder.BOTTOM_UP),
    PRE_BUSHY_JOIN_CLUSTERING(RuleToUse.BUSHY_JOIN_CLUSTERING, HepMatchOrder.BOTTOM_UP),
    PRE_BUSHY_JOIN_TO_LOGICAL_JOIN(RuleToUse.BUSHY_JOIN_TO_LOGICAL_JOIN, HepMatchOrder.BOTTOM_UP),

    /**
     * Push join (with on condition) first to avoid generating subquery like physical sql
     */
    PRE_PUSH_TRANSFORMER_RULE(RuleToUse.PRE_PUSH_TRANSFORMER_RULE, HepMatchOrder.BOTTOM_UP),

    /**
     * Pushes predicates in a SemiJoin into it's input
     */
    CALCITE_NO_FILTER_JOIN_RULE(RuleToUse.CALCITE_NO_FILTER_JOIN_RULE),
    PUSH_BEFORE_JOIN(RuleToUse.PUSH_FILTER_PROJECT_SORT),
    /**
     * The following LOGICAL_JOIN_TO_BUSHY_JOIN, BUSHY_JOIN_CLUSTERING and BUSHY_JOIN_TO_LOGICAL_JOIN rules should keep together
     * BOTTOM_UP match order
     **/
    LOGICAL_JOIN_TO_BUSHY_JOIN(RuleToUse.LOGICAL_JOIN_TO_BUSHY_JOIN, HepMatchOrder.BOTTOM_UP),
    BUSHY_JOIN_CLUSTERING(RuleToUse.BUSHY_JOIN_CLUSTERING, HepMatchOrder.BOTTOM_UP),
    BUSHY_JOIN_TO_LOGICAL_JOIN(RuleToUse.BUSHY_JOIN_TO_LOGICAL_JOIN, HepMatchOrder.BOTTOM_UP),

    /**
     * Pushdown join
     */
    TDDL_PUSH_JOIN_RULE(RuleToUse.TDDL_PUSH_JOIN),
    CALCITE_PUSH_FILTER_PRE(RuleToUse.CALCITE_PUSH_FILTER_PRE),
    CALCITE_PUSH_FILTER_POST(RuleToUse.CALCITE_PUSH_FILTER_POST),
    PUSH_AFTER_JOIN(RuleToUse.PUSH_AFTER_JOIN),

    /**
     * More push after we know which LogicalView scans only one partition
     */
    PUSH_INTO_LOGICALVIEW(RuleToUse.PUSH_INTO_LOGICALVIEW),

    /**
     * Push: purge useless column from LogicalView
     * Pull: pull up Project staying between Join to be friendly for join reorder
     */
    PUSH_PROJECT_RULE(RuleToUse.PUSH_PROJECT_RULE),
    PULL_PROJECT_RULE(RuleToUse.PULL_PROJECT_RULE),

    /**
     * use heuristic to find an optimal ordering
     */
    JOIN_TO_MULTIJOIN(RuleToUse.JOIN_TO_MULTIJOIN, HepMatchOrder.BOTTOM_UP),
    MULTIJOIN_REORDER_TO_JOIN(RuleToUse.MULTIJOIN_REORDER_TO_JOIN, HepMatchOrder.BOTTOM_UP),

    /**
     * CONVERT rules for INSERT, UPDATE, DELETE
     */
    CONVERT_MODIFY(RuleToUse.CONVERT_MODIFY),

    /**
     * Optimize LogicalModify and LogicalInsert
     */
    OPTIMIZE_MODIFY(RuleToUse.OPTIMIZE_MODIFY),

    /**
     * Optimize pushed operator tree, including operator merge rule and join condition simplification.
     * This makes things easier for volcano planer
     */
    OPTIMIZE_LOGICAL_VIEW(RuleToUse.OPTIMIZE_LOGICAL_VIEW),

    OPTIMIZE_AGGREGATE(RuleToUse.OPTIMIZE_AGGREGATE);

    List<ImmutableList<RelOptRule>> collectionList = new LinkedList<>();
    List<RelOptRule> singleList = new LinkedList<>();
    HepMatchOrder order = HepMatchOrder.ARBITRARY;

    SQL_REWRITE_RULE_PHASE(ImmutableList<RelOptRule> col, HepMatchOrder order) {
        collectionList.add(col);
        this.order = order;
    }

    SQL_REWRITE_RULE_PHASE(ImmutableList<RelOptRule> col) {
        collectionList.add(col);
    }

    SQL_REWRITE_RULE_PHASE(RelOptRule rule) {
        singleList.add(rule);
    }

    SQL_REWRITE_RULE_PHASE() {
    }

    public List<ImmutableList<RelOptRule>> getCollectionList() {
        return collectionList;
    }

    public List<RelOptRule> getSingleList() {
        return singleList;
    }

    public HepMatchOrder getMatchOrder() {
        return order;
    }
}
