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

import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLAggJoinToToHashGroupJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalWindowToHashWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLLogicalWindowToSortWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.columnar.COLProjectJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MPPMaterializedViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppBKAJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppCorrelateConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppDynamicValuesConverRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppExpandConversionRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppExpandConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppFilterConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppHashAggConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppHashGroupJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppHashJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppHashWindowConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppLimitConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppMaterializedSemiJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppMemSortConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppMergeSortConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppNLJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppProjectConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSemiBKAJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSemiHashJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSemiNLJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSemiSortMergeJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSortAggConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSortMergeJoinConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppSortWindowConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppTopNConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppUnionConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppValuesConverRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.MppVirtualViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.FilterExchangeTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.FilterRFBuilderTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.JoinToRuntimeFilterJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFBuilderExchangeTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFBuilderJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFBuilderMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFBuilderProjectTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFilterJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.runtimefilter.RFilterProjectTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPAggJoinToToHashGroupJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPJoinTableLookupToBKAJoinTableLookupRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalAggToHashAggRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalJoinToBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalJoinToHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalJoinToNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalSemiJoinToMaterializedSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalSemiJoinToSemiBKAJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalSemiJoinToSemiNLJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalSortToSortRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalWindowToHashWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPLogicalWindowToSortWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPSemiJoinTableLookupToMaterializedSemiJoinTableLookupRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.smp.SMPSemiJoinTableLookupToSemiBKAJoinTableLookupRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.CorrelateProjectRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCorrelateRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSortTransposeRule;
import org.apache.calcite.rel.rules.FilterWindowTransposeRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushTransitivePredicatesRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.UnionMergeRule;

import java.util.List;

/**
 * Rules TDDL used.
 *
 * @author lingce.ldm
 */
public class RuleToUse {

    public static final ImmutableList<RelOptRule> PRE_PUSH_TRANSFORMER_RULE = ImmutableList.of(
        SetOpToSemiJoinRule.INTERSECT,
        SetOpToSemiJoinRule.MINUS,
        PushJoinRule.INSTANCE,
        PushProjectRule.INSTANCE,
//        PushCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        ProjectTableLookupTransposeRule.INSTANCE,
        AggregatePruningRule.INSTANCE,
        TableLookupRemoveRule.INSTANCE,
        PushFilterRule.LOGICALVIEW,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        OptimizeLogicalTableLookupRule.INSTANCE,
        FilterTableLookupTransposeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> SQL_REWRITE_CALCITE_RULE_PRE = ImmutableList.of(
        FilterAggregateTransposeRule.INSTANCE,
        FilterWindowTransposeRule.INSTANCE,
        ProjectRemoveRule.INSTANCE,
        ProjectTableLookupTransposeRule.INSTANCE,
        TableLookupRemoveRule.INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.MINUS_INSTANCE,
        PruneEmptyRules.UNION_INSTANCE,
//        PruneEmptyRules.MINUS_INSTANCE,
//        PruneEmptyRules.INTERSECT_INSTANCE,
        LogicalSemiJoinRule.PROJECT,
        LogicalSemiJoinRule.JOIN,
        PruneEmptyRules.INTERSECT_INSTANCE,
        MergeUnionValuesRule.UNION_VALUES_INSTANCE,
        MergeUnionValuesRule.REMOVE_PROJECT_INSTANCE,
        UnionMergeRule.INSTANCE,
        AggregateReduceFunctionsRule.INSTANCE,
        DrdsAggregateRemoveRule.INSTANCE
    );

    /**
     * 下推 Filter
     */
    private static final ImmutableList<RelOptRule> CALCITE_PUSH_FILTER_BASE = ImmutableList.of(
        FilterJoinRule.FILTER_ON_JOIN,
        FilterJoinRule.JOIN,
        FilterAggregateTransposeRule.INSTANCE,
        PushFilterRule.LOGICALUNION,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        SortProjectTransposeRule.INSTANCE,
        FilterWindowTransposeRule.INSTANCE,
        LogicalSemiJoinRule.PROJECT,
        LogicalSemiJoinRule.JOIN
    );

    public static final ImmutableList<RelOptRule> CALCITE_PUSH_FILTER_PRE = ImmutableList
        .<RelOptRule>builder()
        .addAll(CALCITE_PUSH_FILTER_BASE)
        .add(JoinPushTransitivePredicatesRule.INSTANCE)
        .build();

    public static final ImmutableList<RelOptRule> CALCITE_PUSH_FILTER_POST = ImmutableList
        .<RelOptRule>builder()
        .addAll(CALCITE_PUSH_FILTER_BASE)
        .add(TddlFilterJoinRule.TDDL_FILTER_ON_JOIN)
        .add(FilterProjectTransposeRule.INSTANCE)
        .build();

    public static final ImmutableList<RelOptRule> PUSH_FILTER_PROJECT_SORT = ImmutableList.of(
        FilterProjectTransposeRule.INSTANCE,
        PushFilterRule.LOGICALVIEW,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        FilterTableLookupTransposeRule.INSTANCE,
        ProjectTableLookupTransposeRule.INSTANCE,
        TableLookupRemoveRule.INSTANCE,
        PushProjectRule.INSTANCE,
        PushCorrelateRule.INSTANCE,
        TddlFilterJoinRule.TDDL_FILTER_ON_JOIN,
        FilterSortTransposeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> PUSH_AFTER_JOIN = ImmutableList.of(
        ProjectMergeRule.INSTANCE,
        ProjectRemoveRule.INSTANCE,
        ProjectTableLookupTransposeRule.INSTANCE,
        ProjectSortTransitiveRule.TABLELOOKUP,
        TableLookupRemoveRule.INSTANCE,
        ProjectJoinTransposeRule.INSTANCE,
        ProjectCorrelateTransposeRule.INSTANCE,
        PushFilterRule.LOGICALVIEW,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        FilterTableLookupTransposeRule.INSTANCE,
        OptimizeLogicalTableLookupRule.INSTANCE,
        // Limit Union Transpose
        LimitUnionTransposeRule.INSTANCE,
        PushProjectRule.INSTANCE,
        PushCorrelateRule.INSTANCE,
        TddlFilterJoinRule.TDDL_FILTER_ON_JOIN,
        PushAggRule.SINGLE_GROUP_VIEW,
        PushAggRule.NOT_SINGLE_GROUP_VIEW,
        PushSemiJoinRule.INSTANCE,
        PushSemiJoinDirectRule.INSTANCE,
        PushJoinRule.INSTANCE,
        PushFilterRule.VIRTUALVIEW,
        RemoveJoinConditionFilterRule.INSTANCE
    );

    /**
     * <pre>
     * 1. 将Filter下推至JOIN的ON条件中
     * 2. JOIN 下推
     * </pre>
     */
    public static final ImmutableList<RelOptRule> TDDL_PUSH_JOIN = ImmutableList.of(
        JoinConditionSimplifyRule.INSTANCE,
        TddlFilterJoinRule.TDDL_FILTER_ON_JOIN,
        PushFilterRule.LOGICALVIEW,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        FilterTableLookupTransposeRule.INSTANCE,
        OptimizeLogicalTableLookupRule.INSTANCE,
        PushProjectRule.INSTANCE,
        PushCorrelateRule.INSTANCE,
        PushJoinRule.INSTANCE,
        PushSemiJoinRule.INSTANCE,
        PushSemiJoinDirectRule.INSTANCE,
        RemoveJoinConditionFilterRule.INSTANCE
    );

    /**
     * SUBQUERY 的处理
     */

    public static final ImmutableList<RelOptRule> SUBQUERY = ImmutableList.of(
        SubQueryToSemiJoinRule.FILTER,
        SubQueryToSemiJoinRule.JOIN,
        SubQueryToSemiJoinRule.PROJECT,
        CorrelateProjectRule.INSTANCE,
        FilterConditionSimplifyRule.INSTANCE,
        ProjectToWindowRule.PROJECT,
        ProjectToWindowRule.INSTANCE
    );

    public static ImmutableList<RelOptRule> PUSH_INTO_LOGICALVIEW = ImmutableList.of(
        // Push Agg
        PushAggRule.SINGLE_GROUP_VIEW,
        PushAggRule.NOT_SINGLE_GROUP_VIEW,
        // Push Join
        PushJoinRule.INSTANCE,
        PushSemiJoinRule.INSTANCE,
        PushSemiJoinDirectRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        // PushFilter, because Push Sort move to CBO, so we should transpose, let filter down
        // FIXME: sharding calculation dose not satisfying monotonicity
        FilterSortTransposeRule.INSTANCE,
        FilterProjectTransposeRule.INSTANCE,
        PushFilterRule.LOGICALVIEW,
        FilterMergeRule.INSTANCE,
        FilterConditionSimplifyRule.INSTANCE,
        // Push Project
        PushProjectRule.INSTANCE,

        PushCorrelateRule.INSTANCE,
        // Project Merge
        ProjectMergeRule.INSTANCE,
        // TableLookup Related
        FilterTableLookupTransposeRule.INSTANCE,
        ProjectTableLookupTransposeRule.INSTANCE,
        OptimizeLogicalTableLookupRule.INSTANCE,
        TableLookupRemoveRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CONVERT_MODIFY = ImmutableList.of(
        LogicalModifyToLogicalRelocateRule.INSTANCE,
        RemoveProjectRule.INSTANCE,
        RemoveUnionProjectRule.INSTANCE,
        LogicalValuesToLogicalDynamicValuesRule.INSTANCE);

    public static final List RULE_FOR_NATIVE_SQL = ImmutableList.of(
        FilterProjectTransposeRule.INSTANCE,
        //SortProjectTransposeRule.INSTANCE,
        ProjectRemoveRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> TDDL_SHARDING_RULE = ImmutableList.of(
        ShardLogicalViewRule.INSTANCE,
        MergeSortRemoveGatherRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> EXPAND_VIEW_PLAN = ImmutableList.of(
        ExpandViewPlanRule.INSTANCE
    );

    /**
     * the following two rules are aimed to reorder the joins
     */
    public static ImmutableList<RelOptRule> JOIN_TO_MULTIJOIN = ImmutableList.of(
        LogicalJoinToMultiJoinRule.INSTANCE
    );
    public static ImmutableList<RelOptRule> MULTIJOIN_REORDER_TO_JOIN = ImmutableList.of(
        MultiJoinToLogicalJoinRule.INSTANCE
    );

    /**
     * 将多次join转换为bushy join的结构，随后下推
     */
    public static final ImmutableList<RelOptRule> TDDL_PRE_BUSHY_JOIN_SHALLOW_PUSH_FILTER = ImmutableList.of(
        TddlPreBushyJoinShallowPushFilterRule.INSTANCE,
        FilterJoinRule.SEMI_JOIN
    );

    public static final ImmutableList<RelOptRule> LOGICAL_JOIN_TO_BUSHY_JOIN = ImmutableList.of(
        LogicalJoinToBushyJoinRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> BUSHY_JOIN_CLUSTERING = ImmutableList.of(
        BushyJoinClusteringRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> BUSHY_JOIN_TO_LOGICAL_JOIN = ImmutableList.of(
        BushyJoinToLogicalJoinRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CALCITE_NO_FILTER_JOIN_RULE = ImmutableList.of(
        FilterJoinRule.JOIN
    );

    public static final ImmutableList<RelOptRule> PUSH_PROJECT_RULE = ImmutableList.of(
        ProjectJoinTransposeRule.INSTANCE,
        ProjectCorrelateTransposeRule.INSTANCE,
        ProjectFilterTransposeRule.INSTANCE,
        ProjectWindowTransposeRule.INSTANCE,
        ProjectSortTransitiveRule.INSTANCE,
        PushProjectRule.INSTANCE,
        PushCorrelateRule.INSTANCE,
        ProjectMergeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> PULL_PROJECT_RULE = ImmutableList.of(
        JoinProjectTransposeRule.LEFT_PROJECT,
        JoinProjectTransposeRule.RIGHT_PROJECT,
        SemiJoinProjectTransposeRule.INSTANCE,
        ProjectMergeRule.INSTANCE,
        PushCorrelateRule.INSTANCE,
        ProjectRemoveRule.INSTANCE,
        FilterProjectTransposeRule.INSTANCE,
        AggregateProjectMergeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> EXPAND_TABLE_LOOKUP = ImmutableList.of(
        PushProjectRule.INSTANCE,
//        PushCorrelateRule.INSTANCE,
        ProjectSortTransitiveRule.LOGICALVIEW,
        ProjectRemoveRule.INSTANCE,
        TableLookupExpandRule.INSTANCE);

    public static final ImmutableList<RelOptRule> OPTIMIZE_MODIFY = ImmutableList.of(
        OptimizeLogicalModifyRule.INSTANCE,
        OptimizeLogicalInsertRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> OPTIMIZE_LOGICAL_VIEW = ImmutableList.of(
        OptimizeLogicalViewRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> OPTIMIZE_AGGREGATE = ImmutableList.of(
        // remove aggregation if it does not aggregate and input is already distinct
        DrdsAggregateRemoveRule.INSTANCE,
        // expand distinct aggregate to normal aggregate with groupby
        DrdsAggregateExpandDistinctAggregatesRule.INSTANCE,
        // expand grouping sets
        GroupingSetsToExpandRule.INSTANCE,
        ProjectMergeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CBO_TOO_MANY_JOIN_REORDER_RULE = ImmutableList.of(
        JoinCommuteRule.SWAP_OUTER
    );

    public static final ImmutableList<RelOptRule> CBO_LEFT_DEEP_TREE_JOIN_REORDER_RULE = ImmutableList.of(
        JoinCommuteRule.SWAP_OUTER_SWAP_BOTTOM_JOIN,
        InnerJoinLAsscomRule.INSTANCE,
        InnerJoinLAsscomRule.PROJECT_INSTANCE,
        OuterJoinLAsscomRule.INSTANCE,
        OuterJoinLAsscomRule.PROJECT_INSTANCE,
        SemiJoinSemiJoinTransposeRule.INSTANCE,
        CBOLogicalSemiJoinLogicalJoinTransposeRule.LASSCOM,
        CBOLogicalSemiJoinLogicalJoinTransposeRule.PROJECT_LASSCOM
    );

    public static final ImmutableList<RelOptRule> CBO_ZIG_ZAG_TREE_JOIN_REORDER_RULE = ImmutableList.of(
        JoinCommuteRule.SWAP_OUTER_SWAP_ZIG_ZAG,
        InnerJoinLAsscomRule.INSTANCE,
        InnerJoinLAsscomRule.PROJECT_INSTANCE,
        OuterJoinLAsscomRule.INSTANCE,
        OuterJoinLAsscomRule.PROJECT_INSTANCE,
        SemiJoinSemiJoinTransposeRule.INSTANCE,
        CBOLogicalSemiJoinLogicalJoinTransposeRule.LASSCOM,
        CBOLogicalSemiJoinLogicalJoinTransposeRule.PROJECT_LASSCOM
    );

    public static final ImmutableList<RelOptRule> CBO_BUSHY_TREE_JOIN_REORDER_RULE = ImmutableList.of(
        JoinCommuteRule.SWAP_OUTER,
        ProjectJoinCommuteRule.SWAP_OUTER,
        InnerJoinLeftAssociateRule.INSTANCE,
        InnerJoinLeftAssociateRule.PROJECT_INSTANCE,
        InnerJoinRightAssociateRule.INSTANCE,
        InnerJoinRightAssociateRule.PROJECT_INSTANCE,
        JoinExchangeRule.INSTANCE,
        JoinExchangeRule.BOTH_PROJECT,
        JoinExchangeRule.LEFT_PROJECT,
        JoinExchangeRule.RIGHT_PROJECT,
        OuterJoinAssocRule.INSTANCE,
        OuterJoinLAsscomRule.INSTANCE,
        CBOLogicalSemiJoinLogicalJoinTransposeRule.INSTANCE,
        SemiJoinSemiJoinTransposeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> CBO_JOIN_TABLELOOKUP_REORDER_RULE = ImmutableList.of(
        JoinTableLookupTransposeRule.INSTANCE_RIGHT
    );

    public static final ImmutableList<RelOptRule> CBO_BASE_RULE = ImmutableList.of(
        // Join Algorithm
        SMPLogicalJoinToBKAJoinRule.LOGICALVIEW_NOT_RIGHT,
        SMPLogicalJoinToBKAJoinRule.LOGICALVIEW_RIGHT,
        SMPJoinTableLookupToBKAJoinTableLookupRule.TABLELOOKUP_NOT_RIGHT,
        SMPJoinTableLookupToBKAJoinTableLookupRule.TABLELOOKUP_RIGHT,
        SMPLogicalJoinToNLJoinRule.INSTANCE,
        SMPLogicalJoinToHashJoinRule.INSTANCE,
        SMPLogicalJoinToHashJoinRule.OUTER_INSTANCE,
        LogicalJoinToSortMergeJoinRule.INSTANCE,
        SMPLogicalSemiJoinToMaterializedSemiJoinRule.INSTANCE,
        SMPSemiJoinTableLookupToMaterializedSemiJoinTableLookupRule.INSTANCE,
        SMPLogicalSemiJoinToSemiNLJoinRule.INSTANCE,
        SMPLogicalSemiJoinToSemiHashJoinRule.INSTANCE,
        LogicalSemiJoinToSemiSortMergeJoinRule.INSTANCE,
        SMPLogicalSemiJoinToSemiBKAJoinRule.INSTANCE,
        SMPSemiJoinTableLookupToSemiBKAJoinTableLookupRule.INSTANCE,
        // Agg Algorithm
        LogicalAggToSortAggRule.INSTANCE,
        SMPLogicalAggToHashAggRule.INSTANCE,
        SMPAggJoinToToHashGroupJoinRule.INSTANCE,
        // window
        SMPLogicalWindowToSortWindowRule.INSTANCE,
        SMPLogicalWindowToHashWindowRule.INSTANCE,
        // Push Sort
        PushSortRule.PLAN_ENUERATE,
        // Push Filter
        PushFilterRule.LOGICALVIEW,
        PushFilterRule.MERGE_SORT,
        PushFilterRule.PROJECT_FILTER_LOGICALVIEW,
        // Push Join
        CBOPushSemiJoinRule.INSTANCE,
        CBOPushSemiJoinDirectRule.INSTANCE,
        CBOPushJoinRule.INSTANCE,
        // Push Agg
        CBOPushAggRule.LOGICALVIEW,
        // Join Window Transpose
        CBOJoinWindowTransposeRule.INSTANCE,
        // Agg Join Transpose
        DrdsAggregateJoinTransposeRule.EXTENDED,
        DrdsAggregateJoinTransposeRule.PROJECT_EXTENDED,
        // Access Path
        AccessPathRule.LOGICALVIEW,
        OSSMergeIndexRule.INSTANCE,
        // Sort TableLookup Transpose
        SortTableLookupTransposeRule.INSTANCE,
        // Sort Join Transpose
        DrdsSortJoinTransposeRule.INSTANCE,
        DrdsSortProjectTransposeRule.INSTANCE,
        // Push Modify
        PushModifyRule.VIEW,
        PushModifyRule.MERGESORT,
        PushModifyRule.SORT_VIEW,

        // Convert Sort
        SMPLogicalSortToSortRule.INSTANCE,
        SMPLogicalSortToSortRule.TOPN,
        // Convert
        SMPLogicalViewConvertRule.INSTANCE,
        DrdsExpandConvertRule.SMP_INSTANCE,
        DrdsProjectConvertRule.SMP_INSTANCE,
        DrdsRecursiveCTEAnchorConvertRule.SMP_INSTANCE,
        DrdsRecursiveCTEConvertRule.SMP_INSTANCE,
        DrdsFilterConvertRule.SMP_INSTANCE,
        DrdsCorrelateConvertRule.SMP_INSTANCE,

        DrdsMergeSortConvertRule.INSTANCE,
        DrdsValuesConvertRule.SMP_INSTANCE,
        DrdsUnionConvertRule.SMP_INSTANCE,
        DrdsLogicalTableLookupConvertRule.SMP_INSTANCE,
        DrdsVirtualViewConvertRule.SMP_INSTANCE,
        DrdsDynamicConvertRule.SMP_INSTANCE,
        DrdsMaterializedViewConvertRule.SMP_INSTANCE,

        DrdsModifyConvertRule.INSTANCE,
        DrdsInsertConvertRule.INSTANCE,
        DrdsRelocateConvertRule.INSTANCE,
        DrdsRecyclebinConvertRule.INSTANCE,
        DrdsOutFileConvertRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> COLUMNAR_CBO_RULE = ImmutableList.of(
        // Join Algorithm
        COLLogicalJoinToNLJoinRule.INSTANCE,
        COLLogicalJoinToHashJoinRule.INSTANCE,
        COLLogicalJoinToHashJoinRule.OUTER_INSTANCE,
        COLLogicalSemiJoinToSemiNLJoinRule.INSTANCE,
        COLLogicalSemiJoinToSemiHashJoinRule.INSTANCE,
        COLLogicalSemiJoinToSemiHashJoinRule.OUTER_INSTANCE,
        JoinAggToJoinAggSemiJoinRule.INSTANCE,
        // Agg Algorithm
        COLLogicalAggToHashAggRule.INSTANCE,
        COLAggJoinToToHashGroupJoinRule.INSTANCE,
        // window
        COLLogicalWindowToSortWindowRule.INSTANCE,
        COLLogicalWindowToHashWindowRule.INSTANCE,
        // Push Sort
        PushSortRule.PLAN_ENUERATE,
        // Push Filter
        PushFilterRule.LOGICALVIEW,
        PushFilterRule.MERGE_SORT,
        PushFilterRule.PROJECT_FILTER_LOGICALVIEW,
        // Join Window Transpose
        CBOJoinWindowTransposeRule.INSTANCE,
        // Agg Join Transpose
        DrdsAggregateJoinTransposeRule.EXTENDED,
        DrdsAggregateJoinTransposeRule.PROJECT_EXTENDED,
        new COLProjectJoinTransposeRule(3),
        // Sort Join Transpose
        DrdsSortJoinTransposeRule.INSTANCE,
        DrdsSortProjectTransposeRule.INSTANCE,
        // Convert Sort
        COLLogicalSortToSortRule.INSTANCE,
        COLLogicalSortToSortRule.TOPN,
        // Convert Logicalview
        COLLogicalViewConvertRule.INSTANCE,

        // Convert
        DrdsExpandConvertRule.COL_INSTANCE,
        DrdsProjectConvertRule.COL_INSTANCE,
        DrdsRecursiveCTEAnchorConvertRule.COL_INSTANCE,
        DrdsRecursiveCTEConvertRule.COL_INSTANCE,
        DrdsFilterConvertRule.COL_INSTANCE,
        DrdsCorrelateConvertRule.COL_INSTANCE,
        DrdsMergeSortConvertRule.INSTANCE,
        DrdsValuesConvertRule.COL_INSTANCE,
        DrdsUnionConvertRule.COL_INSTANCE,
        DrdsVirtualViewConvertRule.COL_INSTANCE,
        DrdsDynamicConvertRule.COL_INSTANCE,
        DrdsMaterializedViewConvertRule.COL_INSTANCE
    );

    public static final ImmutableList<RelOptRule> MPP_CBO_RULE = ImmutableList.of(
        // Exchange
        MppExpandConversionRule.INSTANCE,
        // Join Convert
        MppNLJoinConvertRule.INSTANCE,
        MppHashGroupJoinConvertRule.INSTANCE,
        MppHashJoinConvertRule.INSTANCE,
        MppBKAJoinConvertRule.INSTANCE,
        MppSortMergeJoinConvertRule.INSTANCE,
        // Semi Join Convert
        MppSemiHashJoinConvertRule.INSTANCE,
        MppSemiBKAJoinConvertRule.INSTANCE,
        MppSemiNLJoinConvertRule.INSTANCE,
        MppMaterializedSemiJoinConvertRule.INSTANCE,
        MppSemiSortMergeJoinConvertRule.INSTANCE,

        // Agg Convert
        MppHashAggConvertRule.INSTANCE,
        MppSortAggConvertRule.INSTANCE,
        // Window Convert
        MppSortWindowConvertRule.INSTANCE,
        MppHashWindowConvertRule.INSTANCE,
        // Sort Convert
        MppMemSortConvertRule.INSTANCE,
        MppLimitConvertRule.INSTANCE,
        MppTopNConvertRule.INSTANCE,
        // Merge Sort
        MppMergeSortConvertRule.INSTANCE,
        // Others Convert
        MppExpandConvertRule.INSTANCE,
        MppUnionConvertRule.INSTANCE,
        MppValuesConverRule.INSTANCE,
        MppDynamicValuesConverRule.INSTANCE,
        MppProjectConvertRule.INSTANCE,
        MppCorrelateConvertRule.INSTANCE,
        MppFilterConvertRule.INSTANCE,
        MppLogicalViewConvertRule.INSTANCE,
        MppVirtualViewConvertRule.INSTANCE,
        MPPMaterializedViewConvertRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> RUNTIME_FILTER = ImmutableList.of(
        JoinToRuntimeFilterJoinRule.HASHJOIN_INSTANCE,
        JoinToRuntimeFilterJoinRule.SEMIJOIN_INSTANCE,
        RFBuilderExchangeTransposeRule.INSTANCE,
        RFBuilderJoinTransposeRule.INSTANCE,
        RFBuilderProjectTransposeRule.INSTANCE,
        RFBuilderMergeRule.INSTANCE,
        FilterExchangeTransposeRule.INSTANCE,
        RFilterProjectTransposeRule.INSTANCE,
        FilterMergeRule.INSTANCE,
        PushFilterRule.LOGICALUNION,
        FilterCorrelateRule.INSTANCE,
        CorrelateProjectRule.INSTANCE,
        FilterAggregateTransposeRule.INSTANCE,
        RFilterJoinTransposeRule.HASHJOIN_INSTANCE,
        RFilterJoinTransposeRule.SEMI_HASHJOIN_INSTANCE,
        FilterRFBuilderTransposeRule.INSTANCE
    );

    public static final ImmutableList<RelOptRule> PULL_FILTER_OVER_JOIN = ImmutableList.of(
        JoinFilterTransposeRule.BOTH_FILTER,
        JoinFilterTransposeRule.LEFT_FILTER,
        JoinFilterTransposeRule.RIGHT_FILTER
    );
}
