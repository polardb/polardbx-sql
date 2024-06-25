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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterReorderRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinSemiJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.RuleToUse;
import com.alibaba.polardbx.optimizer.core.planner.rule.SemiJoinCorrToSubQueryRule;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPruneStepType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.util.PartitionPruneStepVisitor;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSortTransposeRule;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule;
import org.apache.calcite.rel.rules.SemiJoinProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_IN_PRUNE_CACHE_SIZE;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_IN_PRUNE_CACHE_TABLE_SIZE;
import static com.alibaba.polardbx.optimizer.utils.PlannerUtils.deriveJoinType;
import static com.alibaba.polardbx.optimizer.utils.PushDownUtils.pushAgg;
import static com.alibaba.polardbx.optimizer.utils.PushDownUtils.pushFilter;
import static com.alibaba.polardbx.optimizer.utils.PushDownUtils.pushProject;
import static com.alibaba.polardbx.optimizer.utils.PushDownUtils.pushSort;

/**
 * Push down optimizer.
 *
 * @author Lingce.ldm on 2016/12/21.
 */

public class PushDownOpt {

    private static final Logger logger = LoggerFactory.getLogger(PushDownOpt.class);

    private RelOptSchema relOptSchema;
    private RelBuilder builder;
    private boolean aggIsPushed;
    private SqlNode nativeSqlNodeHintCache;
    private DbType dbType;
    private LogicalView tableScan;
    private TableModify tableModify;
    private PlainRows plainRows;
    /**
     * index of list: the table index of logical table in the LV
     * Map<String, Comparative>: the comp info of the the index of  logical table in LV
     * key: shard column;
     * val: the comp info of the column
     */
    private List<Map<String, Comparative>> comparatives = new ArrayList<>();
    private List<Map<String, Comparative>> fullComparatives = new ArrayList<>();

    /**
     * index of list: the table index of logical table in the LV
     * PartitionPruneStep : the part prune step info of the the index of logical table in LV
     */
    private Map<String, List<PartitionPruneStep>> allPartPruneSteps = Maps.newConcurrentMap();

    private Set<Integer> shardRelatedInTypeParamIndexes;

    public PushDownOpt(LogicalView tableScan, DbType dbType, ExecutionContext ec) {
        this.tableScan = tableScan;
        this.dbType = dbType;
        this.aggIsPushed = false;
        this.relOptSchema = RelUtils.buildCatalogReader(tableScan.getSchemaName(), ec);
        this.builder = createBuilderAndScan(relOptSchema);
        this.plainRows = new PlainRows(tableScan.getRowType(), tableScan.getLogicalTableName());
    }

    public PushDownOpt(LogicalView tableScan, RelNode rel, DbType dbType, ExecutionContext ec) {
        this.tableScan = tableScan;
        this.dbType = dbType;
        this.relOptSchema = RelUtils.buildCatalogReader(tableScan.getSchemaName(), ec);
        this.builder = createBuilder(relOptSchema);
        this.builder.push(rel);
        // don't re-calculate real rowType unless it is necessary
        this.plainRows = new PlainRows(rel.getRowType(), tableScan.getLogicalTableName());
    }

    private PushDownOpt(PushDownOpt pushDownOpt) {
        this.tableScan = pushDownOpt.tableScan;
        this.dbType = pushDownOpt.dbType;
        this.relOptSchema = pushDownOpt.relOptSchema;
        this.builder = pushDownOpt.builder;
        this.aggIsPushed = pushDownOpt.aggIsPushed;
        this.nativeSqlNodeHintCache = pushDownOpt.nativeSqlNodeHintCache;
        this.tableModify = pushDownOpt.tableModify;
        this.plainRows = pushDownOpt.plainRows.copy();
        this.comparatives = pushDownOpt.comparatives;
        this.fullComparatives = pushDownOpt.fullComparatives;
        this.allPartPruneSteps = pushDownOpt.allPartPruneSteps;
    }

    public final PushDownOpt copy(LogicalView logicalView, RelNode rel) {
        PushDownOpt newPushDownOpt = new PushDownOpt(this);
        newPushDownOpt.tableScan = logicalView;
        newPushDownOpt.builder = createBuilder(relOptSchema);
        newPushDownOpt.builder.push(rel);

        if (this.comparatives != null) {
            newPushDownOpt.comparatives = new ArrayList<>();
            for (Map<String, Comparative> m : this.comparatives) {
                Map<String, Comparative> newMap = new HashMap<>();
                for (Map.Entry<String, Comparative> entry : m.entrySet()) {
                    newMap.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().clone());
                }
                newPushDownOpt.comparatives.add(newMap);
            }
        }
        if (this.fullComparatives != null) {
            newPushDownOpt.fullComparatives = new ArrayList<>();
            for (Map<String, Comparative> m : this.fullComparatives) {
                Map<String, Comparative> newMap = new HashMap<>();
                for (Map.Entry<String, Comparative> entry : m.entrySet()) {
                    newMap.put(entry.getKey(), entry.getValue() == null ? null : entry.getValue().clone());
                }
                newPushDownOpt.fullComparatives.add(newMap);
            }
        }
        if (this.allPartPruneSteps != null && !this.allPartPruneSteps.isEmpty() && logicalView.isNewPartDbTbl()) {
            PartRoutingPlanInfo partRoutingPlanInfo =
                newPushDownOpt.buildPartRoutingPlanInfo(logicalView, logicalView.isNewPartDbTbl(),
                    PlannerContext.getPlannerContext(rel).getExecutionContext());
            newPushDownOpt.updatePartRoutingPlanInfo(partRoutingPlanInfo);
        }
        newPushDownOpt.shardRelatedInTypeParamIndexes = shardRelatedInTypeParamIndexes;
        return newPushDownOpt;
    }

    private RelBuilder createBuilder(RelOptSchema relOptSchema) {
        RelBuilder builder = RelBuilder.proto(Contexts.EMPTY_CONTEXT).create(tableScan.getCluster(), relOptSchema);
        return builder;
    }

    private RelBuilder createBuilderAndScan(RelOptSchema relOptSchema) {
        List<String> qualifiedName = tableScan.getTable().getQualifiedName();
        RelBuilder builder = createBuilder(relOptSchema);
        // The tableName to scan, tableName is 'db.table'
        builderScan(qualifiedName, builder);
        return builder;
    }

    private void builderScan(List<String> qualifiedName, RelBuilder builder) {
        builder.scan(qualifiedName);
        pushedIndexHint(builder);
        if (this.tableScan.getFlashback() != null) {
            builder.flashback(this.tableScan.getFlashback(), this.tableScan.getFlashbackOperator());
        }
    }

    public void push(RelNode relNode) {
        if (relNode instanceof LogicalProject) {
            LogicalProject project = (LogicalProject) relNode;
            pushProject(project, builder);

            /**
             * <pre>
             * 下推的 Project 可能会影响 LogicalView 的列信息
             * 根据 Project 的列更新对于底层 rowType 的引用
             * </pre>
             */
            plainRows = plainRows.updateRefIndex(project);
            return;
        }

        if (relNode instanceof LogicalFilter) {
            LogicalFilter filter = (LogicalFilter) relNode;
            pushFilter(filter, builder);

            if (nativeSqlNodeHintCache == null) {
                rebuildPartRoutingPlanInfo();
            }
            return;
        }

        if (relNode instanceof LogicalAggregate) {
            aggIsPushed = true;
            LogicalAggregate agg = (LogicalAggregate) relNode;
            pushAgg(agg, builder);
            plainRows = plainRows.updateRefIndex(agg);
            return;
        }

        if (relNode instanceof LogicalSort) {
            LogicalSort sort = (LogicalSort) relNode;
            pushSort(sort, builder);
            return;
        }

        if (relNode instanceof TableModify) {
            builder.modify((TableModify) relNode);
            this.tableModify = (TableModify) relNode;
        }
    }

    /**
     * transform semiJoin and correlate to subquery, used for visitors who don't support semi join
     *
     * @return the root of a tree without semiJoin and correlate
     */
    public RelNode removeSemiJoin() {
        // semi join could not exist when ENABLE_LV_SUBQUERY_UNWRAP is disabled
        // thus there is no need to transform
        if (!PlannerContext.getPlannerContext(getPushedRelNode()).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP)) {
            return getPushedRelNode();
        }
        if (!OptimizerUtils.findSemiJoin(getPushedRelNode())) {
            return getPushedRelNode();
        }
        HepProgramBuilder builder = new HepProgramBuilder();
        //transform semi join
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(SemiJoinCorrToSubQueryRule.SEMI_JOIN);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(getPushedRelNode());
        RelNode optimizedNode = planner.findBestExp();

        return optimizedNode.accept(new RelCastRemover());
    }

    /**
     * transform semiJoin and correlate to subquery, optimize the tree for native sql
     *
     * @return the root of a tree without semiJoin and correlate
     */
    public void optimizePhySql() {
        HepProgramBuilder builder = new HepProgramBuilder();

        //pull project
        builder.addGroupBegin();
        builder.addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.RIGHT_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.BOTH_PROJECT);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(SemiJoinProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addGroupEnd();

        //pull filter
        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.PULL_FILTER_OVER_JOIN);
        builder.addRuleInstance(SemiJoinFilterTransposeRule.INSTANCE);
        builder.addGroupEnd();

        //pull join
        builder.addGroupBegin();
        builder.addRuleInstance(JoinSemiJoinTransposeRule.LEFT_SEMI);
        builder.addRuleInstance(JoinSemiJoinTransposeRule.RIGHT_SEMI);
        builder.addGroupEnd();

        //transform semi join
        builder.addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin();
        builder.addRuleInstance(SemiJoinCorrToSubQueryRule.SEMI_JOIN_REORDER);
        builder.addGroupEnd();

        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(FilterConditionSimplifyRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE_WONT_IGNORE_REX_SUBQUERY);
        builder.addGroupEnd();

        builder.addGroupBegin();
        builder.addRuleInstance(FilterReorderRule.INSTANCE);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(getPushedRelNode());
        RelNode optimizedNode = planner.findBestExp();

        optimizedNode = optimizedNode.accept(new RelCastRemover());
        this.builder.clear();
        this.builder.push(optimizedNode);
    }

    /**
     * return current native sql, may be a middle state
     */
    public SqlNode buildNativeSql(RelToSqlConverter sqlConverter, ReplaceCallWithLiteralVisitor visitor) {
        // while it not safe to call hepPlanner in executor, it is for sure that there is no semi join after optimizer
        RelNode relNode = removeSemiJoin();
        if (visitor != null) {
            // only in gsi currently
            relNode = replaceCallWithLiteral(relNode, visitor);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("The LogicalView relNode \n" + RelOptUtil.toString(relNode));
        }

        return sqlConverter.visitChild(0, relNode).asStatement();
    }

    public SqlNode getNativeSql(RelToSqlConverter sqlConverter, ReplaceCallWithLiteralVisitor visitor) {
        if (null == this.nativeSqlNodeHintCache) {
            return buildNativeSql(sqlConverter, visitor);
        } else {
            return this.nativeSqlNodeHintCache;
        }
    }

    public void optimize() {
        RelNode optNode = optimize(getPushedRelNode());
        builder.clear();
        builder.push(optNode);
    }

    /**
     * Optimize the Native RelNode
     */
    protected RelNode optimize(RelNode relNode) {
        HepProgramBuilder builder = new HepProgramBuilder();

        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.RULE_FOR_NATIVE_SQL);
        builder.addRuleInstance(JoinProjectTransposeRule.LEFT_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.RIGHT_PROJECT);
        builder.addRuleInstance(JoinProjectTransposeRule.BOTH_PROJECT);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterSortTransposeRule.INSTANCE);

        builder.addGroupEnd();

        /**
         * Merge filter and project Merge Agg-Project
         */
        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE_WONT_IGNORE_REX_SUBQUERY);
        builder.addRuleInstance(AggregateProjectMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(JoinConditionSimplifyRule.INSTANCE);
        builder.addRuleInstance(FilterConditionSimplifyRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(relNode);
        RelNode optimizedNode = planner.findBestExp();

        optimizedNode = optimizedNode.accept(new RelCastRemover());
        return optimizedNode;
    }

    public void optimizeOSS() {
        HepProgramBuilder builder = new HepProgramBuilder();

        builder.addGroupBegin();
        builder.addRuleCollection(RuleToUse.RULE_FOR_NATIVE_SQL);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterSortTransposeRule.INSTANCE);

        builder.addGroupEnd();

        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE_WONT_IGNORE_REX_SUBQUERY);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addGroupEnd();
        builder.addGroupBegin();
        builder.addRuleInstance(FilterConditionSimplifyRule.INSTANCE);
        builder.addGroupEnd();
        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(getPushedRelNode());
        RelNode optimizedNode = planner.findBestExp();

        optimizedNode = optimizedNode.accept(new RelCastRemover());
        RelNode optNode = optimizedNode;

        this.builder.clear();
        this.builder.push(optNode);
    }

    /**
     * 判断agg是否已经下压
     * <p>
     * 在TddlPushDownRule中需要根据该值判断是否继续下压, 否则会死循环
     */
    public boolean aggIsPushed() {
        return aggIsPushed;
    }

    public void pushJoin(Join join, LogicalView rightView, List<RexNode> leftFilters, List<RexNode> rightFilters,
                         RelOptCluster cluster) {
        plainRows = plainRows.updateRefIndexForJoin(
            cluster.getTypeFactory(),
            tableScan.buildCurRowType(),
            rightView.buildCurRowType(),
            rightView.getPushDownOpt().getPlainRows());

        // push TableScan
        RelNode rPushedNode = rightView.getPushedRelNode();
        builder.push(rPushedNode);
        builder.join(join.getJoinType(), join.getCondition());
    }

    public void pushSemiJoinDirect(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                                   List<RexNode> rightFilters, RelOptCluster cluster) {
        pushSemiJoin(join, rightView, leftFilters, rightFilters, cluster);

        //pass the logicalView of left and right
        LogicalSemiJoin semiJoin =
            join.copy(join.getTraitSet(), join.getCondition(), builder.build(), rightView.getPushedRelNode(),
                join.getJoinType(), join.isSemiJoinDone());

        builder.push(semiJoin);
    }

    public void pushSemiJoin(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                             List<RexNode> rightFilters, RelOptCluster cluster) {
        plainRows = plainRows.updateRefIndexForSemiJoin(
            join,
            cluster.getTypeFactory(),
            tableScan.buildCurRowType(),
            rightView.buildCurRowType(),
            rightView.getPushDownOpt().getPlainRows());
    }

    private void updateComparative(Map<String, Map<String, Comparative>> inferred,
                                   Map<String, Map<String, Comparative>> inferredFull, AtomicInteger comparativeIndex) {
        tableScan.getTableNames().forEach(tableName -> {
            if (comparatives.size() == comparativeIndex.get()) {
                comparatives.add(inferred.getOrDefault(tableName, new HashMap<>()));
                fullComparatives.add(inferredFull.getOrDefault(tableName, new HashMap<>()));
            } else {
                comparatives.set(comparativeIndex.get(), inferred.getOrDefault(tableName, new HashMap<>()));
                fullComparatives.set(comparativeIndex.get(), inferredFull.getOrDefault(tableName, new HashMap<>()));
            }
            comparativeIndex.getAndIncrement();
        });
    }

    private void updatePartPruneSteps(Map<String, PartitionPruneStep> allPartPruneStepMap) {
        List<String> logTbNameList = tableScan.getTableNames();
        ExecutionContext ec = PlannerContext.getPlannerContext(tableScan).getExecutionContext();
        List<PartitionPruneStep> allPartPruneSteps = new ArrayList<>();
        for (int i = 0; i < logTbNameList.size(); i++) {
            String tbName = logTbNameList.get(i);
            PartitionPruneStep partStep = allPartPruneStepMap.get(tbName);
            if (partStep == null) {
                // just add full table scan. for sql like column subquery
                partStep =
                    PartitionPruneStepBuilder.genFullScanAllPhyPartsStepInfoByDbNameAndTbName(tableScan.getSchemaName(),
                        tbName, ec);
            }
            allPartPruneSteps.add(partStep);
        }
        String key = OptimizerUtils.buildInExprKey(PlannerContext.getPlannerContext(tableScan).getExecutionContext());

        this.allPartPruneSteps.put(key, allPartPruneSteps);
    }

    /**
     * if any shard column is used in `IN` expr, then put it into shardRelatedInTypeParamIndexes
     *
     * @param ec context that contains in expr params
     */
    public void buildShardRelatedInTypeParamIndexes(ExecutionContext ec) {
        // prune all raw string size to 1, avoiding part routing plan info cannot being built
        // for too large params num of in expr.
        if (ec.getParams() == null || ec.getParams().getCurrentParameter() == null) {
            shardRelatedInTypeParamIndexes = new HashSet<>();
            return;
        }
        Parameters tmp = ec.getParams().clone();
        Map<Integer, ParameterContext> replace = Maps.newHashMap();
        boolean hasRawString = false;
        for (Map.Entry<Integer, ParameterContext> entry : tmp.getCurrentParameter().entrySet()) {
            ParameterContext parameterContext = entry.getValue();
            Object param = parameterContext.getValue();
            if (param instanceof RawString) {
                hasRawString = true;
                RawString rawString = (RawString) param;
                replace.put(entry.getKey(), new ParameterContext(ParameterMethod.setObject1,
                    new Object[] {entry.getKey(), rawString.pruneStep(0)}));
            }
        }
        if (!hasRawString) {
            shardRelatedInTypeParamIndexes = new HashSet<>();
            return;
        }
        tmp.getCurrentParameter().putAll(replace);
        ExecutionContext tmpEc = ec.copy(tmp);

        if (this.tableScan.isNewPartDbTbl()) {
            PartRoutingPlanInfo partRoutingPlanInfo =
                buildPartRoutingPlanInfo(removeSemiJoin(), tableScan.isNewPartDbTbl(), tmpEc);
            PartitionPruneStepVisitor stepVisitor = new PartitionPruneStepVisitor();
            for (PartitionPruneStep step : partRoutingPlanInfo.allPartPruningSteps.values()) {
                stepVisitor.visit(step);
            }
            Set<Integer> shardRelatedInTypeParamIndexes = stepVisitor.getDynamicIndexList();
            shardRelatedInTypeParamIndexes.retainAll(OptimizerUtils.getInParamsIndexes(ec));
            this.shardRelatedInTypeParamIndexes = shardRelatedInTypeParamIndexes;
        } else {
            RexUtils.RexDynamicParamFromComparativeVisitor visitor =
                new RexUtils.RexDynamicParamFromComparativeVisitor();
            Map<String, Comparative> colComparativeMap = this.getComparative(0);
            for (Comparative comparative : colComparativeMap.values()) {
                visitor.go(comparative);
            }
            List<RexDynamicParam> dynamicParams = visitor.getRexDynamicParams();
            Set<Integer> shardRelatedInTypeParamIndexes = new HashSet<>();
            for (RexDynamicParam dynamicParam : dynamicParams) {
                shardRelatedInTypeParamIndexes.add(dynamicParam.getIndex() + 1);
            }
            shardRelatedInTypeParamIndexes.retainAll(OptimizerUtils.getInParamsIndexes(ec));
            this.shardRelatedInTypeParamIndexes = shardRelatedInTypeParamIndexes;
        }
    }

    private List<PartitionPruneStep> buildNewPartPruneSteps(ExecutionContext ec) {
        List<String> logTbNameList = tableScan.getTableNames();
        List<PartitionPruneStep> allPartPruneSteps = new ArrayList<>();
        PartRoutingPlanInfo partRoutingPlanInfo = buildPartRoutingPlanInfo(getPushedRelNode(), true, ec);

        for (int i = 0; i < logTbNameList.size(); i++) {
            String tbName = logTbNameList.get(i);
            PartitionPruneStep partStep = partRoutingPlanInfo.allPartPruningSteps.get(tbName);
            allPartPruneSteps.add(partStep);
        }
        String key = OptimizerUtils.buildInExprKey(ec);
        // cache prune steps for the key
        if (this.allPartPruneSteps.size() < ec.getParamManager().getInt(MAX_IN_PRUNE_CACHE_SIZE) &&
            allPartPruneSteps.size() < ec.getParamManager().getInt(MAX_IN_PRUNE_CACHE_TABLE_SIZE)) {
            this.allPartPruneSteps.put(key, allPartPruneSteps);
        }
        return allPartPruneSteps;
    }

    public RelNode getPushedRelNode() {
        return builder.peek();
    }

    public void pushedIndexHint(RelBuilder builder) {
        RelNode peek = builder.peek();
        if (peek instanceof LogicalTableScan) {
            ((LogicalTableScan) peek).setIndexNode(this.tableScan.getIndexNode());
        }
    }

    public List<Map<String, Comparative>> getComparative() {
        return comparatives;
    }

    public RelShardInfo getRelShardInfo(int tableIndex, ExecutionContext ec) {
        RelShardInfo relShardInfo = new RelShardInfo();
        String logTbName = this.tableScan.getTableNames().get(tableIndex);
        String dbName = this.tableScan.getSchemaName();
        relShardInfo.setTableName(logTbName);
        relShardInfo.setSchemaName(this.tableScan.getSchemaName());
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(dbName);
        if (isNewPartDb) {
            relShardInfo.setPartPruneStepInfo(
                PartitionPruneStepBuilder.genFullScanAllPhyPartsStepInfoByDbNameAndTbName(dbName, logTbName, ec));
        }

        if (!isNewPartDb) {
            relShardInfo.setUsePartTable(false);
            if (comparatives.size() > tableIndex) {
                relShardInfo.setAllComps(comparatives.get(tableIndex));
                relShardInfo.setAllFullComps(fullComparatives.get(tableIndex));
            }
        } else {
            relShardInfo.setUsePartTable(true);
            String key = OptimizerUtils.buildInExprKey(ec);
            if (this.allPartPruneSteps.containsKey(key) && this.allPartPruneSteps.get(key).size() > tableIndex) {
                if (this.allPartPruneSteps.get(key).get(tableIndex) != null) {
                    relShardInfo.setPartPruneStepInfo(this.allPartPruneSteps.get(key).get(tableIndex));
                } else {
                    relShardInfo.setPartPruneStepInfo(
                        PartitionPruneStepBuilder.genFullScanAllPhyPartsStepInfoByDbNameAndTbName(dbName, logTbName,
                            ec));
                }
            } else {
                List<PartitionPruneStep> list = buildNewPartPruneSteps(ec);
                if (list.size() > tableIndex) {
                    relShardInfo.setPartPruneStepInfo(list.get(tableIndex));
                }
            }
        }

        return relShardInfo;
    }

    public Map<String, Comparative> getComparative(int tableIndex) {
        return Optional.of(comparatives).filter(c -> c.size() > tableIndex).map(c -> c.get(tableIndex))
            .orElse(new HashMap<>());
    }

    public List<Map<String, Comparative>> getFullComparatives() {
        return fullComparatives;
    }

    public Map<String, Comparative> getFullComparative(int tableIndex) {
        return Optional.of(fullComparatives).filter(c -> c.size() > tableIndex).map(c -> c.get(tableIndex))
            .orElse(new HashMap<>());
    }

    public RelDataType getPlainRowType() {
        return plainRows.getPlainRowType();
    }

    public TableModify getTableModify() {
        return tableModify;
    }

    private static Pair<Integer, String> defaultRefIndex() {
        return Pair.of(-1, "");
    }

    private static boolean isDefaultRefIndex(Pair<Integer, String> refIndex) {
        return Optional.ofNullable(refIndex).map(r -> r.getKey() == -1).orElse(false);
    }

    private boolean sameRefIndex(Pair<Integer, String> left, Integer rightRefIndex, String rightTableName) {
        return left.getKey().equals(rightRefIndex) && TStringUtil.equalsIgnoreCase(left.getValue(), rightTableName);
    }

    public List<Pair<Integer, String>> getPlainRefIndex() {
        return plainRows.getPlainRefIndex();
    }

    /**
     * 评估下推计划返回的行数
     */
    public Double estimateRowCount(RelMetadataQuery mq) {
        return getPushedRelNode().estimateRowCount(mq);
    }

    public RelBuilder getBuilder() {
        return builder;
    }

    public void setNativeSqlNode(SqlNode nativeSqlNode) {
        this.nativeSqlNodeHintCache = nativeSqlNode;
    }

    public void setPlainRowType(RelDataType plainRowType) {
        this.plainRows = plainRows.setPlainRowType(plainRowType);
    }

    public RelDataType buildCurRowType() {
        return plainRows.buildCurRowType(
            tableScan.getCluster().getTypeFactory());
    }

    public PlainRows getPlainRows() {
        return plainRows;
    }

    public DbType getDbType() {
        return dbType;
    }

    /**
     * If there's some functions that can't be pushed down, calculated it.
     */
    private RelNode replaceCallWithLiteral(RelNode relNode, ReplaceCallWithLiteralVisitor visitor) {
        return relNode.accept(visitor);
    }

    public void rebuildPartRoutingPlanInfo() {
        PartRoutingPlanInfo partRoutingPlanInfo =
            buildPartRoutingPlanInfo(removeSemiJoin(), tableScan.isNewPartDbTbl(),
                PlannerContext.getPlannerContext(getPushedRelNode()).getExecutionContext());
        updatePartRoutingPlanInfo(partRoutingPlanInfo);
    }

    public boolean couldDynamicPruning() {
        getShardRelatedInTypeParamIndexes();
        return shardRelatedInTypeParamIndexes != null && shardRelatedInTypeParamIndexes.size() > 0;
    }

    public boolean dynamicPruningContainsPartitionKey() {
        if (allPartPruneSteps == null || allPartPruneSteps.size() == 0) {
            return true;
        } else if (!allPartPruneSteps.containsKey(OptimizerUtils.EMPTY_KEY)) {
            for (Map.Entry<String, List<PartitionPruneStep>> pruneSteps : allPartPruneSteps.entrySet()) {
                if (!pruneSteps.getKey().equals(OptimizerUtils.EMPTY_KEY)) {
                    for (PartitionPruneStep pruneStep : pruneSteps.getValue()) {
                        // TODO(siyun): to be optimized for PartitionPruneStepCombine
                        if (pruneStep.getStepType() != PartPruneStepType.PARTPRUNE_OP_MISMATCHED_PART_KEY) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * For MySQL CAST function only few type are permitted
     * <p>
     * <pre>
     * @see <a href=
     *     "https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html">https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html</a>
     * </pre>
     */
    private static class RexCastRemover extends RexShuttle {

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.getKind() == SqlKind.CAST) {
                switch (call.getType().getSqlTypeName()) {
                case BINARY:
                case CHAR:
                case DATE:
                case DATETIME:
                case DECIMAL:
                case SIGNED:
                case TIME:
                case UNSIGNED:
                case JSON:
                    // supported by mysql
                    break;
                default:
                    return call.getOperands().get(0).accept(this);
                } // end of switch
            } // end of if
            return super.visitCall(call);
        }
    }

    private static class RelCastRemover extends RelShuttleImpl {

        @Override
        public RelNode visit(LogicalProject project) {
            boolean updated = false;
            List<RexNode> newExprs = new LinkedList<>();
            for (RexNode expr : project.getChildExps()) {
                RexNode newExpr = expr.accept(new RexCastRemover());

                if (newExpr != expr) {
                    updated |= true;
                }

                newExprs.add(newExpr);
            } // end of for

            if (updated) {
                return project.copy(project.getTraitSet(), project.getInput(), newExprs, project.getRowType());
            }

            return super.visit(project);
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            RexNode condition = join.getCondition();
            RexNode newCondition = condition.accept(new RexCastRemover());

            if (condition != newCondition) {
                return join.copy(join.getTraitSet(), newCondition, join.getLeft(), join.getRight(), join.getJoinType(),
                    join.isSemiJoinDone());
            } else {
                return super.visit(join);
            }
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            RexNode condition = filter.getCondition();
            RexNode newCondition = condition.accept(new RexCastRemover());
            if (condition != newCondition) {
                return filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
            } else {
                return super.visit(filter);
            }
        }

        /**
         * Visits a particular child of a parent.
         */
        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            RelNode rel = super.visitChild(parent, i, child);
            if (!(rel instanceof Project)) {
                RelUtils.changeRowType(rel, null);
            }
            return rel;
        }

    }

    protected static class PartRoutingPlanInfo {
        protected boolean usePartitionTable = false;

        /**
         * key: table name
         * val( Map<String, Comparative> ): the pruning info (Comparative tree) of table
         * key: shard column
         * val: the comparative tree of shard column
         */
        protected Map<String, Map<String, Comparative>> allComps = new HashMap<>();
        protected Map<String, Map<String, Comparative>> allFullComps = new HashMap<>();
        /**
         * key: table name
         * val: the pruning plan info of table
         */
        protected Map<String, PartitionPruneStep> allPartPruningSteps = null;

        public PartRoutingPlanInfo() {
        }
    }

    protected PartRoutingPlanInfo buildPartRoutingPlanInfo(RelNode relPlan, boolean usePartitionTable,
                                                           ExecutionContext ec) {

        PartRoutingPlanInfo pruningPlanInfo = new PartRoutingPlanInfo();
        if (!usePartitionTable) {
            final Map<String, Map<String, Comparative>> allComps = new HashMap<>();
            final Map<String, Map<String, Comparative>> allFullComps = new HashMap<>();
            final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(relPlan).extract();
            er.allCondition(allComps, allFullComps, PlannerContext.getPlannerContext(relPlan).getExecutionContext());
            pruningPlanInfo.allComps = allComps;
            pruningPlanInfo.allFullComps = allFullComps;
            pruningPlanInfo.usePartitionTable = false;
        } else {
            ExtractionResult er = ConditionExtractor.predicateFrom(relPlan).extract();
            Map<String, PartitionPruneStep> allPartPruningSteps = er.allPartPruneSteps(ec);
            pruningPlanInfo.usePartitionTable = true;
            pruningPlanInfo.allPartPruningSteps = allPartPruningSteps;
        }
        return pruningPlanInfo;
    }

    protected void updatePartRoutingPlanInfo(PartRoutingPlanInfo newPruningPlanInfo) {
        if (!newPruningPlanInfo.usePartitionTable) {
            updateComparative(newPruningPlanInfo.allComps, newPruningPlanInfo.allFullComps, new AtomicInteger());
        } else {
            updatePartPruneSteps(newPruningPlanInfo.allPartPruningSteps);
        }
        ExecutionContext ec = PlannerContext.getPlannerContext(tableScan).getExecutionContext();
    }

    class PlainRefCalculator extends RelShuttleImpl {
        private PlainRows nodePlainRows;
        RelDataTypeFactory factory;

        public PlainRefCalculator(RelNode node) {
            this.factory = node.getCluster().getTypeFactory();
        }

        @Override
        public RelNode visit(LogicalProject project) {
            super.visit(project);
            nodePlainRows = nodePlainRows.updateRefIndex(project);
            return project;
        }

        @Override
        public RelNode visit(LogicalAggregate agg) {
            super.visit(agg);
            plainRows = nodePlainRows.updateRefIndex(agg);
            return agg;
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            visitChild(join, 1, join.getInput(1));
            RelDataType rightType = nodePlainRows.buildCurRowType(factory);
            PlainRows rightPlainRows = nodePlainRows;

            visitChild(join, 0, join.getInput(0));
            RelDataType leftType = nodePlainRows.buildCurRowType(factory);

            nodePlainRows = nodePlainRows.updateRefIndexForJoin(
                factory,
                leftType,
                rightType,
                rightPlainRows);
            return join;
        }

        @Override
        public RelNode visit(LogicalSemiJoin semiJoin) {
            visitChild(semiJoin, 1, semiJoin.getInput(1));
            RelDataType rightType = nodePlainRows.buildCurRowType(factory);
            PlainRows rightPlainRows = nodePlainRows;

            visitChild(semiJoin, 0, semiJoin.getInput(0));
            RelDataType leftType = nodePlainRows.buildCurRowType(factory);

            nodePlainRows = nodePlainRows.updateRefIndexForSemiJoin(
                semiJoin,
                factory,
                leftType,
                rightType,
                rightPlainRows);
            return semiJoin;
        }

        @Override
        public RelNode visit(TableScan scan) {
            this.nodePlainRows = new PlainRows(scan.getRowType(), Util.last(scan.getTable().getQualifiedName()));
            return tableScan;
        }
    }

    public void calculateRowType() {
        PlainRefCalculator calculator = new PlainRefCalculator(getPushedRelNode());
        getPushedRelNode().accept(calculator);
        this.plainRows = calculator.nodePlainRows;
    }

    public int getRefByColumnName(String tableName, String columnName, boolean last, boolean ignoreDerive) {
        return plainRows.getRefByColumnName(tableName, columnName, last, ignoreDerive, getPushedRelNode());
    }

    class PlainRows {

        /**
         * <pre>
         * 用来解决 tb_a a JOIN tb_a b 时 Calcite 自动修改为 b 中的同名列改名的问题
         * 改名的代码在：SqlValidatorUtil.addFields()
         * 解决思路：提前存一份没改过名字的列信息, 同时建立列与表的关系，用于通过列名查找拆分键序号
         *
         * plainRowType: store the original field name rowtype
         * plainRefRowType: store the current field name rowtype
         * plainRefIndex: store index to the plainRowType, List[Pair(refIndex, tableName)]
         * </pre>
         */

        private final RelDataType plainRowType;
        private final RelDataType plainRefRowType;
        private final List<Pair<Integer, String>> plainRefIndex;

        public PlainRows(RelDataType rowType, String tableName) {
            this.plainRowType = rowType;
            this.plainRefIndex = initPlainRefIndex(plainRowType, tableName);
            this.plainRefRowType = null;
        }

        public PlainRows(RelDataType plainRowType, RelDataType plainRefRowType,
                         List<Pair<Integer, String>> plainRefIndex) {
            this.plainRowType = plainRowType;
            this.plainRefRowType = plainRefRowType;
            this.plainRefIndex = plainRefIndex;
        }

        private List<Pair<Integer, String>> initPlainRefIndex(RelDataType rowType, String tableName) {
            List<Pair<Integer, String>> newPlainRefIndex = new ArrayList<>(rowType.getFieldCount());
            for (int i = 0; i < rowType.getFieldCount(); i++) {
                newPlainRefIndex.add(Pair.of(i, tableName));
            }
            return newPlainRefIndex;
        }

        public RelDataType getPlainRowType() {
            return plainRowType;
        }

        public List<Pair<Integer, String>> getPlainRefIndex() {
            return plainRefIndex;
        }

        public PlainRows setPlainRowType(RelDataType plainRowType) {
            return new PlainRows(plainRowType,
                this.plainRefRowType,
                this.plainRefIndex);
        }

        private Pair<Integer, String> getRefIndex(List<Pair<Integer, String>> plainRefIndex, RexNode expr) {
            if (expr.getKind() == SqlKind.CAST) {
                return getRefIndex(plainRefIndex, ((RexCall) expr).getOperands().get(0));
            }

            if (expr instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) expr;
                return plainRefIndex.get(inputRef.getIndex());
            }

            return defaultRefIndex();
        }

        /**
         * 根据 Project 的投影列,更新输出结果对于对底层列的引用序号
         */
        private PlainRows updateRefIndex(LogicalProject project) {
            List<RexNode> exprs = project.getChildExps();
            List<Pair<Integer, String>> newRefIndex = new ArrayList<>(exprs.size());
            for (RexNode expr : exprs) {
                newRefIndex.add(getRefIndex(plainRefIndex, expr));
            }

            return new PlainRows(
                this.getPlainRowType(),
                project.getRowType(),
                newRefIndex);
        }

        private PlainRows updateRefIndex(LogicalAggregate agg) {
            List<Pair<Integer, String>> newRefIndex = new ArrayList<>();
            for (int ref : agg.getGroupSet()) {
                newRefIndex.add(plainRefIndex.get(ref));
            }

            for (int i = 0; i < agg.getAggCallList().size(); i++) {
                newRefIndex.add(defaultRefIndex());
            }

            return new PlainRows(
                this.getPlainRowType(),
                agg.getRowType(),
                newRefIndex);
        }

        private PlainRows updateRefIndexForJoin(
            RelDataTypeFactory factory,
            RelDataType leftType,
            RelDataType rightType,
            PlainRows rightPlainRows) {
            RelDataType newPlainType = deriveJoinType(factory, leftType, rightType);
            return new PlainRows(
                newPlainType,
                newPlainType,
                extendPlainRefIndex(rightPlainRows.getPlainRefIndex()));

        }

        private PlainRows updateRefIndexForSemiJoin(
            LogicalSemiJoin semiJoin,
            RelDataTypeFactory factory,
            RelDataType leftType,
            RelDataType rightType,
            PlainRows rightPlainRows) {
            RelDataType newPlainType;
            List<Pair<Integer, String>> newPlainRefIndex;
            if (semiJoin.getJoinType() == JoinRelType.LEFT) {
                newPlainRefIndex = extendPlainRefIndex(rightPlainRows.getPlainRefIndex().subList(0, 1));
                newPlainType =
                    SqlValidatorUtil.createLeftSemiJoinType(factory, leftType, rightType, null,
                        semiJoin.getSystemFieldList(), 1);
            } else {
                newPlainRefIndex = extendPlainRefIndex(new ArrayList<>());
                newPlainType = leftType;
            }
            return new PlainRows(
                newPlainType,
                newPlainType,
                newPlainRefIndex);
        }

        private List<Pair<Integer, String>> extendPlainRefIndex(
            List<Pair<Integer, String>> refIndex) {
            List<Pair<Integer, String>> newPlainRefIndex = new ArrayList<>();

            IntStream.range(0, this.plainRefIndex.size()).forEach(index -> {
                final Pair<Integer, String> p = this.plainRefIndex.get(index);
                if (isDefaultRefIndex(p)) {
                    newPlainRefIndex.add(defaultRefIndex());
                } else {
                    newPlainRefIndex.add(Pair.of(index, p.getValue()));
                }
            });

            final int offset = this.plainRefIndex.size();

            IntStream.range(0, refIndex.size()).forEach(index -> {
                final Pair<Integer, String> p = refIndex.get(index);
                if (isDefaultRefIndex(p)) {
                    newPlainRefIndex.add(defaultRefIndex());
                } else {
                    newPlainRefIndex.add(Pair.of(offset + index, p.getValue()));
                }
            });

            return newPlainRefIndex;
        }

        /**
         * @param tableName table name of the column
         * @param columnName name of the column
         * @param last search from head or from tail
         * @param ignoreDerive ignore derived column or not
         * @param node root of relNode tree
         * @return search the serial number of column in the relNode tree
         */
        public int getRefByColumnName(String tableName, String columnName, boolean last,
                                      boolean ignoreDerive, RelNode node) {
            Preconditions.checkArgument(StringUtils.isNotEmpty(columnName));
            RelMetadataQuery metadataQuery = node.getCluster().getMetadataQuery();
            boolean oldIgnoreProjectDeriveOriginColumn = metadataQuery.isIgnoreProjectDeriveOriginColumn();
            try {
                metadataQuery.setIgnoreProjectDeriveOriginColumn(true);
                List<RelDataTypeField> fields = plainRowType.getFieldList();
                int loc;
                for (int i = 0; i < plainRowType.getFieldCount(); i++) {
                    // search from head or from tail
                    loc = last ? plainRowType.getFieldCount() - 1 - i : i;
                    if (!columnName.equalsIgnoreCase(fields.get(loc).getName())) {
                        continue;
                    }

                    for (int j = 0; j < plainRefIndex.size(); j++) {
                        if (!sameRefIndex(plainRefIndex.get(j), loc, tableName)) {
                            continue;
                        }
                        if (ignoreDerive) {
                            if (metadataQuery.getColumnOrigin(node, j) != null) {
                                return j;
                            }
                        } else {
                            final Set<RelColumnOrigin> origins = metadataQuery.getColumnOrigins(node, j);
                            if (origins != null && origins.size() == 1) {
                                return j;
                            }
                        }
                    }
                }
                // 没有找到
                return -1;
            } finally {
                metadataQuery.setIgnoreProjectDeriveOriginColumn(oldIgnoreProjectDeriveOriginColumn);
            }
        }

        public RelDataType buildCurRowType(
            RelDataTypeFactory factory) {
            return buildCurRowType(factory,
                this.plainRowType,
                this.plainRefRowType,
                this.plainRefIndex
            );
        }

        PlainRows copy() {
            if (this.plainRefIndex != null) {
                List<Pair<Integer, String>> newRefIndex = new ArrayList<>(plainRefIndex.size());
                for (Pair<Integer, String> pair : this.plainRefIndex) {
                    newRefIndex.add(Pair.of(pair.getKey(), pair.getValue()));
                }
                return new PlainRows(
                    this.plainRowType,
                    this.plainRefRowType,
                    newRefIndex);
            }

            return new PlainRows(
                this.plainRowType,
                this.plainRefRowType,
                null);
        }

        public RelDataType buildCurRowType(
            RelDataTypeFactory factory,
            RelDataType plainRowType,
            RelDataType plainRefRowType,
            List<Pair<Integer, String>> plainRefIndex) {
            List<RelDataTypeField> fieldList = new ArrayList<>(plainRefIndex.size());

            for (int i = 0; i < plainRefIndex.size(); i++) {
                int value = plainRefIndex.get(i).getKey();
                final RelDataTypeField field;
                if (value == -1) {
                    field = plainRefRowType.getFieldList().get(i);
                } else {
                    field = plainRowType.getFieldList().get(value);
                }
                fieldList.add(new RelDataTypeFieldImpl(field.getName(), i, field.getType()));
            }
            return factory.createStructType(fieldList);
        }
    }

    public Set<Integer> getShardRelatedInTypeParamIndexes() {
        if (shardRelatedInTypeParamIndexes == null) {
            buildShardRelatedInTypeParamIndexes(PlannerContext.getPlannerContext(tableScan).getExecutionContext());
        }
        return shardRelatedInTypeParamIndexes;
    }
}
