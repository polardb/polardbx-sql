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
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinSemiJoinTransposeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.RuleToUse;
import com.alibaba.polardbx.optimizer.core.planner.rule.SemiJoinCorrToSubQueryRule;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
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
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

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

    private RelDataType plainRowType;
    private RelDataType plainRefRowType;
    private List<Pair<Integer, String>> plainRefIndex;

    /**
     * index of list: the table index of logical table in the LV
     * Map<String, Comparative>: the comp info of the the index of  logical table in LV
     * key: shard column;
     * val: the comp info of the column
     */
    private List<Map<String, Comparative>> comparatives = new ArrayList<>();
    private List<Map<String, Comparative>> fullComparatives = new ArrayList<>();
    private final ExecutionContext ec;

    /**
     * index of list: the table index of logical table in the LV
     * PartitionPruneStep : the part prune step info of the the index of logical table in LV
     */
    private List<PartitionPruneStep> allPartPruneSteps = new ArrayList<>();

    public PushDownOpt(LogicalView tableScan, DbType dbType, ExecutionContext ec) {
        this.tableScan = tableScan;
        this.dbType = dbType;
        this.aggIsPushed = false;
        this.ec = ec;
        this.relOptSchema = RelUtils.buildCatalogReader(tableScan.getSchemaName(), ec);
        this.builder = createBuilderAndScan(relOptSchema);
        this.plainRowType = tableScan.getRowType();
        initPlainRefIndex(plainRowType, tableScan.getLogicalTableName());
    }

    public PushDownOpt(LogicalView tableScan, RelNode rel, DbType dbType, ExecutionContext ec) {
        this.tableScan = tableScan;
        this.dbType = dbType;
        this.ec = ec;
        this.relOptSchema = RelUtils.buildCatalogReader(tableScan.getSchemaName(), ec);
        this.builder = createBuilder(relOptSchema);
        this.builder.push(rel);
        this.plainRowType = rel.getRowType();
        initPlainRefIndex(plainRowType, tableScan.getLogicalTableName());
    }

    private PushDownOpt(PushDownOpt pushDownOpt, ExecutionContext ec) {
        this.ec = ec;
        this.tableScan = pushDownOpt.tableScan;
        this.dbType = pushDownOpt.dbType;
        this.relOptSchema = pushDownOpt.relOptSchema;
        this.builder = pushDownOpt.builder;
        this.aggIsPushed = pushDownOpt.aggIsPushed;
        this.nativeSqlNodeHintCache = pushDownOpt.nativeSqlNodeHintCache;
        this.tableModify = pushDownOpt.tableModify;
        this.plainRowType = pushDownOpt.plainRowType;
        this.plainRefRowType = pushDownOpt.plainRefRowType;
        this.plainRefIndex = pushDownOpt.plainRefIndex;
        this.comparatives = pushDownOpt.comparatives;
        this.fullComparatives = pushDownOpt.fullComparatives;
        this.allPartPruneSteps = pushDownOpt.allPartPruneSteps;
    }

    public final PushDownOpt copy(LogicalView logicalView, RelNode rel) {
        PushDownOpt newPushDownOpt = new PushDownOpt(this, ec);
        newPushDownOpt.tableScan = logicalView;
        newPushDownOpt.builder = createBuilder(relOptSchema);
        newPushDownOpt.builder.push(rel);
        if (this.plainRefIndex != null) {
            newPushDownOpt.plainRefIndex = new ArrayList<>();
            for (Pair<Integer, String> pair : this.plainRefIndex) {
                newPushDownOpt.plainRefIndex.add(Pair.of(pair.getKey(), pair.getValue()));
            }
        }
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
                newPushDownOpt.buildPartRoutingPlanInfo(logicalView, logicalView.isNewPartDbTbl());
            newPushDownOpt.updatePartRoutingPlanInfo(partRoutingPlanInfo);
        }
        return newPushDownOpt;
    }

    private void initPlainRefIndex(RelDataType rowType, String tableName) {
        plainRefIndex = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            plainRefIndex.add(Pair.of(i, tableName));
        }
    }

    private RelBuilder createBuilder(RelOptSchema relOptSchema) {
        RelBuilder builder = RelBuilder.proto(Contexts.EMPTY_CONTEXT)
            .create(tableScan.getCluster(), relOptSchema);
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
            builder.flashback(this.tableScan.getFlashback());
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
            if (!relNode.getRowType().equals(plainRowType)) {
                updateRefIndex(project);
            }
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
            updateRefIndex(agg);
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
     * @param relNode root of the tree to be transformed
     * @return the root of a tree without semiJoin and correlate
     */
    private RelNode semiJoinCorrToSubQuery(RelNode relNode) {

        // semi join could not exist when ENABLE_LV_SUBQUERY_UNWRAP is disabled
        // thus there is no need to transform
        if (!PlannerContext.getPlannerContext(relNode).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP)) {
            return relNode;
        }

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
        builder.addRuleInstance(SemiJoinCorrToSubQueryRule.SEMI_JOIN);
        builder.addGroupEnd();

        builder.addGroupBegin();
        builder.addRuleInstance(FilterMergeRule.INSTANCE);
        builder.addRuleInstance(FilterConditionSimplifyRule.INSTANCE);
        builder.addRuleInstance(ProjectRemoveRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build());
        planner.stopOptimizerTrace();
        planner.setRoot(relNode);
        RelNode optimizedNode = planner.findBestExp();

        optimizedNode = optimizedNode.accept(new RelCastRemover());

        return optimizedNode;
    }

    /**
     * return current native sql, may be a middle state
     */
    public SqlNode buildNativeSql(RelToSqlConverter sqlConverter, ReplaceCallWithLiteralVisitor visitor) {
        RelNode relNode = getPushedRelNode();
        relNode = semiJoinCorrToSubQuery(relNode);
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
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
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
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
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
        RelNode optNode =  optimizedNode;
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
        RelDataType leftType = tableScan.buildCurRowType();
        extendPlainRefIndex(rightView.getPlainRefIndex());

        plainRowType = deriveJoinType(cluster.getTypeFactory(), leftType, rightView.buildCurRowType());
        plainRefRowType = plainRowType;

        // push TableScan
        RelNode rPushedNode = rightView.getPushedRelNode();
        builder.push(rPushedNode);
        builder.join(join.getJoinType(), join.getCondition());
    }

    public void pushSemiJoinDirect(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                                   List<RexNode> rightFilters, RelOptCluster cluster) {
        pushSemiJoin(join, rightView, leftFilters, rightFilters, cluster);

        //pass the logicalView of left and right
        LogicalSemiJoin semiJoin = join.copy(
            join.getTraitSet(),
            join.getCondition(),
            builder.build(),
            rightView.getPushedRelNode(),
            join.getJoinType(),
            join.isSemiJoinDone());

        builder.push(semiJoin);
    }

    public void pushSemiJoin(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                             List<RexNode> rightFilters, RelOptCluster cluster) {
        RelDataType leftType = tableScan.buildCurRowType();
        RelDataType rightType = rightView.buildCurRowType();
        if (join.getJoinType() == JoinRelType.LEFT) {
            extendPlainRefIndex(rightView.getPlainRefIndex().subList(0, 1));
            plainRowType = SqlValidatorUtil.createLeftSemiJoinType(
                cluster.getTypeFactory(),
                leftType,
                rightType,
                null,
                join.getSystemFieldList(), 1);
        } else {
            extendPlainRefIndex(new ArrayList<>());
            plainRowType = leftType;
        }
        plainRefRowType = plainRowType;
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
        List<PartitionPruneStep> allPartPruneSteps = new ArrayList<>();
        for (int i = 0; i < logTbNameList.size(); i++) {
            String tbName = logTbNameList.get(i);
            PartitionPruneStep partStep = allPartPruneStepMap.get(tbName);
            if (partStep == null) {
                // just add full table scan. for sql like column subquery
                ExecutionContext ec = PlannerContext.getPlannerContext(tableScan).getExecutionContext();
                partStep =
                    PartitionPruneStepBuilder.generateFullScanPruneStepInfo(tableScan.getSchemaName(),
                        tbName, ec);
            }
            allPartPruneSteps.add(partStep);
        }
        this.allPartPruneSteps = allPartPruneSteps;
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

    public RelShardInfo getRelShardInfo(int tableIndex) {
        RelShardInfo relShardInfo = new RelShardInfo();
        String logTbName = this.tableScan.getTableNames().get(tableIndex);
        String dbName = this.tableScan.getSchemaName();
        relShardInfo.setTableName(logTbName);
        relShardInfo.setSchemaName(this.tableScan.getSchemaName());
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(dbName);
        if (isNewPartDb) {
            relShardInfo
                .setPartPruneStepInfo(PartitionPruneStepBuilder.generateFullScanPruneStepInfo(dbName, logTbName, ec));
        }
        if (comparatives.size() > tableIndex) {
            relShardInfo.setUsePartTable(false);
            relShardInfo.setAllComps(comparatives.get(tableIndex));
            relShardInfo.setAllFullComps(fullComparatives.get(tableIndex));
        } else if (this.allPartPruneSteps.size() > tableIndex) {
            relShardInfo.setUsePartTable(true);
            relShardInfo.setPartPruneStepInfo(this.allPartPruneSteps.get(tableIndex));
        }

        return relShardInfo;
    }

    public Map<String, Comparative> getComparative(int tableIndex) {
        return Optional.of(comparatives)
            .filter(c -> c.size() > tableIndex)
            .map(c -> c.get(tableIndex))
            .orElse(new HashMap<>());
    }

    public List<Map<String, Comparative>> getFullComparatives() {
        return fullComparatives;
    }

    public Map<String, Comparative> getFullComparative(int tableIndex) {
        return Optional.of(fullComparatives)
            .filter(c -> c.size() > tableIndex)
            .map(c -> c.get(tableIndex))
            .orElse(new HashMap<>());
    }

    public RelDataType getPlainRowType() {
        return plainRowType;
    }

    public TableModify getTableModify() {
        return tableModify;
    }

    private Pair<Integer, String> defaultRefIndex() {
        return Pair.of(-1, "");
    }

    private boolean isDefaultRefIndex(Pair<Integer, String> refIndex) {
        return Optional.ofNullable(refIndex).map(r -> r.getKey() == -1).orElse(false);
    }

    private boolean sameRefIndex(Pair<Integer, String> left, Integer rightRefIndex, String rightTableName) {
        return left.getKey().equals(rightRefIndex) && TStringUtil.equalsIgnoreCase(left.getValue(), rightTableName);
    }

    /**
     * 根据 Project 的投影列,更新输出结果对于对底层列的引用序号
     */
    private void updateRefIndex(LogicalProject project) {
        List<RexNode> exprs = project.getChildExps();
        List<Pair<Integer, String>> newRefIndex = new ArrayList<>(exprs.size());
        for (RexNode expr : exprs) {
            newRefIndex.add(getRefIndex(expr));
        }

        plainRefIndex = newRefIndex;
        plainRefRowType = project.getRowType();
    }

    private void updateRefIndex(LogicalAggregate agg) {
        List<Pair<Integer, String>> newRefIndex = new ArrayList<>();
        for (int ref : agg.getGroupSet()) {
            newRefIndex.add(plainRefIndex.get(ref));
        }

        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            newRefIndex.add(defaultRefIndex());
        }

        plainRefIndex = newRefIndex;
        plainRefRowType = agg.getRowType();
    }

    private Pair<Integer, String> getRefIndex(RexNode expr) {
        if (expr.getKind() == SqlKind.CAST) {
            return getRefIndex(((RexCall) expr).getOperands().get(0));
        }

        if (expr instanceof RexInputRef) {
            RexInputRef inputRef = (RexInputRef) expr;
            return plainRefIndex.get(inputRef.getIndex());
        }

        return defaultRefIndex();
    }

    public int getRefByColumnName(String tableName, String columnName, boolean last) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(columnName));
        RelMetadataQuery metadataQuery = this.getPushedRelNode().getCluster().getMetadataQuery();
        boolean oldIgnoreProjectDeriveOriginColumn = metadataQuery.isIgnoreProjectDeriveOriginColumn();
        try {
            metadataQuery.setIgnoreProjectDeriveOriginColumn(true);
            List<RelDataTypeField> fields = plainRowType.getFieldList();
            /**
             * 从后向前找
             */
            if (last) {
                for (int i = plainRowType.getFieldCount() - 1; i >= 0; i--) {
                    if (!columnName.equalsIgnoreCase(fields.get(i).getName())) {
                        continue;
                    }

                    for (int j = 0; j < plainRefIndex.size(); j++) {
                        if (sameRefIndex(plainRefIndex.get(j), i, tableName)) {
                            final Set<RelColumnOrigin> origins = metadataQuery.getColumnOrigins(builder.peek(), j);
                            if (origins == null || origins.size() != 1) {
                                continue;
                            } else {
                                return j;
                            }
                        }
                    }
                }

                // 没有找到
                return -1;
            }

            /**
             * 从前向后找
             */
            for (int i = 0; i < plainRowType.getFieldCount(); i++) {
                if (!columnName.equalsIgnoreCase(fields.get(i).getName())) {
                    continue;
                }

                for (int j = 0; j < plainRefIndex.size(); j++) {
                    if (sameRefIndex(plainRefIndex.get(j), i, tableName)) {
                        final Set<RelColumnOrigin> origins = metadataQuery.getColumnOrigins(builder.peek(), j);
                        if (origins == null || origins.size() != 1) {
                            continue;
                        } else {
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

    public List<Pair<Integer, String>> getPlainRefIndex() {
        return plainRefIndex;
    }

    private void extendPlainRefIndex(List<Pair<Integer, String>> refIndex) {
        List<Pair<Integer, String>> newPlainRefIndex = new ArrayList<>();

        IntStream.range(0, this.plainRefIndex.size()).forEach(index -> {
            final Pair<Integer, String> p = this.plainRefIndex.get(index);
            if (isDefaultRefIndex(p)) {
                newPlainRefIndex.add(defaultRefIndex());
            } else {
                newPlainRefIndex.add(Pair.of(index, p.getValue()));
            }
        });

        final int offset = plainRefIndex.size();

        IntStream.range(0, refIndex.size()).forEach(index -> {
            final Pair<Integer, String> p = refIndex.get(index);
            if (isDefaultRefIndex(p)) {
                newPlainRefIndex.add(defaultRefIndex());
            } else {
                newPlainRefIndex.add(Pair.of(offset + index, p.getValue()));
            }
        });

        this.plainRefIndex = newPlainRefIndex;
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
        this.plainRowType = plainRowType;
    }

    public RelDataType buildCurRowType() {
        RelDataTypeFactory factory = tableScan.getCluster().getTypeFactory();
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

    public Pair<String, String> getTableAndColumnName(int i) {
        Pair<Integer, String> pair = plainRefIndex.get(i);
        int value = pair.getKey();
        String tableName = pair.getValue();
        String columnName;
        if (value == -1) {
            columnName = plainRefRowType.getFieldNames().get(i);
        } else {
            columnName = plainRowType.getFieldNames().get(value);
        }
        return Pair.of(tableName, columnName);
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
            buildPartRoutingPlanInfo(removeSemiJoin(), tableScan.isNewPartDbTbl());
        updatePartRoutingPlanInfo(partRoutingPlanInfo);
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
                return join.copy(join.getTraitSet(),
                    newCondition,
                    join.getLeft(),
                    join.getRight(),
                    join.getJoinType(),
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

    protected PartRoutingPlanInfo buildPartRoutingPlanInfo(RelNode relPlan, boolean usePartitionTable) {

        PartRoutingPlanInfo pruningPlanInfo = new PartRoutingPlanInfo();
        if (!usePartitionTable) {
            final Map<String, Map<String, Comparative>> allComps = new HashMap<>();
            final Map<String, Map<String, Comparative>> allFullComps = new HashMap<>();
            final ExtractionResult er = ConditionExtractor.predicateFrom(relPlan).extract();
            er.allCondition(allComps, allFullComps, ec);
            pruningPlanInfo.allComps = allComps;
            pruningPlanInfo.allFullComps = allFullComps;
            pruningPlanInfo.usePartitionTable = false;
            return pruningPlanInfo;
        } else {
            ExtractionResult er = ConditionExtractor.predicateFrom(relPlan).extract();
            Map<String, PartitionPruneStep> allPartPruningSteps = er.allPartPruneSteps(ec);
            pruningPlanInfo.usePartitionTable = true;
            pruningPlanInfo.allPartPruningSteps = allPartPruningSteps;
            return pruningPlanInfo;
        }
    }

    protected void updatePartRoutingPlanInfo(PartRoutingPlanInfo newPruningPlanInfo) {
        if (!newPruningPlanInfo.usePartitionTable) {
            updateComparative(newPruningPlanInfo.allComps, newPruningPlanInfo.allFullComps, new AtomicInteger());
        } else {
            updatePartPruneSteps(newPruningPlanInfo.allPartPruningSteps);
        }
    }

    public List<PartitionPruneStep> getAllPartPruneSteps() {
        return allPartPruneSteps;
    }

}
