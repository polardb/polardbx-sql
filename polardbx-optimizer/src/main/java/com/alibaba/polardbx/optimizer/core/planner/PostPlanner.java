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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.constants.CpuStatAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.rule.OptimizeModifyReturningRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushModifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyViewBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.TableFinder;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import com.alibaba.polardbx.optimizer.utils.CheckModifyLimitation;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils.LogicalModifyViewBuilder;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RecursiveCTE;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalOutFile;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Util;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static com.alibaba.polardbx.optimizer.core.planner.rule.OptimizeModifyReturningRule.OPTIMIZE_MODIFY_RETURNING_RULES;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSharding;
import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.hasDNHint;
import static com.google.common.util.concurrent.Runnables.doNothing;

/**
 * @author lingce.ldm 2018-02-06 00:06
 */
public class PostPlanner {

    private static PostPlanner INSTANCE = new PostPlanner();

    private PostPlanner() {
    }

    public static PostPlanner getInstance() {
        return INSTANCE;
    }

    public void setSkipPostOptFlag(PlannerContext plannerContext, RelNode optimizedNode,
                                   boolean direct,
                                   boolean shouldSkipPostPlanner) {
        class SkipPostPlanVisitor extends RelShuttleImpl {

            private boolean skipPostPlan = false;

            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof LogicalExpand) {
                    this.skipPostPlan = true;
                    return other;
                } else if (other instanceof SortWindow) {
                    this.skipPostPlan = true;
                    return other;
                } else if (other instanceof HashWindow) {
                    this.skipPostPlan = true;
                    return other;
                } else if (other instanceof RecursiveCTE) {
                    this.skipPostPlan = true;
                    return other;
                } else if (other instanceof LogicalView) {
                    this.skipPostPlan = skipPostPlan || ((LogicalView) other).isInToUnionAll();
                    return visitChildren(other);
                } else {
                    return visitChildren(other);
                }
            }

            public boolean isSkipPostPlan() {
                return skipPostPlan;
            }
        }
        if (direct || shouldSkipPostPlanner || plannerContext.hasLocalIndexHint() || hasDNHint(plannerContext)
        || plannerContext.isHasAutoPagination()) {
            plannerContext.setSkipPostOpt(true);
            return;
        }
        SkipPostPlanVisitor planVisitor = new SkipPostPlanVisitor();
        optimizedNode.accept(planVisitor);
        plannerContext.setSkipPostOpt(planVisitor.isSkipPostPlan());
    }

    public ExecutionPlan optimize(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        boolean enableTaskCpuProfileStat =
            MetricLevel.isSQLMetricEnabled(executionContext.getParamManager().getInt(
                ConnectionParams.MPP_METRIC_LEVEL))
                && executionContext.getRuntimeStatistics() != null;

        if (executionPlan == null) {
            throw new OptimizerException("ExecutionPlan is null.");
        }

        if (!executionPlan.isUsePostPlanner()) {
            return executionPlan;
        }

        RelNode plan = executionPlan.getPlan();
        SqlNode ast = executionPlan.getAst();
        if (!executionPlan.isUsePostPlanner() || skipPostPlanner(executionPlan, executionContext)) {
            if (plan instanceof LogicalRelocate) {
                executionPlan.getPlanProperties().add(ExecutionPlanProperties.MODIFY_CROSS_DB);
            }
            return executionPlan;
        }

        long startBuildDistributedPlanNano = 0;
        CpuStat cpuStat = null;
        if (enableTaskCpuProfileStat) {
            startBuildDistributedPlanNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
            cpuStat = executionContext.getRuntimeStatistics().getCpuStat();
        }

        try {
            /*
              Only single broadcast table
              Choose a random group within current transaction, to avoid limited on the
              first group.
             */
            if (isOnlyBroadcast(executionPlan, executionContext)) {
                DirectTableOperation dto = (DirectTableOperation) plan;
                String dbIndex = getBroadcastTableGroup(executionContext, dto.getSchemaName());
                DirectTableOperation newDto =
                    (DirectTableOperation) dto.copy(dto.getTraitSet(), dto.getInputs());
                newDto.setDbIndex(dbIndex);
                return executionPlan.copy(newDto);
            }

            if (ast == null || !ast.getKind()
                .belongsTo(EnumSet.of(SqlKind.SELECT, SqlKind.DELETE, SqlKind.UPDATE))) {
                return executionPlan;
            }
            final boolean withIndexHint =
                executionPlan.checkProperty(ExecutionPlanProperties.WITH_INDEX_HINT);
            if (withIndexHint) {
                final List<String> originTableNames = executionPlan.getOriginTableNames();
                final List<String> resultTableNames = new ArrayList<>();

                // For one force index hint, there will be one index table referenced
                // If any table lookup is not removed, the table reference count must be bigger than original
                plan.accept(new TableFinder(ts -> {
                    resultTableNames.add(Util.last(ts.getTable().getQualifiedName()));
                    return ts;
                }, true));

                if (originTableNames.size() != resultTableNames.size()) {
                    // if table lookup exists, skip post optimize
                    return executionPlan;
                }
            }

            final ReplaceTableNameWithQuestionMarkVisitor visitor =
                new ReplaceTableNameWithQuestionMarkVisitor(executionContext.getSchemaName(),
                    withIndexHint,
                    executionContext);
            final SqlNode sqlTemplate = ast.accept(visitor);

            final List<String> logTableNames = new ArrayList<>(visitor.getTableNames());

            boolean forceAllowFullTableScan = executionContext.getParamManager().getBoolean(
                ConnectionParams.ALLOW_FULL_TABLE_SCAN) || executionPlan.isExplain();

            List<String> schemaNamesOfPlan = new ArrayList<String>();

            // Optimize modify on topN
            final List<RelNode> modifyTopNOperands = new ArrayList<>();
            final boolean isModifyTopN =
                isModifyTopNCanBeOptimizedByReturning(executionPlan, modifyTopNOperands, executionContext);

            final boolean cachePartPrunedResult = isModifyTopN;
            final Map<String, List<PartPrunedResult>> partitionResult = new TreeMap<>(String::compareToIgnoreCase);
            final BiConsumer<String, PartPrunedResult> partitionResultConsumer = cachePartPrunedResult ?
                (tableName, partResult) -> partitionResult
                    .computeIfAbsent(tableName, (k) -> new ArrayList<>())
                    .add(partResult)
                : null;

            // Build target tables
            Map<String, List<List<String>>> targetTables = getTargetTablesAndSchemas(logTableNames,
                executionPlan,
                executionContext,
                schemaNamesOfPlan,
                forceAllowFullTableScan,
                false,
                partitionResultConsumer);

            boolean canPushdown = (schemaNamesOfPlan.size() == 1);
            if (ScaleOutPlanUtil.isEnabledScaleOut(executionContext.getParamManager())
                && ast.getKind()
                .belongsTo(EnumSet.of(SqlKind.INSERT, SqlKind.DELETE, SqlKind.UPDATE))
                && executionPlan.getTableSet() != null) {
                Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
                List<TableMeta> tableMetas = new ArrayList<>();
                for (Pair<String, String> tableInfo : tableSet) {
                    TableMeta tableMeta = executionContext.getSchemaManager(tableInfo.getKey())
                        .getTable(tableInfo.getValue());
                    tableMetas.add(tableMeta);
                    boolean isNewPart =
                        DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
                    if (isNewPart) {
                        canPushdown &= (!tableMeta.getPartitionInfo().isBroadcastTable());
                    } else {
                        TableRule tableRule =
                            executionContext.getSchemaManager(tableMeta.getSchemaName())
                                .getTddlRuleManager()
                                .getTableRule(tableMeta.getTableName());
                        canPushdown &= (!tableRule.isBroadcast());
                    }
                }
            }

            boolean isAllAtOnePhyTb = allAtOnePhyTable(targetTables, logTableNames.size());
            if (plan instanceof LogicalModifyView) {
                if (((LogicalModifyView) plan).getHintContext() != null
                    && ((LogicalModifyView) plan).getHintContext().containsInventoryHint()
                    && !isAllAtOnePhyTb) {
                    executionPlan.getPlanProperties().add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                }
                if (!RelUtils.dmlWithDerivedSubquery(plan, ast)) {
                    return executionPlan;
                }
            }
            if (canPushdown && isAllAtOnePhyTb && ast.isA(SqlKind.DML)) {
                final String schemaNamesOfAst = schemaNamesOfPlan.get(0);
                boolean withGsi = true;
                boolean needRelicateWrite = true;
                boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaNamesOfAst);
                String groupName = targetTables.keySet().iterator().next();
                boolean hasGeneratedColumn = false;
                for (String tableName : logTableNames) {
                    withGsi &=
                        GlobalIndexMeta.hasGsi(tableName, schemaNamesOfAst, executionContext);
                    hasGeneratedColumn |=
                        GeneratedColumnUtil.containLogicalGeneratedColumn(schemaNamesOfAst,
                            tableName,
                            executionContext);
                    TableMeta tableMeta =
                        executionContext.getSchemaManager(schemaNamesOfAst).getTable(tableName);
                    if (!isNewPart) {
                        needRelicateWrite &= ComplexTaskPlanUtils
                            .canWrite(tableMeta, groupName);
                    } else {
                        for (List<String> phyTable : targetTables.get(groupName)) {
                            String partName = ComplexTaskPlanUtils
                                .getPartNameFromGroupAndTable(tableMeta, groupName,
                                    phyTable.get(0));
                            needRelicateWrite &= ComplexTaskPlanUtils
                                .canWrite(tableMeta, partName);
                            if (needRelicateWrite) {
                                break;
                            }
                        }

                    }
                    canPushdown &= !withGsi & !needRelicateWrite & !hasGeneratedColumn;
                }
            }

            // handle optimize modify top n by returning

            canPushdown &= !RelUtils.existUnPushableLastInsertId(executionPlan);
            canPushdown &= isAllAtOnePhyTb && !existUnPushableRelNode(plan);
            if (canPushdown) {
                String schemaNamesOfAst = schemaNamesOfPlan.get(0);
                switch (ast.getKind()) {

                case SELECT:
                    /**
                     * All at one group and only have one PhyTable per table.
                     */
                    PhyTableScanBuilder builder = new PhyTableScanBuilder((SqlSelect) sqlTemplate,
                        targetTables,
                        executionContext,
                        plan,
                        DbType.MYSQL,
                        plan.getRowType(),
                        schemaNamesOfAst,
                        logTableNames,
                        true,
                        true,
                        null);
                    builder.setUnionSize(0);
                    List<RelNode> phyTableScans = builder.build(executionContext);
                    RelNode ret = phyTableScans.get(0);
                    return executionPlan.copy(ret);
                case DELETE: {
                    TableModify tableModify = null;
                    if (plan instanceof LogicalModifyView) {
                        tableModify = ((LogicalModifyView) plan).getTableModify();
                    } else {
                        tableModify = (TableModify) plan;
                    }

                    if (tableModify.isDelete() && logTableNames.size() > 1
                        && CheckModifyLimitation.checkModifyBroadcast(tableModify, doNothing())) {
                        if (executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                            executionPlan = executionPlan.copy(executionPlan.getPlan());
                            executionPlan.getPlanProperties()
                                .add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                            return executionPlan;
                        } else {
                            throw new TddlNestableRuntimeException(
                                "multi delete not support broadcast");
                        }
                    }

                    final PhyTableModifyViewBuilder modifyViewBuilder =
                        new PhyTableModifyViewBuilder(sqlTemplate,
                            targetTables, executionContext.getParamMap(), plan, DbType.MYSQL,
                            logTableNames,
                            schemaNamesOfAst);
                    modifyViewBuilder.setBuildForPushDownOneShardOnly(true);
                    final List<RelNode> phyTableModifies =
                        modifyViewBuilder.build(executionContext);
                    return executionPlan.copy(phyTableModifies.get(0));
                }
                case UPDATE: {
                    TableModify tableModify = null;
                    if (plan instanceof LogicalModifyView) {
                        tableModify = ((LogicalModifyView) plan).getTableModify();
                    } else {
                        tableModify = (TableModify) plan;
                    }

                    if (tableModify.isUpdate() && logTableNames.size() > 1
                        && CheckModifyLimitation.checkModifyBroadcast(tableModify, doNothing())) {
                        if (executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                            executionPlan = executionPlan.copy(executionPlan.getPlan());
                            executionPlan.getPlanProperties()
                                .add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                            return executionPlan;
                        } else {
                            throw new TddlNestableRuntimeException(
                                "multi update not support broadcast");
                        }
                    }

                    final PhyTableModifyViewBuilder modifyViewBuilder =
                        new PhyTableModifyViewBuilder(sqlTemplate,
                            targetTables,
                            executionContext.getParamMap(),
                            plan,
                            DbType.MYSQL,
                            logTableNames,
                            schemaNamesOfAst);
                    modifyViewBuilder.setBuildForPushDownOneShardOnly(true);
                    final List<RelNode> phyTableModifies =
                        modifyViewBuilder.build(executionContext);
                    return executionPlan.copy(phyTableModifies.get(0));
                }
                default:
                } // end of switch

            } else {
                // Check pruned partition is ordered by partition key
                final boolean pushModifyTopN = isModifyTopN
                    && modifyTopNOperands.size() > 1
                    && PartitionUtils.checkPrunedPartitionMonotonic(
                    (LogicalView) modifyTopNOperands.get(2),
                    partitionResult.values().iterator().next());
                if (pushModifyTopN) {
                    if (!isAllAtOnePhyTb) {
                        executionPlan.getPlanProperties().add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                    }
                    return executionPlan.copy(pushdownModifyOnTopN(modifyTopNOperands));
                }

                /*
                 * Maybe some tables at one group
                 */
                if (ast.getKind() == SqlKind.DELETE) {
                    if (GeneralUtil.isNotEmpty(targetTables) && RelUtils.dmlWithDerivedSubquery(
                        plan, ast)) {
                        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
                        final LogicalModifyView lmv = (LogicalModifyView) plan;
                        lmv.setTargetTables(targetTables);
                        lmv.setSqlTemplate(ast);
                        return executionPlan.copy(lmv);
                    } else if (!executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                        throw new TddlNestableRuntimeException("not support cross db delete");
                    } else if (plan instanceof LogicalModify
                        && ((LogicalModify) plan).isWithoutPk()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                            String.join(",", ((LogicalModify) plan).getTargetTableNames()));
                    } else {
                        if (isAllAtOnePhyTb) {
                            final List<RelNode> bindings = new ArrayList<>();
                            final LogicalModifyViewBuilder lmvBuilder =
                                isLogicalMultiWriteCanBeOptimizedByReturning(executionPlan, bindings, executionContext);

                            if (lmvBuilder != null) {
                                final LogicalModify modify = (LogicalModify) bindings.get(0);
                                modify.getMultiWriteInfo().setLmvBuilder(lmvBuilder);
                                modify.getMultiWriteInfo().setOptimizeByReturning(true);
                            }
                        }
                        executionPlan = executionPlan.copy(executionPlan.getPlan());
                        executionPlan.getPlanProperties()
                            .add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                    }
                } else if (ast.getKind() == SqlKind.UPDATE) {
                    if (GeneralUtil.isNotEmpty(targetTables) && RelUtils.dmlWithDerivedSubquery(
                        plan, ast)) {
                        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
                        final LogicalModifyView lmv = (LogicalModifyView) plan;
                        lmv.setTargetTables(targetTables);
                        lmv.setSqlTemplate(ast);
                        return executionPlan.copy(lmv);
                    } else if (!executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                        throw new TddlNestableRuntimeException("not support cross db update");
                    } else if (plan instanceof LogicalModify
                        && ((LogicalModify) plan).isWithoutPk()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                            String.join(",", ((LogicalModify) plan).getTargetTableNames()));
                    } else {
                        executionPlan = executionPlan.copy(executionPlan.getPlan());
                        executionPlan.getPlanProperties()
                            .add(ExecutionPlanProperties.MODIFY_CROSS_DB);
                    }
                }
            }
        } finally {
            if (enableTaskCpuProfileStat) {
                cpuStat.addCpuStatItem(CpuStatAttribute.CpuStatAttr.BUILD_DISTRIBUTED_PLAN,
                    ThreadCpuStatUtil.getThreadCpuTimeNano()
                        - startBuildDistributedPlanNano);
            }
        }
        return executionPlan;
    }

    private static LogicalModifyViewBuilder isLogicalMultiWriteCanBeOptimizedByReturning(
        @NotNull ExecutionPlan executionPlan,
        @NotNull List<RelNode> bindings,
        @NotNull ExecutionContext ec) {
        if (!supportOptimizeMultiWriteByReturning(executionPlan, ec)) {
            return null;
        }

        return isPushableLogicalMultiWrite(executionPlan, bindings);
    }

    /**
     * Check xrpc is enabled and returning is supported
     */
    private static boolean supportOptimizeMultiWriteByReturning(@NotNull ExecutionPlan plan,
                                                                @NotNull ExecutionContext ec) {
        if (null == plan.getSchemaNames() || plan.getSchemaNames().size() != 1) {
            // do not support cross schema
            return false;
        }

        final String schemaName = plan.getSchemaNames().iterator().next();

        // check DML_USE_RETURNING enabled
        // check OPTIMIZE_DELETE_BY_RETURNING enabled
        // check returning supported in dn
        // check xrpc enabled
        if (!ec.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING)
            || !ec.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_DELETE_BY_RETURNING)
            || !ec.getStorageInfo(schemaName).isSupportsReturning()
            || !ConnPoolConfigManager.getInstance().getConnPoolConfig().isStorageDbXprotoEnabled()) {
            return false;
        }
        return true;
    }

    /**
     * Validate plan is logical multi write and can be pushdown
     *
     * @param executionPlan plan
     * @param bindings for return binding operands, List{LogicalModify,MergeSort,LogicalView}
     */
    private static LogicalModifyViewBuilder isPushableLogicalMultiWrite(@NotNull ExecutionPlan executionPlan,
                                                                        @NotNull List<RelNode> bindings) {
        final boolean isMultiWriteCanBeOptimizedByReturning = executionPlan.getPlan() instanceof LogicalModify
            && ((LogicalModify) executionPlan.getPlan()).isMultiWriteCanBeOptimizedByReturning();

        if (!isMultiWriteCanBeOptimizedByReturning) {
            return null;
        }

        for (OptimizeModifyReturningRule rule : OPTIMIZE_MODIFY_RETURNING_RULES) {
            if (RelUtils.matchPlan(executionPlan.getPlan(), rule.getOperand(), bindings, null)) {
                return rule;
            }
        }

        return null;
    }

    private static LogicalModifyView pushdownModifyOnTopN(List<RelNode> modifyOnTopNOperands) {
        LogicalModify modify = (LogicalModify) modifyOnTopNOperands.get(0);
        final LogicalView lv = (LogicalView) modifyOnTopNOperands.get(2);
        final LogicalModifyView lmv =
            new LogicalModifyView(lv, modify.getModifyTopNInfo().setOptimizeByReturning(true));
        lmv.push(modify);
        RelUtils.changeRowType(lmv, modify.getRowType());

        return lmv;
    }

    /**
     * Validation for pushdown logicalModify at runtime in case of xrpc is closed manually
     *
     * @return <pre>
     * true means:
     *  1. current plan is a modify-on-topN (order by limit)
     *  2. current plan is possible to be optimized by returning, when partitions to be scanned is monotonic on order by column
     * </pre>
     */
    private static boolean isModifyTopNCanBeOptimizedByReturning(@NotNull ExecutionPlan executionPlan,
                                                                 @NotNull List<RelNode> bindings,
                                                                 @NotNull ExecutionContext ec) {
        if (!supportOptimizeModifyTopNByReturning(executionPlan, ec)) {
            return false;
        }

        return isModifyTopN(executionPlan, bindings);
    }

    /**
     * Check xrpc is enabled and returning is supported
     */
    private static boolean supportOptimizeModifyTopNByReturning(@NotNull ExecutionPlan plan,
                                                                @NotNull ExecutionContext ec) {
        if (null == plan.getSchemaNames() || plan.getSchemaNames().size() != 1) {
            // do not support cross schema
            return false;
        }

        final String schemaName = plan.getSchemaNames().iterator().next();

        // check DML_USE_RETURNING enabled
        // check OPTIMIZE_MODIFY_TOP_N_BY_RETURNING enabled
        // check returning supported in dn
        // check xrpc enabled
        if (!ec.getParamManager().getBoolean(ConnectionParams.DML_USE_RETURNING)
            || !ec.getParamManager().getBoolean(ConnectionParams.OPTIMIZE_MODIFY_TOP_N_BY_RETURNING)
            || !ec.getStorageInfo(schemaName).isSupportsReturning()
            || !ConnPoolConfigManager.getInstance().getConnPoolConfig().isStorageDbXprotoEnabled()) {
            return false;
        }
        return true;
    }

    /**
     * Validate plan is modify on topN
     *
     * @param executionPlan plan
     * @param bindings for return binding operands, List{LogicalModify,MergeSort,LogicalView}
     */
    private static boolean isModifyTopN(@NotNull ExecutionPlan executionPlan, @NotNull List<RelNode> bindings) {

        /*
        * Plan for
        *   update t1 set pad = 11 where id=1 and user_id=1 and create_time<=now() order by create_time desc limit 1;
        * looks like:
        | LogicalModify(TYPE="UPDATE", SET="t1.pad=?0")                                                                                                                                                                                                                                           |
        |   MergeSort(sort="create_time DESC", fetch=?4)                                                                                                                                                                                                                                          |
        |     LogicalView(tables="t1[p16sp202403a,p16sp202403b,p16sp202403c,p16sp202404a]", shardCount=4, sql="SELECT `id`, `user_id`, `pad`, `create_time`, ? FROM `t1` AS `t1` WHERE ((`id` = ?) AND (`user_id` = ?) AND (`create_time` <= ?)) ORDER BY `create_time` DESC LIMIT ? FOR UPDATE")
        */

        // check plan is modify on topN
        return executionPlan.getPlan() instanceof LogicalModify
            && ((LogicalModify) executionPlan.getPlan()).isModifyTopN()
            && RelUtils.matchPlan(executionPlan.getPlan(), PushModifyRule.MERGESORT.getOperand(), bindings, null);
    }

    /**
     * Whether is direct broadcast table plan
     */
    private static boolean isOnlyBroadcast(final ExecutionPlan executionPlan,
                                           final ExecutionContext executionContext) {
        RelNode plan = executionPlan.getPlan();
        if (!PlannerContext.getPlannerContext(plan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_RANDOM_READ)) {
            return false;
        }
        if (!(plan instanceof DirectTableOperation)) {
            return false;
        }
        if (((DirectTableOperation) plan).getLockMode() == SqlSelect.LockMode.EXCLUSIVE_LOCK) {
            return false;
        }
        Set<Pair<String, String>> tables = executionPlan.getTableSet();
        if (GeneralUtil.isNotEmpty(tables)) {
            Pair<String, String> firstTableSet = tables.iterator().next();
            String schemaName = firstTableSet.getKey();
            String tableName = firstTableSet.getValue();
            if (schemaName == null) {
                schemaName = executionContext.getSchemaName();
            }
            // when scale-out in progress don't enable broadcast random read
            TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
            if (tableMeta.getComplexTaskTableMetaBean() != null && !tableMeta.getComplexTaskTableMetaBean()
                .allPartIsPublic()) {
                return false;
            }

        }
        return executionPlan.getPlanProperties().contains(ExecutionPlanProperties.ONLY_BROADCAST_TABLE);
    }

    /**
     * Only broadcast table:
     * 1. Choose an existed group from held connections if already in a transaction
     * 2. Choose a random group if it's the first statement in transaction
     * 3. Skip single-group if any other groups exist
     */
    public static String getBroadcastTableGroup(ExecutionContext executionContext, String schemaName) {
        ITransaction transaction = executionContext.getTransaction();
        List<String> candidateGroups = null;
        if (transaction != null) {
            candidateGroups = transaction.getConnectionHolder().getHeldGroupsOfSchema(schemaName);
        }
        if (candidateGroups == null || candidateGroups.isEmpty()) {
            candidateGroups = HintUtil.allGroupsWithBroadcastTable(schemaName);
        }

        int allSingleCount =
            (int) candidateGroups.stream().filter(GroupInfoUtil::isSingleGroup).count();
        if (allSingleCount == candidateGroups.size()) {
            return candidateGroups.get(new Random().nextInt(allSingleCount));
        } else {
            int count = candidateGroups.size() - allSingleCount;
            return candidateGroups.stream().filter(x -> !GroupInfoUtil.isSingleGroup(x))
                .skip(new Random().nextInt(count)).findFirst().orElse(null);
        }
    }

    public static boolean skipPostPlanner(final ExecutionPlan executionPlan,
                                          final ExecutionContext executionContext) {
        SqlNode ast = executionPlan.getAst();
        RelNode plan = executionPlan.getPlan();
        if (!PlannerContext.getPlannerContext(plan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_POST_PLANNER)) {
            return true;
        }
        if (isOnlyBroadcast(executionPlan, executionContext)) {
            return false;
        }

        if (PlannerContext.getPlannerContext(plan).isSkipPostOpt()) {
            return true;
        }

        if (plan instanceof LogicalRelocate || plan instanceof BaseTableOperation
            || plan instanceof LogicalRecyclebin || plan instanceof BroadcastTableModify
            || plan instanceof LogicalOutFile) {
            return true;
        }

        if (plan instanceof LogicalModifyView && ((LogicalModifyView) plan).getHintContext() != null
            && ((LogicalModifyView) plan).getHintContext().containsInventoryHint()) {
            return false;
        }

        // mysql 不支持 Update / Delete limit m,n; 不可下推
        if (plan instanceof LogicalModify && ((LogicalModify) plan).getOriginalSqlNode() != null) {
            SqlNode originNode = ((LogicalModify) plan).getOriginalSqlNode();
            if ((originNode instanceof SqlDelete && ((SqlDelete) originNode).getOffset() != null) ||
                (originNode instanceof SqlUpdate && ((SqlUpdate) originNode).getOffset() != null)) {
                return true;
            }
        }

        if (plan instanceof LogicalModify && ((LogicalModify) plan).isModifyForeignKey() ||
            plan instanceof LogicalInsert && ((LogicalInsert) plan).isModifyForeignKey()) {
            return true;
        }

        if (plan instanceof Gather && ((Gather) plan).getInput() instanceof LogicalView) {
            return true;
        }
        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
        return plan instanceof LogicalModifyView && !RelUtils.dmlWithDerivedSubquery(plan, ast);
    }

//    public static boolean dmlWithDerivedSubquery(final RelNode plan, final SqlNode ast) {
//        if (plan instanceof LogicalModifyView) {
//            switch (ast.getKind()) {
//            case UPDATE:
//                return ((SqlUpdate) ast).withSubquery();
//            case DELETE:
//                return ((SqlDelete) ast).withSubquery();
//            default:
//                return false;
//            }
//        }
//        return false;
//    }

    /**
     * Map[分库 List[按下标分组 List[一个物理 SQL 中的所有表]]]
     */
    private Map<String, List<List<String>>> getTargetTablesAndSchemas(List<String> tableNames,
                                                                      ExecutionPlan executionPlan,
                                                                      ExecutionContext executionContext,
                                                                      List<String> schemasToBeReturn,
                                                                      boolean forceAllowFullTableScan,
                                                                      boolean allowMultiSchema,
                                                                      BiConsumer<String, PartPrunedResult> partPrunedResultBiConsumer) {

        Map<Integer, ParameterContext> params =
            executionContext.getParams() == null ? null :
                executionContext.getParams().getCurrentParameter();

        final RelNode plan = executionPlan.getPlan();
        Set<String> schemaNames = executionPlan.getSchemaNames();
        String key = OptimizerUtils.buildInExprKey(executionContext);
        PlanShardInfo planShardInfo = executionPlan.getPlanShardInfo(key);

        ExtractionResult er = null;
        if (planShardInfo == null || schemaNames == null) {
            er = ConditionExtractor.partitioningConditionFrom(plan).extract();
            schemaNames = er.getSchemaNameSet();
        } else {
            er = planShardInfo.getEr();
        }

        String schemaNameOfPlan = null;
        if (schemaNames.size() == 1) {
            // if find only one schema in logical plan, then use it to do sharding and plan pushdown
            schemaNameOfPlan = schemaNames.iterator().next();
        } else if (schemaNames.size() == 0) {
            // if no found any schemas in logical plan, then use the default schema to do sharding and plan pushdown
            schemaNameOfPlan = OptimizerContext.getContext(
                executionContext.getSchemaName()).getSchemaName();
            // build new set to avoid concurrent modify error
            schemaNames = new HashSet<>();
            schemaNames.add(schemaNameOfPlan);
            executionPlan.setSchemaNames(schemaNames);
        } else if (!allowMultiSchema) {
            // if found more than 2 schemas in logical plan, then reject to do plan pushdown
            return new HashMap<>();
        }
        schemasToBeReturn.addAll(schemaNames);

        //Map<String, RelShardInfo> allRelShardInfo = allShardInfos;
        RelShardInfo relShardInfo;
        if (!(plan instanceof DirectTableOperation) && planShardInfo == null) {
            //er.allCondition(allComps, allFullComps);
            planShardInfo = er.allShardInfo(executionContext);
        }

        // AST tableNames must match ExtractionResult tables one by one
        // after GSI index selection, we must check
        if (!checkTableCountMatch(tableNames, er)) {
            return new HashMap<>();
        }

        final List<List<TargetDB>> targetDBs = new ArrayList<>();
        if (!allowMultiSchema) {
            TddlRuleManager tddlRuleManager =
                executionContext.getSchemaManager(schemaNameOfPlan).getTddlRuleManager();
            Map<String, List<List<String>>> result = new HashMap<>();

            // used by shardedTb
            Map<String, Map<String, Comparative>> allComps =
                planShardInfo.getAllTableComparative(schemaNameOfPlan);
            Map<String, Map<String, Comparative>> allFullComps =
                planShardInfo.getAllTableFullComparative(schemaNameOfPlan);

            for (String name : tableNames) {

                // used by partitionedTb
                RelShardInfo shardInfo = planShardInfo.getRelShardInfo(schemaNameOfPlan, name);
                if (!shardInfo.isUsePartTable()) {
                    Map<String, Comparative> comps = allComps.get(name);
                    Map<String, Object> calcParams = new HashMap<>();
                    calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
                    calcParams.put(CalcParamsAttribute.COM_DB_TB, allFullComps);
                    calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE,
                        executionContext.getTimeZone());
                    calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, executionContext);
                    List<TargetDB> tdbs =
                        tddlRuleManager
                            .shard(name, true, forceAllowFullTableScan, comps, params, calcParams,
                                executionContext);
                    targetDBs.add(tdbs);
                } else {
                    // do routing for each partTable
                    PartitionPruneStep stepInfo = shardInfo.getPartPruneStepInfo();
                    PartPrunedResult prunedResult =
                        PartitionPruner.doPruningByStepInfo(stepInfo, executionContext);

                    if (null != partPrunedResultBiConsumer) {
                        partPrunedResultBiConsumer.accept(name, prunedResult);
                    }

                    // covert to targetDbList
                    targetDBs.add(
                        PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(prunedResult));
                }
            }
            result.putAll(PlannerUtils.convertTargetDB(targetDBs, schemaNameOfPlan));
            return result;
        } else {
            Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
            final Map<String, List<String>> schemaTableNamesMap = new HashMap<>();
            Map<String, List<List<String>>> result = new HashMap<>();
            if (GeneralUtil.isNotEmpty(tableSet)) {
                tableSet.forEach(table -> {
                    if (!schemaTableNamesMap.containsKey(table.getKey())) {
                        schemaTableNamesMap.put(table.getKey(), new ArrayList<>());
                    }
                    schemaTableNamesMap.get(table.getKey()).add(table.getValue());
                });
                for (Map.Entry<String, List<String>> entry : schemaTableNamesMap.entrySet()) {
                    String schemaName = entry.getKey();
                    if (schemaName == null) {
                        schemaName = executionContext.getSchemaName();
                    }
                    List<String> tbNameList = entry.getValue();
                    TddlRuleManager tddlRuleManager =
                        executionContext.getSchemaManager(schemaName).getTddlRuleManager();
                    targetDBs.clear();

                    // routed by shardDbTb
                    Map<String, Map<String, Comparative>> allComps =
                        planShardInfo.getAllTableComparative(schemaName);
                    Map<String, Map<String, Comparative>> allFullComps =
                        planShardInfo.getAllTableFullComparative(schemaName);

                    for (String name : tbNameList) {
                        RelShardInfo shardInfo = planShardInfo.getRelShardInfo(schemaName, name);
                        if (!shardInfo.isUsePartTable()) {
                            Map<String, Comparative> comps = allComps.get(name);
                            Map<String, Object> calcParams = new HashMap<>();
                            calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
                            calcParams.put(CalcParamsAttribute.COM_DB_TB, allFullComps);
                            calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE,
                                executionContext.getTimeZone());
                            calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, executionContext);
                            List<TargetDB> tdbs =
                                tddlRuleManager.shard(name, true, forceAllowFullTableScan, comps,
                                    params, calcParams,
                                    executionContext);
                            targetDBs.add(tdbs);
                        } else {

                            // routed partTable
                            PartitionPruneStep stepInfo = shardInfo.getPartPruneStepInfo();
                            PartPrunedResult prunedResult =
                                PartitionPruner.doPruningByStepInfo(stepInfo, executionContext);

                            if (null != partPrunedResultBiConsumer) {
                                partPrunedResultBiConsumer.accept(name, prunedResult);
                            }

                            // covert to targetDbList
                            targetDBs.add(PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(
                                prunedResult));
                        }

                    }
                    result.putAll(PlannerUtils.convertTargetDB(targetDBs, entry.getKey()));
                }
            }
            return result;
        }
    }

    private boolean checkTableCountMatch(List<String> tableNames, ExtractionResult er) {
        Map<RelOptTable, Map<RelNode, Label>> tableMapMap = er.getTableLabelMap();
        Map<String, Integer> countMap = new HashMap<>();
        for (Map.Entry<RelOptTable, Map<RelNode, Label>> entry : tableMapMap.entrySet()) {
            TableMeta tableMeta = CBOUtil.getTableMeta(entry.getKey());
            if (tableMeta == null) {
                return false;
            }
            Integer cnt = countMap.get(tableMeta.getTableName().toLowerCase());
            if (cnt == null) {
                countMap.put(tableMeta.getTableName().toLowerCase(), entry.getValue().size());
            } else {
                countMap.put(tableMeta.getTableName().toLowerCase(), entry.getValue().size() + cnt);
            }
        }

        for (String tableName : tableNames) {
            Integer cnt = countMap.get(tableName.toLowerCase());
            if (cnt != null) {
                cnt -= 1;
                if (cnt > 0) {
                    countMap.put(tableName.toLowerCase(), cnt);
                } else {
                    countMap.remove(tableName.toLowerCase());
                }
            } else {
                return false;
            }
        }

        return countMap.isEmpty();
    }

    /**
     *
     */
    private boolean allAtOnePhyTable(Map<String, List<List<String>>> targetTables, int tableSize) {
        if (targetTables.size() != 1) {
            return false;
        }

        for (List<List<String>> tables : targetTables.values()) {
            if (tables.size() != 1) {
                return false;
            }

            for (List<String> names : tables) {
                if (names.size() != tableSize) {
                    return false;
                }
            }
        }

        return true;
    }

    public static boolean usePostPlanner(ExplainResult explain, boolean usingHint) {
        return !usingHint &&
            !isExplainOptimizer(explain) &&
            !isExplainSharding(explain);
    }

    private boolean existUnPushableRelNode(RelNode node) {
        class CheckUnPushableRelVisitor extends RelVisitor {
            private boolean exists = false;

            @Override
            public void visit(RelNode relNode, int ordinal, RelNode parent) {
                if (relNode instanceof OSSTableScan) {
                    // don't optimize oss table scan in post planner
                    exists = true;
                } else {
                    if (relNode instanceof Project) {
                        exists = PushProjectRule.doNotPush((Project) relNode) ? true : exists;
                    } else if (relNode instanceof Filter) {
                        exists =
                            RexUtil.containsUnPushableFunction(((Filter) relNode).getCondition(),
                                true) ? true :
                                exists;
                    }
                }
                super.visit(relNode, ordinal, parent);
            }

            public boolean exists() {
                return exists;
            }
        }

        final CheckUnPushableRelVisitor checkUnPushableRelVisitor = new CheckUnPushableRelVisitor();
        checkUnPushableRelVisitor.go(node);
        return checkUnPushableRelVisitor.exists();
    }

}
