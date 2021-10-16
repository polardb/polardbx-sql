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
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyViewBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.TableFinder;
import com.alibaba.polardbx.optimizer.exception.OptimizerException;
import com.alibaba.polardbx.optimizer.hint.util.HintUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
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
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSharding;
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

    public void setSkipPostOptFlag(PlannerContext plannerContext, RelNode optimizedNode, boolean direct) {
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
                } else {
                    return visitChildren(other);
                }
            }

            public boolean isSkipPostPlan() {
                return skipPostPlan;
            }
        }

        SkipPostPlanVisitor planVisitor = new SkipPostPlanVisitor();
        optimizedNode.accept(planVisitor);
        plannerContext.setSkipPostOpt(planVisitor.isSkipPostPlan() || direct);
    }

    public ExecutionPlan optimize(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        boolean enableTaskCpuProfileStat =
            MetricLevel.isSQLMetricEnabled(executionContext.getParamManager().getInt(
                ConnectionParams.MPP_METRIC_LEVEL)) && executionContext.getRuntimeStatistics() != null;

        if (executionPlan == null) {
            throw new OptimizerException("ExecutionPlan is null.");
        }

        if (!executionPlan.isUsePostPlanner()) {
            return executionPlan;
        }

        RelNode plan = executionPlan.getPlan();
        SqlNode ast = executionPlan.getAst();
        if (!executionPlan.isUsePostPlanner() || skipPostPlanner(executionPlan)) {
            if (plan instanceof LogicalRelocate) {
                executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
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
            if (isOnlyBroadcast(executionPlan)) {
                DirectTableOperation dto = (DirectTableOperation) plan;
                String dbIndex = getBroadcastTableGroup(executionContext, dto.getSchemaName());
                DirectTableOperation newDto = (DirectTableOperation) dto.copy(dto.getTraitSet(), dto.getInputs());
                newDto.setDbIndex(dbIndex);
                return executionPlan.copy(newDto);
            }

            if (ast == null || !ast.getKind().belongsTo(EnumSet.of(SqlKind.SELECT, SqlKind.DELETE, SqlKind.UPDATE))) {
                return executionPlan;
            }
            final boolean withForceIndex = executionPlan.checkProperty(ExecutionPlanProperties.WITH_FORCE_INDEX);
            if (withForceIndex) {
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
                new ReplaceTableNameWithQuestionMarkVisitor(executionContext.getSchemaName(), withForceIndex,
                    executionContext);
            final SqlNode sqlTemplate = ast.accept(visitor);

            final List<String> tableNames = new ArrayList<>(visitor.getTableNames());

            boolean forceAllowFullTableScan = executionContext.getParamManager().getBoolean(
                ConnectionParams.ALLOW_FULL_TABLE_SCAN) || executionPlan.isExplain();

            List<String> schemaNamesOfPlan = new ArrayList<String>();

            Map<String, List<List<String>>> targetTables = getTargetTablesAndSchemas(tableNames,
                executionPlan,
                executionContext,
                schemaNamesOfPlan,
                forceAllowFullTableScan,
                false);

            boolean canPushdown = (schemaNamesOfPlan.size() == 1);
            if (ScaleOutPlanUtil.isEnabledScaleOut(executionContext.getParamManager()) && ast.getKind()
                .belongsTo(EnumSet.of(SqlKind.INSERT, SqlKind.DELETE, SqlKind.UPDATE))
                && executionPlan.getTableSet() != null) {
                Set<Pair<String, String>> tableSet = executionPlan.getTableSet();
                List<TableMeta> tableMetas = new ArrayList<>();
                for (Pair<String, String> tableInfo : tableSet) {
                    TableMeta tableMeta = executionContext.getSchemaManager(tableInfo.getKey())
                        .getTable(tableInfo.getValue());
                    tableMetas.add(tableMeta);
                    boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(tableMeta.getSchemaName());
                    if (isNewPart) {
                        canPushdown &= (!tableMeta.getPartitionInfo().isBroadcastTable());
                    } else {
                        TableRule tableRule =
                            executionContext.getSchemaManager(tableMeta.getSchemaName()).getTddlRuleManager()
                                .getTableRule(tableMeta.getTableName());
                        canPushdown &= (!tableRule.isBroadcast());
                    }
                }
                Map<String, List<List<String>>> targetTablesForScaleout = null;
                if (GeneralUtil.isEmpty(targetTables)) {
                    List<String> schemaNamesOfPlanForScaleout = new ArrayList<String>();
                    targetTablesForScaleout = getTargetTablesAndSchemas(tableNames,
                        executionPlan,
                        executionContext,
                        schemaNamesOfPlanForScaleout,
                        forceAllowFullTableScan,
                        true);
                } else {
                    targetTablesForScaleout = targetTables;
                }
            }

            boolean isAllAtOnePhyTb = allAtOnePhyTable(targetTables, tableNames.size());
            if (plan instanceof LogicalModifyView) {
                if (((LogicalModifyView) plan).getHintContext() != null
                    && ((LogicalModifyView) plan).getHintContext().containsInventoryHint()
                    && !isAllAtOnePhyTb) {
                    executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
                }
                if (!dmlWithDerivedSubquery(plan, ast)) {
                    return executionPlan;
                }
            }
            if (canPushdown && isAllAtOnePhyTb && ast.isA(SqlKind.DML)) {
                final String schemaNamesOfAst = schemaNamesOfPlan.get(0);
                boolean withGsi = true;
                boolean needRelicateWrite = true;
                boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaNamesOfAst);
                String groupName = targetTables.keySet().iterator().next();
                for (String tableName : tableNames) {
                    withGsi &= GlobalIndexMeta.hasIndex(tableName, schemaNamesOfAst, executionContext);
                    TableMeta tableMeta = executionContext.getSchemaManager(schemaNamesOfAst).getTable(tableName);
                    if (!isNewPart) {
                        needRelicateWrite &= ComplexTaskPlanUtils
                            .canWrite(tableMeta, groupName);
                    } else {
                        for (List<String> phyTable : targetTables.get(groupName)) {
                            String partName = ComplexTaskPlanUtils
                                .getPartNameFromGroupAndTable(tableMeta, groupName, phyTable.get(0));
                            needRelicateWrite &= ComplexTaskPlanUtils
                                .canWrite(tableMeta, partName);
                            if (needRelicateWrite) {
                                break;
                            }
                        }

                    }
                    canPushdown &= !withGsi & !needRelicateWrite;
                }
            }
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
                        schemaNamesOfAst,
                        tableNames);
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

                    if (tableModify.isDelete() && tableNames.size() > 1
                        && CheckModifyLimitation.checkModifyBroadcast(tableModify, doNothing())) {
                        if (executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                            executionPlan = executionPlan.copy(executionPlan.getPlan());
                            executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
                            return executionPlan;
                        } else {
                            throw new TddlNestableRuntimeException("multi delete not support broadcast");
                        }
                    }

                    final PhyTableModifyViewBuilder modifyViewBuilder = new PhyTableModifyViewBuilder(sqlTemplate,
                        targetTables, executionContext, plan, DbType.MYSQL, tableNames, schemaNamesOfAst);
                    final List<RelNode> phyTableModifies = modifyViewBuilder.build();
                    return executionPlan.copy(phyTableModifies.get(0));
                }
                case UPDATE: {
                    TableModify tableModify = null;
                    if (plan instanceof LogicalModifyView) {
                        tableModify = ((LogicalModifyView) plan).getTableModify();
                    } else {
                        tableModify = (TableModify) plan;
                    }

                    if (tableModify.isUpdate() && tableNames.size() > 1
                        && CheckModifyLimitation.checkModifyBroadcast(tableModify, doNothing())) {
                        if (executionContext.getParamManager()
                            .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                            executionPlan = executionPlan.copy(executionPlan.getPlan());
                            executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
                            return executionPlan;
                        } else {
                            throw new TddlNestableRuntimeException("multi update not support broadcast");
                        }
                    }

                    final PhyTableModifyViewBuilder modifyViewBuilder = new PhyTableModifyViewBuilder(sqlTemplate,
                        targetTables,
                        executionContext,
                        plan,
                        DbType.MYSQL,
                        tableNames,
                        schemaNamesOfAst);
                    final List<RelNode> phyTableModifies = modifyViewBuilder.build();
                    return executionPlan.copy(phyTableModifies.get(0));
                }
                default:
                } // end of switch

            } else {
                /*
                 * Maybe some tables at one group
                 */
                if (ast.getKind() == SqlKind.DELETE) {
                    if (GeneralUtil.isNotEmpty(targetTables) && dmlWithDerivedSubquery(plan, ast)) {
                        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
                        final LogicalModifyView lmv = (LogicalModifyView) plan;
                        lmv.setTargetTables(targetTables);
                        lmv.setSqlTemplate(sqlTemplate);
                        return executionPlan.copy(lmv);
                    } else if (!executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                        throw new TddlNestableRuntimeException("not support cross db delete");
                    } else if (plan instanceof LogicalModify && ((LogicalModify) plan).isWithoutPk()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                            String.join(",", ((LogicalModify) plan).getTargetTableNames()));
                    } else {
                        executionPlan = executionPlan.copy(executionPlan.getPlan());
                        executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
                    }
                } else if (ast.getKind() == SqlKind.UPDATE) {
                    if (GeneralUtil.isNotEmpty(targetTables) && dmlWithDerivedSubquery(plan, ast)) {
                        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
                        final LogicalModifyView lmv = (LogicalModifyView) plan;
                        lmv.setTargetTables(targetTables);
                        lmv.setSqlTemplate(sqlTemplate);
                        return executionPlan.copy(lmv);
                    } else if (!executionContext.getParamManager()
                        .getBoolean(ConnectionParams.ENABLE_COMPLEX_DML_CROSS_DB)) {
                        throw new TddlNestableRuntimeException("not support cross db update");
                    } else if (plan instanceof LogicalModify && ((LogicalModify) plan).isWithoutPk()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_UPDATE_DELETE_NO_PRIMARY_KEY,
                            String.join(",", ((LogicalModify) plan).getTargetTableNames()));
                    } else {
                        executionPlan = executionPlan.copy(executionPlan.getPlan());
                        executionPlan.getPlanProperties().set(ExecutionPlanProperties.MODIFY_CROSS_DB);
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

    /**
     * Whether is direct broadcast table plan
     */
    private static boolean isOnlyBroadcast(final ExecutionPlan executionPlan) {
        RelNode plan = executionPlan.getPlan();
        if (!PlannerContext.getPlannerContext(plan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_RANDOM_READ)) {
            return false;
        }
        if (!(plan instanceof DirectTableOperation)) {
            return false;
        }
        return executionPlan.getPlanProperties().get(ExecutionPlanProperties.ONLY_BROADCAST_TABLE);
    }

    /**
     * Only broadcast table:
     * 1. Choose an existed group from held connections if already in a transaction
     * 2. Choose a random group if it's the first statement in transaction
     * 3. Skip single-group if any other groups exist
     */
    private String getBroadcastTableGroup(ExecutionContext executionContext, String schemaName) {
        ITransaction transaction = executionContext.getTransaction();
        List<String> candidateGroups = null;
        if (transaction != null) {
            candidateGroups = transaction.getConnectionHolder().getHeldGroupsOfSchema(schemaName);
        }
        if (candidateGroups == null || candidateGroups.isEmpty()) {
            candidateGroups = HintUtil.allGroup(schemaName);
        }

        int allSingleCount = (int) candidateGroups.stream().filter(GroupInfoUtil::isSingleGroup).count();
        if (allSingleCount == candidateGroups.size()) {
            return candidateGroups.get(new Random().nextInt(allSingleCount));
        } else {
            int count = candidateGroups.size() - allSingleCount;
            return candidateGroups.stream().filter(x -> !GroupInfoUtil.isSingleGroup(x))
                .skip(new Random().nextInt(count)).findFirst().orElse(null);
        }
    }

    public static boolean skipPostPlanner(final ExecutionPlan executionPlan) {
        SqlNode ast = executionPlan.getAst();
        RelNode plan = executionPlan.getPlan();
        if (!PlannerContext.getPlannerContext(plan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_POST_PLANNER)) {
            return true;
        }
        if (isOnlyBroadcast(executionPlan)) {
            return false;
        }

        if (PlannerContext.getPlannerContext(plan).isSkipPostOpt()) {
            return true;
        }

        boolean pushedDql;
//        if (PlannerContext.getPlannerContext(plan).isUseMppPlan()) {
//            if (plan instanceof LogicalView) {
//                //mpp plan
//                return !(allTableSingle(((LogicalView) plan).getTableNames(), ((LogicalView) plan).getSchemaName()));
//            } else {
//                return false;
//            }
//        } else {
//            pushedDql = !(plan instanceof LogicalModifyView) && plan instanceof LogicalView;
//        }

        pushedDql = !(plan instanceof LogicalModifyView) && plan instanceof LogicalView;

        if (pushedDql || plan instanceof LogicalRelocate || plan instanceof BaseTableOperation
            || plan instanceof LogicalRecyclebin || plan instanceof BroadcastTableModify) {
            return true;
        }

        if (plan instanceof LogicalModifyView && ((LogicalModifyView) plan).getHintContext() != null
            && ((LogicalModifyView) plan).getHintContext().containsInventoryHint()) {
            return false;
        }

        // For Pushed DML with subquery, sqlTemplate in LogicalModifyView should be replaced by original ast
        return plan instanceof LogicalModifyView && !dmlWithDerivedSubquery(plan, ast);
    }

    public static boolean dmlWithDerivedSubquery(final RelNode plan, final SqlNode ast) {
        if (plan instanceof LogicalModifyView) {
            switch (ast.getKind()) {
            case UPDATE:
                return ((SqlUpdate) ast).withSubquery();
            case DELETE:
                return ((SqlDelete) ast).withSubquery();
            default:
                return false;
            }
        }
        return false;
    }

    /**
     * Map[分库 List[按下标分组 List[一个物理 SQL 中的所有表]]]
     */
    private Map<String, List<List<String>>> getTargetTablesAndSchemas(List<String> tableNames,
                                                                      ExecutionPlan executionPlan,
                                                                      ExecutionContext executionContext,
                                                                      List<String> schemasToBeReturn,
                                                                      boolean forceAllowFullTableScan,
                                                                      boolean allowMultiSchema) {

        Map<Integer, ParameterContext> params =
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();

        final RelNode plan = executionPlan.getPlan();
        Set<String> schemaNames = executionPlan.getSchemaNames();
        PlanShardInfo planShardInfo = executionPlan.getPlanShardInfo();

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
            schemaNames.add(schemaNameOfPlan);
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
            TddlRuleManager tddlRuleManager = executionContext.getSchemaManager(schemaNameOfPlan).getTddlRuleManager();
            Map<String, List<List<String>>> result = new HashMap<>();

            // used by shardedTb
            Map<String, Map<String, Comparative>> allComps = planShardInfo.getAllTableComparative(schemaNameOfPlan);
            Map<String, Map<String, Comparative>> allFullComps =
                planShardInfo.getAllTableFullComparative(schemaNameOfPlan);

            List<PartPrunedResult> rsList = new ArrayList<>();
            for (String name : tableNames) {

                // used by partitionedTb
                RelShardInfo shardInfo = planShardInfo.getRelShardInfo(schemaNameOfPlan, name);
                if (!shardInfo.isUsePartTable()) {
                    Map<String, Comparative> comps = allComps.get(name);
                    Map<String, Object> calcParams = new HashMap<>();
                    calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
                    calcParams.put(CalcParamsAttribute.COM_DB_TB, allFullComps);
                    calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
                    List<TargetDB> tdbs =
                        tddlRuleManager
                            .shard(name, true, forceAllowFullTableScan, comps, params, calcParams, executionContext);
                    targetDBs.add(tdbs);
                } else {
                    // do routing for each partTable
                    PartitionPruneStep stepInfo = shardInfo.getPartPruneStepInfo();
                    PartPrunedResult prunedResult =
                        PartitionPruner.doPruningByStepInfo(stepInfo, executionContext);

                    // covert to targetDbList
                    targetDBs.add(PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(prunedResult));
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
                    Map<String, Map<String, Comparative>> allComps = planShardInfo.getAllTableComparative(schemaName);
                    Map<String, Map<String, Comparative>> allFullComps =
                        planShardInfo.getAllTableFullComparative(schemaName);

                    for (String name : tbNameList) {
                        RelShardInfo shardInfo = planShardInfo.getRelShardInfo(schemaName, name);
                        if (!shardInfo.isUsePartTable()) {
                            Map<String, Comparative> comps = allComps.get(name);
                            Map<String, Object> calcParams = new HashMap<>();
                            calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
                            calcParams.put(CalcParamsAttribute.COM_DB_TB, allFullComps);
                            calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
                            List<TargetDB> tdbs =
                                tddlRuleManager.shard(name, true, forceAllowFullTableScan, comps, params, calcParams,
                                    executionContext);
                            targetDBs.add(tdbs);
                        } else {

                            // routed partTable
                            PartitionPruneStep stepInfo = shardInfo.getPartPruneStepInfo();
                            PartPrunedResult prunedResult =
                                PartitionPruner.doPruningByStepInfo(stepInfo, executionContext);
                            // covert to targetDbList
                            targetDBs.add(PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(prunedResult));
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
        return !usingHint && !isExplainOptimizer(explain) && !isExplainSharding(explain);
    }

    private boolean existUnPushableRelNode(RelNode node) {
        class CheckUnPushableRelVisitor extends RelVisitor {
            private boolean exists = false;

            @Override
            public void visit(RelNode relNode, int ordinal, RelNode parent) {
                if (relNode instanceof LogicalView) {
                    final boolean singleGroup = ((LogicalView) relNode).getSize() <= 1;
                    if (!singleGroup) {
                        exists = true;
                    }
                } else if (relNode instanceof Project) {
                    exists = PushProjectRule.doNotPush((Project) relNode) ? true : exists;
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
