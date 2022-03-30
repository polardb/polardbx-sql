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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.statistics.ExplainStatisticsHandler;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.rpc.client.XSession;
import com.alibaba.polardbx.rpc.result.XResult;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.Xprotocol.XRowSet;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.build.VectorizedExpressionBuilder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.row.ResultSetRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.index.AdviceResult;
import com.alibaba.polardbx.optimizer.index.Configuration;
import com.alibaba.polardbx.optimizer.index.IndexAdvisor;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.PlanShardInfo;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainAdvisor;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainExecute;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainJsonPlan;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainLogicalView;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainOptimizer;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSharding;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainSimple;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainStatistics;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isExplainVec;
import static com.alibaba.polardbx.optimizer.utils.ExplainResult.isPhysicalFragment;
import static com.alibaba.polardbx.optimizer.utils.RelUtils.disableMpp;

public class ExplainExecutorUtil {

    private static final int MAX_EXPLAIN_VEC_SUB_QUERY_STACK_SIZE = 16;

    public static ResultCursor explain(ExecutionPlan executionPlan, ExecutionContext executionContext,
                                       ExplainResult explain) {
        if (isExplainOptimizer(explain)) {
            return ExplainExecutorUtil.handleExplainWithStage(executionContext, executionPlan);
        } else if (isExplainSharding(explain)) {
            return ExplainExecutorUtil.handleExplainSharding(executionContext, executionPlan);
        } else if (isExplainSimple(explain)) {
            return ExplainExecutorUtil.handleExplainSimple(executionContext, executionPlan);
        } else if (isExplainJsonPlan(explain)) {
            return handleExplainJsonPlan(executionPlan, executionContext);
        } else if (isExplainExecute(explain)) {
            //mpp 模式现在还不支持 explain execute
            disableMpp(executionContext);
            return ExplainExecutorUtil.handleExplainExecute(executionPlan, executionContext);
        } else if (isPhysicalFragment(explain)) {
            ExecutorHelper.selectExecutorMode(
                executionPlan.getPlan(), executionContext, true);
            if (executionContext.getExecuteMode() == ExecutorMode.MPP) {
                executionContext.getExtraCmds().put(ConnectionProperties.MERGE_UNION, false);
                return ExplainExecutorUtil.handleExplainMppPhysicalPlan(executionContext, executionPlan);
            } else if (executionContext.getExecuteMode() == ExecutorMode.AP_LOCAL
                || executionContext.getExecuteMode() == ExecutorMode.TP_LOCAL) {
                return ExplainExecutorUtil.handleExplainLocalPhysicalPlan(
                    executionContext, executionPlan, executionContext.getExecuteMode());
            } else {
                return ExplainExecutorUtil.handleExplain(
                    executionContext, executionPlan, ExplainResult.ExplainMode.LOGIC);
            }
        } else if (isExplainLogicalView(explain)) {
            PlannerContext plannerContext = PlannerContext.getPlannerContext(executionPlan.getPlan());
            boolean oldExplainLogicalView =
                plannerContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_LOGICALVIEW);
            plannerContext.getExtraCmds().put(ConnectionProperties.EXPLAIN_LOGICALVIEW, true);
            try {
                return ExplainExecutorUtil.handleExplain(executionContext, executionPlan, explain.explainMode);
            } finally {
                plannerContext.getExtraCmds().put(ConnectionProperties.EXPLAIN_LOGICALVIEW, oldExplainLogicalView);
            }
        } else if (isExplainAdvisor(explain)) {

            List<AdviceResult> adviceResultList = new ArrayList<>();
            IndexAdvisor indexAdvisor = new IndexAdvisor(executionPlan, executionContext);

            String adviseTypeString = executionContext.getParamManager().getString(ConnectionParams.ADVISE_TYPE);
            if (adviseTypeString != null) {
                IndexAdvisor.AdviseType adviseType = null;
                if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.LOCAL_INDEX.toString())) {
                    adviseType = IndexAdvisor.AdviseType.LOCAL_INDEX;
                } else if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.GLOBAL_INDEX.toString())) {
                    adviseType = IndexAdvisor.AdviseType.GLOBAL_INDEX;
                } else if (adviseTypeString
                    .equalsIgnoreCase(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX.toString())) {
                    adviseType = IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX;
                } else if (adviseTypeString.equalsIgnoreCase(IndexAdvisor.AdviseType.BROADCAST.toString())) {
                    adviseType = IndexAdvisor.AdviseType.BROADCAST;
                } else if (adviseTypeString.equalsIgnoreCase("ALL")) {
                    AdviceResult localIndexAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.LOCAL_INDEX);
                    AdviceResult gsiAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_INDEX);
                    AdviceResult coveringGsiAdviceResult =
                        indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX);
                    AdviceResult broadcastAdviceResult = indexAdvisor.advise(IndexAdvisor.AdviseType.BROADCAST);

                    adviceResultList.add(localIndexAdviceResult);
                    adviceResultList.add(gsiAdviceResult);
                    adviceResultList.add(coveringGsiAdviceResult);
                    adviceResultList.add(broadcastAdviceResult);

                    return ExplainExecutorUtil.handleExplainAdvisor(adviceResultList, executionContext);
                }
                if (adviseType != null) {
                    AdviceResult adviceResult = indexAdvisor.advise(adviseType);
                    adviceResultList.add(adviceResult);
                    return ExplainExecutorUtil.handleExplainAdvisor(adviceResultList, executionContext);
                }
            }

            AdviceResult bestResult = null;
            IndexAdvisor.AdviseType[] types = {IndexAdvisor.AdviseType.LOCAL_INDEX,
                IndexAdvisor.AdviseType.GLOBAL_INDEX, IndexAdvisor.AdviseType.BROADCAST};
            for (IndexAdvisor.AdviseType type : types) {
                AdviceResult adviceResult = indexAdvisor.advise(type);
                if (adviceResult.getAfterPlan() != null) {
                    if (bestResult == null || adviceResult.getConfiguration().getAfterCost()
                        .isLt(bestResult.getConfiguration().getAfterCost())) {
                        bestResult = adviceResult;
                    }
                }
            }
            if (bestResult != null) {
                adviceResultList.add(bestResult);
            } else {
                AdviceResult coveringGsiAdviceResult =
                    indexAdvisor.advise(IndexAdvisor.AdviseType.GLOBAL_COVERING_INDEX);
                adviceResultList.add(coveringGsiAdviceResult);
            }
            return ExplainExecutorUtil.handleExplainAdvisor(adviceResultList, executionContext);
        } else if (isExplainStatistics(explain)) {
            return ExplainStatisticsHandler.handleExplainStatistics(executionContext, executionPlan);
        } else if (isExplainVec(explain)) {
            return ExplainExecutorUtil.handleExplainVec(executionContext, executionPlan, explain.explainMode);
        } else {
            return ExplainExecutorUtil.handleExplain(executionContext, executionPlan, explain.explainMode);
        }
    }

    private static ResultCursor handleExplainAdvisor(List<AdviceResult> adviceResultList,
                                                     ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("AdviceResult");

        result.addColumn("IMPROVE_VALUE", DataTypes.StringType);
        result.addColumn("IMPROVE_CPU", DataTypes.StringType);
        result.addColumn("IMPROVE_MEM", DataTypes.StringType);
        result.addColumn("IMPROVE_IO", DataTypes.StringType);
        result.addColumn("IMPROVE_NET", DataTypes.StringType);
        result.addColumn("BEFORE_VALUE", DataTypes.DoubleType);
        result.addColumn("BEFORE_CPU", DataTypes.DoubleType);
        result.addColumn("BEFORE_MEM", DataTypes.DoubleType);
        result.addColumn("BEFORE_IO", DataTypes.DoubleType);
        result.addColumn("BEFORE_NET", DataTypes.DoubleType);
        result.addColumn("AFTER_VALUE", DataTypes.DoubleType);
        result.addColumn("AFTER_CPU", DataTypes.DoubleType);
        result.addColumn("AFTER_MEM", DataTypes.DoubleType);
        result.addColumn("AFTER_IO", DataTypes.DoubleType);
        result.addColumn("AFTER_NET", DataTypes.DoubleType);
        result.addColumn("ADVISE_INDEX", DataTypes.StringType);
        result.addColumn("NEW_PLAN", DataTypes.StringType);
        result.addColumn("INFO", DataTypes.StringType);
        result.initMeta();

        boolean hasRow = false;

        for (AdviceResult adviceResult : adviceResultList) {
            if (adviceResult != null && adviceResult.getConfiguration() != null
                && adviceResult.getAfterPlanForDisplay() != null) {
                Configuration configuration = adviceResult.getConfiguration();
                DrdsRelOptCostImpl beforeCost = (DrdsRelOptCostImpl) adviceResult.getBeforeCost();
                DrdsRelOptCostImpl afterCost = (DrdsRelOptCostImpl) configuration.getAfterCost();
                result.addRow(new Object[] {
                    afterCost.getValue() != 0 ?
                        toPercent((beforeCost.getValue() - afterCost.getValue()) / afterCost.getValue()) : "INF",
                    afterCost.getCpu() != 0 ?
                        toPercent((beforeCost.getCpu() - afterCost.getCpu()) / afterCost.getCpu()) : "INF",
                    afterCost.getMemory() != 0 ?
                        toPercent((beforeCost.getMemory() - afterCost.getMemory()) / afterCost.getMemory()) : "INF",
                    afterCost.getIo() != 0 ? toPercent((beforeCost.getIo() - afterCost.getIo()) / afterCost.getIo()) :
                        "INF",
                    afterCost.getNet() != 0 ?
                        toPercent((beforeCost.getNet() - afterCost.getNet()) / afterCost.getNet()) : "INF",
                    round(beforeCost.getValue(), 1),
                    round(beforeCost.getCpu(), 1),
                    round(beforeCost.getMemory(), 1),
                    round(beforeCost.getIo(), 1),
                    round(beforeCost.getNet(), 1),
                    round(afterCost.getValue(), 1),
                    round(afterCost.getCpu(), 1),
                    round(afterCost.getMemory(), 1),
                    round(afterCost.getIo(), 1),
                    round(afterCost.getNet(), 1),
                    configuration.broadcastSql() +
                        (configuration.getCandidateIndexSet().size()> 0?
                    String.join(";",
                        configuration.getCandidateIndexSet().stream()
                            .map(candidateIndex -> candidateIndex.getSql()).collect(Collectors.toList())) + ";" : ""),
                    adviceResult.getAfterPlanForDisplay(),
                    adviceResult.getInfo()
                });
                hasRow = true;
            }
        }

        if (!hasRow) {
            DrdsRelOptCostImpl beforeCost = (DrdsRelOptCostImpl) adviceResultList.get(0).getBeforeCost();
            result.addRow(new Object[] {
                null,
                null,
                null,
                null,
                null,
                round(beforeCost.getValue(), 1),
                round(beforeCost.getCpu(), 1),
                round(beforeCost.getMemory(), 1),
                round(beforeCost.getIo(), 1),
                round(beforeCost.getNet(), 1),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                adviceResultList.get(0).getInfo()
            });
        }
        return result;
    }

    public static double round(double value, int scale) {
        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(scale, BigDecimal.ROUND_FLOOR);
        double d = bd.doubleValue();
        return d;
    }

    private static String toPercent(double s) {
        DecimalFormat fmt = new DecimalFormat("##0.0%");
        return fmt.format(s);
    }

    private static ResultCursor handleExplainJsonPlan(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        String logicalPlanString = PlanManagerUtil.relNodeToJson(executionPlan.getPlan());

        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Plan", DataTypes.StringType);
        result.initMeta();

        result.addRow(new Object[] {StringUtils.normalizeSpace(logicalPlanString)});

        result.addRow(new Object[] {""});
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();

        handleSubquerySimpleExplain(executionContext, executionPlan.getPlan(), result, max, current);
        return result;
    }

    private static ResultCursor handleExplainWithStage(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        final Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);
        CalcitePlanOptimizerTrace.getOptimizerTracer().get()
            .addSnapshot("End", executionPlan.getPlan(), PlannerContext.getPlannerContext(executionContext, evalFunc));
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        List<Map.Entry<String, String>> optimizerSnapshots = CalcitePlanOptimizerTrace.getOptimizerTracer()
            .get()
            .getPlanSnapshots();
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Stage", DataTypes.StringType);
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();
        for (Map.Entry<String, String> snapShots : optimizerSnapshots) {
            String ruleName = snapShots.getKey();
            for (String row : StringUtils.split(snapShots.getValue(), "\r\n")) {
                result.addRow(new Object[] {"", row});
            }
            result.addRow(new Object[] {ruleName, ""});
            result.addRow(new Object[] {"", ""});

        }
        CalcitePlanOptimizerTrace.setOpen(false);
        CalcitePlanOptimizerTrace.getOptimizerTracer().get().clean();
        return result;
    }

    private static ResultCursor handleExplainSharding(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        final ArrayResultCursor result = new ArrayResultCursor("Sharding Table");
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        result.addColumn("Logical_Table", DataTypes.StringType);
        result.addColumn("Sharding", DataTypes.StringType);
        result.addColumn("Shard_Count", DataTypes.StringType);
        result.addColumn("Broadcast", DataTypes.StringType);
        result.addColumn("Condition", DataTypes.StringType);
        result.initMeta();

        final RelNode plan = executionPlan.getPlan();
        final String schemaName = executionContext.getSchemaName();
        final Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        final ExtractionResult er = ConditionExtractor.predicateFrom(plan).extract();

        final Map<String, Map<String, Comparative>> allComps = new HashMap<>();
        final Map<String, Map<String, Comparative>> allFullComps = new HashMap<>();
        final List<Pair<String, String>> logicalTables = new ArrayList<>();

        if (plan instanceof DirectTableOperation) {
            ((DirectTableOperation) plan).getLogicalTableNames()
                .forEach(t -> logicalTables.add(Pair.of(schemaName, t)));
        } else {
            er.allCondition(allComps, allFullComps, executionContext);
            er.getLogicalTables().forEach(t -> logicalTables.add(RelUtils.getQualifiedTableName(t)));
        }

        PlanShardInfo planShardInfo = ConditionExtractor.predicateFrom(plan).extract().allShardInfo(executionContext);

        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        calcParams.put(CalcParamsAttribute.COM_DB_TB, allFullComps);
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());

        for (Pair<String, String> qn : logicalTables) {
            final OptimizerContext context = OptimizerContext.getContext(qn.left);
            final PartitionInfoManager partitionInfoManager = context.getPartitionInfoManager();
            final TddlRuleManager or = context.getRuleManager();
            final String tableName = qn.getValue();
            final Map<String, Comparative> comps = allComps.get(tableName);
            final Map<String, Set<String>> tableMap = new LinkedHashMap<>();
            final Set<String> groupSet = new HashSet<>();
            int shardCount = 0;
            boolean isPhyTableGet = true;
            List<TargetDB> tdbs = new ArrayList<>();
            if (!partitionInfoManager.isNewPartDbTable(tableName)) {
                calcParams.remove(CalcParamsAttribute.DB_SHARD_KEY_SET);
                calcParams.remove(CalcParamsAttribute.TB_SHARD_KEY_SET);
                try {
                    tdbs = or
                        .shard(tableName, true, true, comps, params, calcParams, executionContext);
                } catch (Exception e) {
                    isPhyTableGet = false;
                }
            } else {
                PartitionPruneStep partitionPruneStep =
                    planShardInfo.getRelShardInfo(qn.left, qn.right).getPartPruneStepInfo();
                PartPrunedResult
                    partPrunedResult = PartitionPruner.doPruningByStepInfo(partitionPruneStep, executionContext);
                tdbs = PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(partPrunedResult);
            }

            for (TargetDB targetDB : tdbs) {
                groupSet.add(targetDB.getDbIndex());
                if (!tableMap.containsKey(tableName)) {
                    tableMap.put(tableName, new HashSet<>());
                }
                shardCount += targetDB.getTableNames().size();
                tableMap.get(tableName).addAll(targetDB.getTableNames());
            }

            final String phyTableString =
                isPhyTableGet ? ExplainUtils.compressPhyTableString(tableMap, groupSet) : tableName;
            final TableRule tableRule = or.getTableRule(tableName);

            StringBuilder condition = new StringBuilder();
            Map<String, DataType> tmpDataTypeMap = null;
            if (!MapUtils.isEmpty(comps)) {
                SchemaManager schemaManager =
                    OptimizerContext.getContext(executionContext.getSchemaName()).getLatestSchemaManager();
                tmpDataTypeMap = PlannerUtils.buildDataType(ImmutableList.copyOf(comps.keySet()),
                    schemaManager.getTable(tableName));

                for (Map.Entry<String, Comparative> entry : comps.entrySet()) {
                    final String column = entry.getKey();
                    final Comparative comparative = or.getComparative(
                        tableRule,
                        comps,
                        column,
                        params,
                        tmpDataTypeMap,
                        calcParams);

                    final String conditionString = null == comparative ? "" : ExplainUtils.comparativeToString(column,
                        comparative,
                        null);
                    if (TStringUtil.isNotBlank(conditionString)) {
                        if (condition.length() > 0) {
                            condition.append(", ");
                        }
                        condition.append(conditionString);
                    }
                } // end of for
            }

            boolean isBroacast = or.isBroadCast(tableName);
            if (isBroacast) {
                shardCount = 1;
            }
            result.addRow(new Object[] {
                tableName, phyTableString, shardCount, String.valueOf(isBroacast),
                condition.toString()});
        } // end of for

        return result;
    }

    private static ResultCursor handleExplainSimple(ExecutionContext executionContext, ExecutionPlan executionPlan) {

        String logicalPlanString = RelOptUtil.dumpPlan("",
            executionPlan.getPlan(),
            SqlExplainFormat.TEXT,
            SqlExplainLevel.NO_ATTRIBUTES);
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();

        for (String row : StringUtils.split(logicalPlanString, "\r\n")) {
            result.addRow(new Object[] {row});
        }

        result.addRow(new Object[] {""});
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();

        handleSubquerySimpleExplain(executionContext, executionPlan.getPlan(), result, max, current);
        return result;
    }

    private static void handleSubquerySimpleExplain(ExecutionContext executionContext, RelNode logicalPlan,
                                                    ArrayResultCursor result, AtomicInteger max,
                                                    AtomicInteger current) {
        if (current.incrementAndGet() > max.get()) {
            max.set(current.get());
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery"});
            } else {
                result.addRow(new Object[] {">> individual correlate subquery"});
            }
            String subLogicalPlanString = RelOptUtil.dumpPlan("",
                rexDynamicParam.getRel(),
                SqlExplainFormat.TEXT,
                SqlExplainLevel.NO_ATTRIBUTES);
            for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
            result.addRow(new Object[] {""});
            handleSubqueryExplain(executionContext, rexDynamicParam.getRel(), result, max, current);
        }
        current.decrementAndGet();
    }

    private static void handleSubqueryExplain(ExecutionContext executionContext, RelNode logicalPlan,
                                              ArrayResultCursor result, AtomicInteger max, AtomicInteger current) {
        if (current.incrementAndGet() > max.get()) {
            max.set(current.get());
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery : " + rexDynamicParam.getRel().hashCode()});
            } else {
                result.addRow(new Object[] {
                    ">> individual correlate subquery : "
                        + rexDynamicParam.getRel().hashCode()});
            }
            String subLogicalPlanString = RelUtils.toString(rexDynamicParam.getRel(), executionContext.getParams()
                .getCurrentParameter(), null, executionContext);
            for (String row : StringUtils.split(subLogicalPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
            result.addRow(new Object[] {""});
            handleSubqueryExplain(executionContext, rexDynamicParam.getRel(), result, max, current);
        }
        current.decrementAndGet();
    }

    private static ResultCursor handleExplainVec(ExecutionContext executionContext, ExecutionPlan executionPlan,
                                                 ExplainResult.ExplainMode mode) {
        SqlExplainLevel explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        CalcitePlanOptimizerTrace.setSqlExplainLevel(explainLevel);

        Map<Integer, ParameterContext> parameters = executionContext.getParams().getCurrentParameter();
        Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);

        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.addColumn("Extra info", DataTypes.StringType);
        result.initMeta();

        Function<RelNode, String> extraInfoBuilder = relNode -> {
            if (relNode == null) {
                return "";
            } else {
                StringBuilder builder = new StringBuilder();
                builder
                    .append(relNode.getClass().getSimpleName())
                    .append(":")
                    .append(relNode.getId());
                if (relNode instanceof Project) {
                    return buildExtraInfoForProject(builder, executionContext, (Project) relNode);
                } else {
                    return builder.toString();
                }
            }
        };

        List<Object[]> res = RelUtils.toStringWithExtraInfo(
            executionPlan.getPlan(), parameters, evalFunc, executionContext,
            extraInfoBuilder);

        res.forEach(result::addRow);

        handleSubQueryExplainVec(executionContext, executionPlan.getPlan(), result, extraInfoBuilder, 0);

        return result;
    }

    private static void handleSubQueryExplainVec(ExecutionContext executionContext, RelNode logicalPlan,
                                                 ArrayResultCursor result,
                                                 Function<RelNode, String> extraInfoBuilder, int level) {
        // prevent from stack overflow.
        if (level > MAX_EXPLAIN_VEC_SUB_QUERY_STACK_SIZE) {
            return;
        }

        for (RexDynamicParam rexDynamicParam : OptimizerUtils.findSubquery(logicalPlan)) {
            if (rexDynamicParam.getIndex() == -2) {
                result.addRow(new Object[] {">> individual scalar subquery : " + rexDynamicParam.getRel().hashCode()});
            } else {
                result.addRow(new Object[] {
                    ">> individual correlate subquery : "
                        + rexDynamicParam.getRel().hashCode()});
            }

            List<Object[]> res = RelUtils.toStringWithExtraInfo(
                rexDynamicParam.getRel(),
                executionContext.getParams().getCurrentParameter(),
                null,
                executionContext,
                extraInfoBuilder);

            res.forEach(result::addRow);

            // split row
            result.addRow(new Object[] {"", ""});

            // next sub-query
            handleSubQueryExplainVec(executionContext, rexDynamicParam.getRel(), result, extraInfoBuilder, level);
        }
    }

    private static String buildExtraInfoForProject(StringBuilder treeBuilder, ExecutionContext executionContext,
                                                   Project relNode) {
        Project project = relNode;

        RelNode input = project.getInput();
        List<DataType<?>> inputTypes = getInputDataType(input);

        treeBuilder.append('\n');

        for (RexNode rexNode : project.getProjects()) {
            // binding to vectorized expressions.
            VectorizedExpression vectorizedExpression = VectorizedExpressionBuilder
                .buildVectorizedExpression(inputTypes, rexNode, executionContext).getKey();

            String digest = VectorizedExpressionUtils.digest(vectorizedExpression);
            treeBuilder
                .append(rexNode.toString())
                .append('\n')
                .append(digest);
        }

        return treeBuilder.toString();
    }

    private static List<DataType<?>> getInputDataType(RelNode input) {
        // get input types from rel data type
        return input.getRowType()
            .getFieldList()
            .stream()
            .map(f -> new Field(f.getType()).getDataType())
            .map(d -> (DataType<?>) d)
            .collect(Collectors.toList());
    }

    private static ResultCursor handleExplainDdl(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        if (executionPlan.getPlan() instanceof BaseDdlOperation) {
            final BaseDdlOperation plan = (BaseDdlOperation) executionPlan.getPlan();
            ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
            result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
            result.addColumn("DDL Sql", DataTypes.StringType);
            result.addColumn("Logical_Table", DataTypes.StringType);
            result.addColumn("Sharding", DataTypes.StringType);
            result.addColumn("Count", DataTypes.IntegerType);
            result.initMeta();
            result.addRow(new Object[] {
                "DDL:ReturnType:" + plan.getRowType() + ",HitCache:" + executionPlan.isHitCache(),
                executionPlan.getAst().toString(), plan.getTableName(),
                "", -1});
            return result;
        } else {
            return handleExplain(executionContext, executionPlan, ExplainResult.ExplainMode.DETAIL);
        }

    }

    private static ResultCursor handleExplain(ExecutionContext executionContext, ExecutionPlan executionPlan,
                                              ExplainResult.ExplainMode mode) {
        SqlExplainLevel explainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        if (mode.isCost() || mode.isAnalyze()) {
            // set parameters for precise cost
            PlannerContext.getPlannerContext(executionPlan.getPlan()).setParams(executionContext.getParams());
            explainLevel = SqlExplainLevel.ALL_ATTRIBUTES;
        }
        CalcitePlanOptimizerTrace.setSqlExplainLevel(explainLevel);

        RuntimeStatistics statistics = (RuntimeStatistics) executionContext.getRuntimeStatistics();
        if (mode.isAnalyze()) {
            executionContext.getExtraCmds()
                .put(ConnectionProperties.MPP_METRIC_LEVEL, MetricLevel.OPERATOR.metricLevel);
            if (statistics == null) {
                statistics = RuntimeStatHelper.buildRuntimeStat(executionContext);
                executionContext.setRuntimeStatistics(statistics);
            }
            statistics.setPlanTree(executionPlan.getPlan());
            Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic;
            ExecutorHelper.selectExecutorMode(
                executionPlan.getPlan(), executionContext, true);
            if (executionContext.getExecuteMode() == ExecutorMode.MPP) {
                if (executionContext.getHintCmds() == null) {
                    executionContext.putAllHintCmds(new HashMap<>());
                }
                executionContext.getHintCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 3);
                executePlanForExplainAnalyze(executionPlan, executionContext);
                runtimeStatistic = statistics.toMppSketch();
            } else {
                executePlanForExplainAnalyze(executionPlan, executionContext);
                runtimeStatistic = statistics.toSketch();
            }
            CalcitePlanOptimizerTrace.getOptimizerTracer().get().setRuntimeStatistics(runtimeStatistic);
        }

        PropUtil.ExplainOutputFormat outputFormat = (PropUtil.ExplainOutputFormat) executionContext.getParamManager()
            .getEnum(ConnectionParams.EXPLAIN_OUTPUT_FORMAT);

        Map<Integer, ParameterContext> parameters = executionContext.getParams().getCurrentParameter();
        Function<RexNode, Object> evalFunc = RexUtils.getEvalFunc(executionContext);

        String output;
        if (outputFormat == PropUtil.ExplainOutputFormat.JSON) {
            output = RelUtils.toJsonString(executionPlan.getPlan(), parameters, evalFunc, executionContext);
        } else {
            output = RelUtils.toString(executionPlan.getPlan(), parameters, evalFunc, executionContext);
        }

        ArrayResultCursor result = new ArrayResultCursor("ExecutionPlan");
        result.addColumn("Logical ExecutionPlan", DataTypes.StringType);
        result.initMeta();

        for (String row : StringUtils.split(output, "\r\n")) {
            result.addRow(new Object[] {row});
        }
        result.addRow(new Object[] {"HitCache:" + executionPlan.isHitCache()});
        result.addRow(new Object[] {"Source:" + executionContext.getPlanSource()});
        if (mode.isCost()) {
            result.addRow(new Object[] {
                "WorkloadType: " +
                    executionContext.getWorkloadType()});
        }
        BaselineInfo baselineInfo = PlannerContext.getPlannerContext(executionPlan.getPlan()).getBaselineInfo();
        PlanInfo planInfo = PlannerContext.getPlannerContext(executionPlan.getPlan()).getPlanInfo();
        if (mode.isBaseLine() && baselineInfo != null && planInfo != null) {
            result.addRow(new Object[] {"BaselineInfo Id: " + baselineInfo.getId()});
            result.addRow(new Object[] {"BaselineInfo TablesHashCode: " + planInfo.getTablesHashCode()});
            result.addRow(new Object[] {"BaselineInfo TableSet: " + baselineInfo.getTableSet()});
            result.addRow(new Object[] {
                "BaselineInfo acceptedPlan: " +
                    baselineInfo.getAcceptedPlans().values().stream().map(x -> String.valueOf(x.getId()))
                        .collect(Collectors.joining(","))});
            result.addRow(new Object[] {
                "BaselineInfo unacceptedPlan: " +
                    baselineInfo.getUnacceptedPlans().values().stream().map(x -> String.valueOf(x.getId()))
                        .collect(Collectors.joining(","))});
            result.addRow(new Object[] {"PlanInfo Id: " + planInfo.getId()});
            result.addRow(new Object[] {"PlanInfo fixed: " + planInfo.isFixed()});
            result.addRow(new Object[] {"PlanInfo accepted: " + planInfo.isAccepted()});
            result.addRow(new Object[] {
                String.format("PlanInfo estimateExecutionTime: %.3fs", planInfo.getEstimateExecutionTime())});
            result.addRow(new Object[] {
                "PlanInfo lastExecuteTime: " +
                    new Timestamp(
                        (planInfo.getLastExecuteTime() == null ? -1 : planInfo.getLastExecuteTime()) * 1000)});
            result.addRow(new Object[] {"PlanInfo createTime: " + new Timestamp(planInfo.getCreateTime() * 1000)});
            result.addRow(new Object[] {"PlanInfo chooseCount: " + planInfo.getChooseCount()});
            result.addRow(new Object[] {"PlanInfo traceId: " + planInfo.getTraceId()});
            result.addRow(new Object[] {"PlanInfo origin: " + planInfo.getOrigin()});
        }
        AtomicInteger max = new AtomicInteger();
        AtomicInteger current = new AtomicInteger();
        if (executionPlan.getAst() != null) {
            if (SqlKind.SUPPORT_DDL.contains(executionPlan.getAst().getKind())
                && executionPlan.getAst().getKind() != SqlKind.CREATE_DATABASE
                && executionPlan.getAst().getKind() != SqlKind.DROP_DATABASE
                && executionPlan.getAst().getKind() != SqlKind.CREATE_VIEW
                && executionPlan.getAst().getKind() != SqlKind.DROP_VIEW) {
                return handleExplainDdl(executionContext, executionPlan);
            }
        }
        handleSubqueryExplain(executionContext, executionPlan.getPlan(), result, max, current);
        StringBuilder judgement = new StringBuilder();
        if (max.get() - PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes().size() > 2) {
            judgement.append("extremely slow for multi nested subqueries");
        }

        if (statistics != null) {
            // Make sure to release the reference to operators
            statistics.clear();
        }

        // show cache
        if (PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes().size() > 0) {
            result.addRow(new Object[] {"cache node:"});

            for (RelNode relNode : PlannerContext.getPlannerContext(executionPlan.getPlan()).getCacheNodes()) {
                result.addRow(new Object[] {relNode.getDigest()});
            }
        }

        if (judgement.length() > 0) {
            result.addRow(new Object[] {judgement});
        }

        //show sql template id
        String sqlTid = "NULL";
        PlanCache.CacheKey cacheKey = executionContext.getFinalPlan().getCacheKey();
        if (cacheKey != null) {
            sqlTid = cacheKey.getTemplateId();
        }
        result.addRow(new Object[] {"TemplateId: " + sqlTid});
        return result;
    }

    private static void executePlanForExplainAnalyze(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        /*
            Explain analyze 复用同一个executionContext, 参数可能会被污染
            已知情况: limit的物理sql cache需要保存计算好的limit到Params中
         */
        Parameters parameters = executionContext.cloneParamsOrNull();
        ResultCursor cursor = PlanExecutor.execByExecPlanNodeByOne(executionPlan, executionContext);
        executionContext.setParams(parameters);
        ArrayList<Throwable> exceptions = new ArrayList<>();
        try {
            Row result;
            while ((result = cursor.next()) != null) {
                // do nothing
            }
        } finally {
            cursor.close(exceptions);
        }

        if (!exceptions.isEmpty()) {
            throw GeneralUtil.nestedException(exceptions.get(0));
        }
    }

    /**
     * 展示MPP的物理执行计划
     */
    private static ResultCursor handleExplainMppPhysicalPlan(ExecutionContext executionContext,
                                                             ExecutionPlan executionPlan) {
        ArrayResultCursor result = null;
        try {
            //TODO 校验MPP
            executionContext.setMemoryPool(MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool(
                "mpp_explain_physical" + executionContext.getTraceId(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.QUERY));
            result = new ArrayResultCursor("PhysicalPlan");
            result.addColumn("Plan", DataTypes.StringType);
            result.initMeta();
            String mppPlanString = PlanUtils.textPlan(executionContext, executionPlan.getPlan());
            for (String row : StringUtils.split(mppPlanString, "\r\n")) {
                result.addRow(new Object[] {row});
            }
        } finally {
            executionContext.getMemoryPool().destroy();
        }

        return result;
    }

    private static ResultCursor handleExplainLocalPhysicalPlan(ExecutionContext executionContext,
                                                               ExecutionPlan executionPlan,
                                                               ExecutorMode type) {
        ArrayResultCursor result = new ArrayResultCursor("PhysicalPlan");
        result.addColumn("Plan", DataTypes.StringType);
        result.initMeta();
        String mppPlanString = PlanUtils.textLocalPlan(executionContext, executionPlan.getPlan(), type);
        for (String row : StringUtils.split(mppPlanString, "\r\n")) {
            result.addRow(new Object[] {row});
        }
        return result;
    }

    private static ResultCursor handleExplainExecute(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        if (executionPlan.getAst() instanceof SqlInsert || executionPlan.getAst() instanceof SqlCreateTable) {
            throw new NotSupportException("explain execute insert or ddl ");
        }

        List<LogicalView> views = Lists.newArrayList();
        RelShuttle logicalViewGetter = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                if (scan instanceof LogicalView) {
                    views.add((LogicalView) scan);
                }
                return scan;
            }
        };

        executionPlan.getPlan().accept(logicalViewGetter);

        // build meta info for physical plan
        boolean metaInit = false;
        ArrayResultCursor result = new ArrayResultCursor("PhysicalPlan");
        if (executionPlan.getPlan() instanceof DirectTableOperation
            || executionPlan.getPlan() instanceof SingleTableOperation
            || executionPlan.getPlan() instanceof DirectShardingKeyTableOperation
            || executionPlan.getPlan() instanceof PhyTableOperation
            || executionPlan.getPlan() instanceof PhyQueryOperation) {
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(executionPlan, executionContext);
            rc.setCursorMeta(result.getMeta());
            try {
                Row row = rc.next();
                if (!metaInit) {
                    initOriginMeta(rc, row, result);
                    metaInit = true;
                }
                while (row != null) {
                    result.addRow(row.getValues().toArray());
                    row = rc.next();
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }

        for (LogicalView lv : views) {
            ExecutionPlan lp = new ExecutionPlan(executionPlan.getAst(), lv, null);
            ResultCursor rc = PlanExecutor.execByExecPlanNodeByOne(lp, executionContext);
            rc.setCursorMeta(result.getMeta());
            try {
                Row row = rc.next();
                if (!metaInit) {
                    initOriginMeta(rc, row, result);
                    metaInit = true;
                }
                while (row != null) {
                    result.addRow(row.getValues().toArray());
                    row = rc.next();
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            } finally {
                rc.close(Lists.newArrayList());
            }
        }
        return result;
    }

    private static void initOriginMeta(ResultCursor rc, Row row, ArrayResultCursor result)
        throws SQLException, UnsupportedEncodingException {
        List<ColumnMeta> metas = Lists.newArrayList();
        String tableName = "explain";
        for (int i = 1; i <= row.getColNum(); i++) {
            final String columnName;
            if (row instanceof XRowSet) {
                final XResult xResult = ((XRowSet) row).getResult();
                columnName = xResult.getMetaData().get(i - 1).getName().toString(XSession
                    .toJavaEncoding(xResult.getSession().getResultMetaEncodingMySQL()));
            } else {
                columnName = ((ResultSetRow) row).getOriginMeta().getColumnName(i);
            }
            TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
            RelDataType dataType = factory.createSqlType(SqlTypeName.VARCHAR);
            Field col = new Field(tableName, columnName, dataType);
            ColumnMeta iColumnMeta = new ColumnMeta(tableName, columnName, null, col);
            metas.add(iColumnMeta);
            result.addColumn(iColumnMeta);
        }
        CursorMeta cursorMeta = CursorMeta.build(metas);
        row.setCursorMeta(cursorMeta);
        result.initMeta();
        rc.setCursorMeta(cursorMeta);
    }
}
