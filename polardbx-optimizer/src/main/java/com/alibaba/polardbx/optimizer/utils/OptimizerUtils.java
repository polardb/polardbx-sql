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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.DynamicComparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertManager;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.EXPLICIT_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_SHARE_READVIEW_TRANSACTION;
import static java.util.Collections.EMPTY_SET;

/**
 * @since 5.0.0
 */
public class OptimizerUtils {
    public static final String EMPTY_KEY = "NO_IN_EXPR";

    public static Date parseDate(String str, String[] parsePatterns) throws ParseException {
        try {
            return parseDate(str, parsePatterns, Locale.ENGLISH);
        } catch (ParseException e) {
            return parseDate(str, parsePatterns, Locale.getDefault());
        }
    }

    public static boolean hasDNHint(PlannerContext plannerContext) {
        return plannerContext.getExtraCmds() != null &&
            plannerContext.getExtraCmds().containsKey(ConnectionProperties.DN_HINT);
    }

    public static Map<Integer, ParameterContext> getParametersMapForOptimizer(RelNode rel) {
        Parameters parameters = getParametersForOptimizer(rel);
        Map<Integer, ParameterContext> params = parameters.getCurrentParameter();
        return params;
    }

    public static Parameters getParametersForOptimizer(RelNode rel) {
        return getParametersForOptimizer(PlannerContext.getPlannerContext(rel));
    }

    public static Parameters getParametersForOptimizer(PlannerContext context) {
        Parameters parameters = RelMetadataQuery.THREAD_PARAMETERS.get();
        if (parameters == null) {
            parameters = context.getParams();
        }
        return parameters;
    }

    public static String buildInExprKey(Map<Integer, ParameterContext> currentParameter) {
        StringBuilder key = new StringBuilder();
        for (int i = 1; i <= currentParameter.size(); i++) {
            ParameterContext pc = currentParameter.get(i);
            if (pc != null && pc.getValue() instanceof RawString) {
                RawString rawString = (RawString) pc.getValue();
                key.append(rawString.size()).append(":");
            }
        }
        if (key.length() > 1) {
            key.setLength(key.length() - 1);
            return key.toString();
        } else {
            return EMPTY_KEY;
        }
    }

    public static Set<Integer> getInParamsIndexes(ExecutionContext ec) {
        if (ec.getParams() == null) {
            return EMPTY_SET;
        }
        Set<Integer> rs = new HashSet<>();
        Map<Integer, ParameterContext> currentParameter = ec.getParams().getCurrentParameter();
        for (int i = 1; i <= currentParameter.size(); i++) {
            ParameterContext pc = currentParameter.get(i);
            if (pc != null && pc.getValue() instanceof RawString) {
                rs.add(i);
            }
        }
        return rs;
    }

    public static String buildInExprKey(ExecutionContext ec) {
        if (ec.getParams() == null) {
            return EMPTY_KEY;
        }
        return buildInExprKey(ec.getParams().getCurrentParameter());
    }

    public static Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new NotSupportException("Unable to parse the date: " + str);
    }

    public static Map<Integer, ParameterContext> buildParam(List<?> params) {
        return buildParam(params, null);
    }

    public static Map<Integer, ParameterContext> buildParam(List<?> params, ExecutionContext executionContext) {
        Int2ObjectOpenHashMap<ParameterContext> newParam = new Int2ObjectOpenHashMap<>();
        if (params == null) {
            return newParam;
        }
        for (int i = 0, j = 1; i < params.size(); i++, j++) {
            Object o = params.get(i);
            if (executionContext != null && executionContext.isExecutingPreparedStmt()) {
                if (o instanceof PreparedParamRef) {
                    o = ((PreparedParamRef) o).getValue();
                } else {
                    o = Planner.processSingleParam(i, o, executionContext);
                }
            }
            ParameterContext pc = new ParameterContext(getParameterMethod(o), new Object[] {j, o});
            newParam.put(j, pc);
        }
        return newParam;
    }

    /**
     * Batch prepare 参数构建
     */
    public static List<Map<Integer, ParameterContext>> buildBatchParam(List<Object> params,
                                                                       ExecutionContext executionContext) {
        List<Map<Integer, ParameterContext>> batchParameters = new ArrayList<>();
        for (Map<Integer, ParameterContext> oldMap : executionContext.getParams().getBatchParameters()) {
            Map<Integer, ParameterContext> newMap = new HashMap<>();
            for (int i = 0, j = 1; i < params.size(); i++, j++) {
                if (params.get(i) instanceof PreparedParamRef) {
                    PreparedParamRef preparedParamRef = (PreparedParamRef) params.get(i);
                    int index = preparedParamRef.getIndex() + 1;
                    ParameterContext oldCtx = oldMap.get(index);
                    ParameterContext newCtx =
                        new ParameterContext(oldCtx.getParameterMethod(), new Object[] {j, oldCtx.getValue()});
                    newMap.put(j, newCtx);
                } else {
                    Object o = params.get(i);
                    o = Planner.processSingleParam(i, o, executionContext);
                    ParameterContext pc = new ParameterContext(getParameterMethod(o), new Object[] {j, o});
                    newMap.put(j, pc);
                }
            }
            batchParameters.add(newMap);
        }
        return batchParameters;
    }

    public static ParameterMethod getParameterMethod(Object v) {
        if (v instanceof String) {
            return ParameterMethod.setString;
        } else if (v instanceof Boolean) {
            return ParameterMethod.setBoolean;
        } else {
            return ParameterMethod.setObject1;
        }
    }

    public static boolean supportedSqlKind(SqlNode ast) {
        switch (ast.getKind()) {
        case SELECT:
        case UNION:
        case INTERSECT:
        case EXCEPT:
        case ORDER_BY:
        case INSERT:
        case REPLACE:
        case UPDATE:
        case DELETE:
        case CREATE_MATERIALIZED_VIEW:
        case DROP_MATERIALIZED_VIEW:
        case REFRESH_MATERIALIZED_VIEW:
        case CREATE_VIEW:
        case DROP_VIEW:
        case CREATE_TABLE:
        case DROP_TABLE:
        case CREATE_INDEX:
        case DROP_INDEX:
        case RENAME_TABLE:
        case ALTER_INDEX:
        case ALTER_TABLE:
        case TRUNCATE_TABLE:
        case ALTER_SEQUENCE:
        case CREATE_SEQUENCE:
        case DROP_SEQUENCE:
        case FLASHBACK_TABLE:
        case PURGE:
        case RENAME_SEQUENCE:
        case EXPLAIN:
        case ALTER_RULE:
        case CREATE_DATABASE:
        case DROP_DATABASE:
        case CREATE_JAVA_FUNCTION:
        case DROP_JAVA_FUNCTION:
        case CHANGE_CONSENSUS_ROLE:
        case ALTER_SYSTEM_SET_CONFIG:
        case LOCK_TABLE:
        case CREATE_TRIGGER:
        case DROP_TRIGGER:
        case PUSH_DOWN_UDF:
        case CREATE_FUNCTION:
        case DROP_FUNCTION:
        case ALTER_FUNCTION:
        case ALTER_PROCEDURE:
        case CREATE_PROCEDURE:
        case DROP_PROCEDURE:
        case WITH:
        case WITH_ITEM:
        case ALTER_TABLEGROUP:
        case CREATE_TABLEGROUP:
        case DROP_TABLEGROUP:
        case UNARCHIVE:
        case ALTER_TABLE_SET_TABLEGROUP:
        case REFRESH_TOPOLOGY:
        case CREATE_SCHEDULE:
        case DROP_SCHEDULE:
        case ALTER_FILESTORAGE:
        case DROP_FILESTORAGE:
        case CLEAR_FILESTORAGE:
        case CREATE_FILESTORAGE:
        case PAUSE_SCHEDULE:
        case CONTINUE_SCHEDULE:
        case FIRE_SCHEDULE:
        case CREATE_JOINGROUP:
        case DROP_JOINGROUP:
        case ALTER_JOINGROUP:
        case MERGE_TABLEGROUP:
        case ALTER_DATABASE:
        case CREATE_STORAGE_POOL:
        case DROP_STORAGE_POOL:
        case ALTER_STORAGE_POOL:
        case INSPECT_INDEX:
        case IMPORT_DATABASE:
        case IMPORT_SEQUENCE:
        case CREATE_SECURITY_LABEL_COMPONENT:
        case DROP_SECURITY_LABEL_COMPONENT:
        case CREATE_SECURITY_LABEL:
        case DROP_SECURITY_LABEL:
        case CREATE_SECURITY_POLICY:
        case DROP_SECURITY_POLICY:
        case CREATE_SECURITY_ENTITY:
        case DROP_SECURITY_ENTITY:
        case GRANT_SECURITY_LABEL:
        case REVOKE_SECURITY_LABEL:
        case ALTER_INSTANCE:
            return true;
        default:
            if (ast.isA(SqlKind.DAL)) {
                return true;
            }
            return false;
        }
    }

    public static boolean findRexSubquery(RelNode rootRel) {
        class RexSubqueryParamFinder extends RelVisitor {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalFilter) {
                    if (RexUtil.hasSubQuery(((LogicalFilter) node).getCondition())) {
                        throw Util.FoundOne.NULL;
                    }
                } else if (node instanceof LogicalProject) {
                    if (((LogicalProject) node).getProjects().stream().anyMatch(rex -> RexUtil.hasSubQuery(rex))) {
                        throw Util.FoundOne.NULL;
                    }
                } else if (node instanceof LogicalSemiJoin) {
                    throw Util.FoundOne.NULL;
                }
                super.visit(node, ordinal, parent);
            }

            boolean run(RelNode node) {
                try {
                    go(node);
                    return false;
                } catch (Util.FoundOne e) {
                    return true;
                }
            }
        }

        return new RexSubqueryParamFinder().run(rootRel);
    }

    public static List<RexDynamicParam> findSubquery(RelNode rootRel) {
        class RelDynamicParamFinder extends RelVisitor {
            private List<RexDynamicParam> scalar = Lists.newArrayList();

            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalFilter) {
                    DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                    ((LogicalFilter) node).getCondition().accept(dynamicDeepFinder);
                } else if (node instanceof LogicalProject) {
                    for (RexNode r : ((LogicalProject) node).getProjects()) {
                        DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                        r.accept(dynamicDeepFinder);
                    }
                }
                super.visit(node, ordinal, parent);
            }

            List<RexDynamicParam> run(RelNode node) {
                go(node);
                return scalar;
            }
        }

        return new RelDynamicParamFinder().run(rootRel);
    }

    public static boolean findSemiJoin(RelNode rootRel) {
        class RelSemiJoinFinder extends RelVisitor {
            private boolean semiJoin = false;

            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof LogicalSemiJoin) {
                    semiJoin = true;
                }
                if (!semiJoin) {
                    super.visit(node, ordinal, parent);
                }
            }

            boolean run(RelNode node) {
                go(node);
                return semiJoin;
            }
        }

        return new RelSemiJoinFinder().run(rootRel);
    }

    public static Map<Pair<String, List<String>>, Parameters> pruningInValue(LogicalView lv,
                                                                             ExecutionContext context) {
        if (lv.hasTargetTableHint()) {
            return null;
        }
        if (!DynamicConfig.getInstance().isEnablePruningIn()) {
            return null;
        }
        Set<Integer> shardIndexes = lv.getPushDownOpt().getShardRelatedInTypeParamIndexes();
        if (shardIndexes != null) {
            Pair<Integer, RawString>[] rawStrings = findAllRawStrings(shardIndexes, context);
            int maxPruneTime = context.getParamManager().getInt(ConnectionParams.IN_PRUNE_MAX_TIME);
            Parameters requestParams = context.getParams();
            if (requestParams == null || rawStrings.length == 0) {
                return null;
            }

            PruningRawStringStep iterator = OptimizerUtils.iterateRawStrings(rawStrings, maxPruneTime);
            if (iterator == null) {
                return null;
            }
            // final result
            Map<Pair<String, List<String>>, Parameters> pruningResult;

            // start pruning
            long startTime = System.currentTimeMillis();
            Pair<Integer, PruneRawString>[] pruningPairArr = iterator.getTargetPair();

            // reuse parameter and execution context for pruning
            Parameters parameters = copy(requestParams, pruningPairArr, false);

            if (lv.isNewPartDbTbl()) {
                // pruning
                Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pair =
                    pruningPartTable(lv, iterator, parameters, context);
                if (pair == null) {
                    // meaning full scan
                    return null;
                }

                // merge
                pruningResult = mergePartTablePruningResult(pair, iterator.getTargetIndex());
            } else {
                Pair<Integer, Integer> pair = couldSimpleShard(lv, context);
                if (pair != null) {
                    // rawStrings length must be one in simple shard mode
                    int rawStringIndex = rawStrings[0].getKey();
                    List<Object> pruningObject = rawStrings[0].getValue().getObjList();

                    // simple pruning
                    Map<Integer, BitSet> bitSetMap = simplePruning(lv, context, rawStringIndex, pair.getValue());

                    // merge result
                    pruningResult =
                        mergeSimplePruning(lv, context, bitSetMap, rawStringIndex, pruningObject, parameters);
                } else {
                    // shard pruning has no middle pruning result:bitset. and this will make it merging slow
                    pruningResult = pruningShardTable(lv, iterator, context, parameters, pruningPairArr);
                }
            }
            long endTime = System.currentTimeMillis();
            long pruningTime = endTime - startTime;
            if (pruningTime > DynamicConfig.getInstance().getPruningTimeWarningThreshold()) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.PRUNING_SLOW, context);
            }
            context.addPruningTime(pruningTime);
            return pruningResult;
        }
        return null;
    }

    public static SortedMap<Long, Set<String>> pruningInValueForColumnar(LogicalView lv,
                                                                         ExecutionContext context,
                                                                         SortedMap<Long, PartitionInfo> multiVersionPartitionInfo) {
        if (!DynamicConfig.getInstance().isEnablePruningIn()) {
            return null;
        }
        Set<Integer> shardIndexes = lv.getPushDownOpt().getShardRelatedInTypeParamIndexes();
        if (shardIndexes != null) {
            // TODO(siyun): refactor duplicated codes
            Pair<Integer, RawString>[] rawStrings = findAllRawStrings(shardIndexes, context);
            int maxPruneTime =
                context.getParamManager().getInt(ConnectionParams.IN_PRUNE_MAX_TIME) / multiVersionPartitionInfo.size();
            Parameters requestParams = context.getParams();
            if (requestParams == null || rawStrings.length == 0) {
                return null;
            }

            // final result
            SortedMap<Long, Set<String>> allPruningResults = new ConcurrentSkipListMap<>();

            // start pruning
            long startTime = System.currentTimeMillis();

            if (lv.isNewPartDbTbl()) {
                // pruning
                for (Map.Entry<Long, PartitionInfo> partitionInfoEntry : multiVersionPartitionInfo.entrySet()) {
                    PruningRawStringStep iterator = OptimizerUtils.iterateRawStrings(rawStrings, maxPruneTime);
                    if (iterator == null) {
                        return null;
                    }
                    Pair<Integer, PruneRawString>[] pruningPairArr = iterator.getTargetPair();

                    // reuse parameter and execution context for pruning
                    Parameters parameters = copy(requestParams, pruningPairArr, false);
                    Long tso = partitionInfoEntry.getKey();
                    PartitionInfo partitionInfo = partitionInfoEntry.getValue();

                    Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pair =
                        pruningPartTable(lv, iterator, parameters, context, partitionInfo);

                    if (pair == null) {
                        // meaning full scan
                        // TODO(siyun): optimize: for full scan, return null and deal it with all partitions
                        PartitionPruneStep fullScanStep = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(
                            partitionInfo,
                            partitionInfo.getPartitionBy().getPartLevel(), true
                        );
                        PartPrunedResult tablePrunedResult =
                            PartitionPruner.doPruningByStepInfo(fullScanStep, context);
                        lv.filterBySelectedPartition(tablePrunedResult);

                        allPruningResults.put(tso, tablePrunedResult.getPrunedParttions().stream().map(
                            PhysicalPartitionInfo::getPartName
                        ).collect(Collectors.toSet()));
                    } else {
                        // merge
                        allPruningResults.put(tso, mergePartTablePruningResultIntoPartNames(pair));
                    }
                }
            } else {
                throw new RuntimeException("CCI must be a new partition table");
            }
            long endTime = System.currentTimeMillis();
            long pruningTime = endTime - startTime;
            if (pruningTime > DynamicConfig.getInstance().getPruningTimeWarningThreshold()) {
                OptimizerAlertManager.getInstance().log(OptimizerAlertType.PRUNING_SLOW, context);
            }
            context.addPruningTime(pruningTime);
            return allPruningResults;
        }
        return null;
    }

    /**
     * pruning and merge result in a same iterator, this might be really slow
     */
    private static Map<Pair<String, List<String>>, Parameters> pruningShardTable(LogicalView lv,
                                                                                 PruningRawStringStep iterator,
                                                                                 ExecutionContext context,
                                                                                 Parameters parameters,
                                                                                 Pair<Integer, PruneRawString>[] pruningPairArr) {
        Map<Pair<String, List<String>>, Parameters> pruningResult = Maps.newHashMap();
        ExecutionContext contextForInPruning = new ExecutionContext(context.getSchemaName());
        contextForInPruning.setParamManager(context.getParamManager());
        contextForInPruning.setParams(parameters);
        boolean checked = false;
        while (iterator.hasNext()) {
            iterator.next();
            Map<String, List<List<String>>> groupAndTables = lv.buildTargetTables(contextForInPruning);

            if (!checked) {
                checked = true;
                int shardCount = 0;
                for (List list : groupAndTables.values()) {
                    shardCount += list.size();
                }
                if (shardCount == lv.getTotalShardCount()) {
                    // meaning full scan
                    return null;
                }
            }

            // merge result
            for (Map.Entry<String, List<List<String>>> entry1 : groupAndTables.entrySet()) {
                String group = entry1.getKey();
                List<List<String>> tables = entry1.getValue();
                for (List<String> tbls : tables) {
                    Pair<String, List<String>> pairs = new Pair<>(group, tbls);
                    if (pruningResult.get(pairs) == null) {
                        pruningResult.put(pairs, copy(parameters, pruningPairArr, true));
                    } else {
                        mergeRawStringParameters(pruningResult.get(pairs), pruningPairArr);
                    }
                }
            }
        }
        return pruningResult;
    }

    /**
     * merge pruning result for simple shard table
     */
    private static Map<Pair<String, List<String>>, Parameters> mergeSimplePruning(LogicalView lv,
                                                                                  ExecutionContext executionContext,
                                                                                  Map<Integer, BitSet> bitSetMap,
                                                                                  int rawStringIndex,
                                                                                  List<Object> pruningObject,
                                                                                  Parameters parameters) {
        Map<Pair<String, List<String>>, Parameters> pruningResult = Maps.newHashMap();
        TddlRuleManager or = executionContext.getSchemaManager().getTddlRuleManager();
        TableRule tr = or.getTableRule(lv.getShardingTable());

        for (Map.Entry<Integer, BitSet> entry : bitSetMap.entrySet()) {
            int shard = entry.getKey();
            BitSet bitSet = entry.getValue();
            Pair<String, List<String>> pairs =
                new Pair<>(tr.getDbNames()[shard], Lists.newArrayList(tr.getTbNames()[shard]));
            Pair<Integer, PruneRawString>[] pruningPairArrTmp = new Pair[1];
            pruningPairArrTmp[0] = new Pair<>(rawStringIndex,
                new PruneRawString(pruningObject, PruneRawString.PRUNE_MODE.MULTI_INDEX, -1, -1, bitSet));
            pruningResult.put(pairs, copy(parameters, pruningPairArrTmp, false));
        }
        return pruningResult;
    }

    /**
     * simple pruning for shard table, pretty fast
     *
     * @return shard index -> bitset for pruning value
     */
    private static Map<Integer, BitSet> simplePruning(LogicalView lv, ExecutionContext executionContext,
                                                      int rawStringIndex, int skIndex) {
        SimpleShardProcessor simpleShardProcessor =
            ShardProcessor.buildSimple(lv, lv.getShardingTable(), executionContext);
        ParameterContext pc = executionContext.getParams().getCurrentParameter().get(rawStringIndex);
        RawString rawString = (RawString) pc.getValue();
        Map<Integer, BitSet> bitSetMap = Maps.newHashMap();
        for (int i = 0; i < rawString.size(); i++) {
            Object obj = rawString.getObj(i, skIndex);
            int shard = simpleShardProcessor.simpleShard(obj, executionContext.getEncoding());
            bitSetMap.computeIfAbsent(shard, k -> new BitSet()).set(i);
        }
        return bitSetMap;
    }

    private static Set<String> mergePartTablePruningResultIntoPartNames(
        Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pair) {
        Set<String> partNames = Sets.newHashSet();
        Map<List<BitSet>, Parameters> pruningMapForPartTable = pair.getKey();
        List<PartPrunedResult> partPrunedResultsCache = pair.getValue();

        // For CCI, each table scan has only one table
        Preconditions.checkArgument(partPrunedResultsCache.size() == 1);

        for (Map.Entry<List<BitSet>, Parameters> entry : pruningMapForPartTable.entrySet()) {
            // transform bitset to PartPrunedResult
            PartPrunedResult partPrunedResultTmp = partPrunedResultsCache.get(0).copy();
            partPrunedResultTmp.setPartBitSet(entry.getKey().get(0));
            for (PhysicalPartitionInfo physicalPartitionInfo : partPrunedResultTmp.getPrunedParttions()) {
                partNames.add(physicalPartitionInfo.getPartName());
            }
        }
        return partNames;
    }

    /**
     * merge pruning result for part table
     */
    private static Map<Pair<String, List<String>>, Parameters> mergePartTablePruningResult(
        Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pair, int[] pruningTargetIndex) {
        Map<Pair<String, List<String>>, Parameters> pruneRawStringMap = Maps.newHashMap();
        Map<List<BitSet>, Parameters> pruningMapForPartTable = pair.getKey();
        List<PartPrunedResult> partPrunedResultsCache = pair.getValue();

        for (Map.Entry<List<BitSet>, Parameters> entry : pruningMapForPartTable.entrySet()) {
            // transform bitset to PartPrunedResult
            List<PartPrunedResult> partPrunedResultsTmp = Lists.newArrayList();
            for (int i = 0; i < partPrunedResultsCache.size(); i++) {
                PartPrunedResult partPrunedResult = partPrunedResultsCache.get(i).copy();
                partPrunedResult.setPartBitSet(entry.getKey().get(i));
                partPrunedResultsTmp.add(partPrunedResult);
            }
            Map<String, List<List<String>>> groupAndTbl =
                PartitionPrunerUtils.buildTargetTablesByPartPrunedResults(partPrunedResultsTmp);

            Parameters beMerged = entry.getValue();
            // one bitset represents multi group&shard table
            for (Map.Entry<String, List<List<String>>> entry1 : groupAndTbl.entrySet()) {
                String group = entry1.getKey();
                List<List<String>> tables = entry1.getValue();

                for (List<String> tbls : tables) {
                    // logical view might have multi logical table pushed,
                    // tbls represents physical table for each logical table
                    Pair<String, List<String>> pairs = new Pair<>(group, tbls);
                    if (pruneRawStringMap.get(pairs) == null) {
                        pruneRawStringMap.put(pairs, copy(entry.getValue(), pruningTargetIndex, true));
                    } else {
                        Parameters merge = pruneRawStringMap.get(pairs);
                        for (int rawStringIndex : pruningTargetIndex) {
                            PruneRawString pruneRawStringMerge =
                                (PruneRawString) merge.getCurrentParameter().get(rawStringIndex).getValue();
                            PruneRawString pruneRawStringBeMerged =
                                (PruneRawString) beMerged.getCurrentParameter().get(rawStringIndex).getValue();
                            pruneRawStringMerge.merge(pruneRawStringBeMerged);
                        }
                    }
                }
            }
        }
        return pruneRawStringMap;
    }

    /**
     * pruning part table
     *
     * @param lv target logicalview
     * @param iterator pruning iterator
     * @param parameters pruning parameters
     * @param context execution context
     * @return pruning result and prunedResult
     */
    private static Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pruningPartTable(LogicalView lv,
                                                                                                PruningRawStringStep iterator,
                                                                                                Parameters parameters,
                                                                                                ExecutionContext context) {
        return pruningPartTable(lv, iterator, parameters, context, null);
    }

    public static Pair<Map<List<BitSet>, Parameters>, List<PartPrunedResult>> pruningPartTable(LogicalView lv,
                                                                                               PruningRawStringStep iterator,
                                                                                               Parameters parameters,
                                                                                               ExecutionContext context,
                                                                                               PartitionInfo cciPartInfo) {
        boolean checked = false;
        List<PartPrunedResult> partPrunedResultsCache = null;
        ExecutionContext contextForInPruning = new ExecutionContext(context.getSchemaName());
        contextForInPruning.setParamManager(context.getParamManager());
        contextForInPruning.setParams(parameters);

        Pair<Integer, PruneRawString>[] pruningPairArr = iterator.getTargetPair();

        // use bitset to represent part table when process iterate and pruning
        Map<List<BitSet>, Parameters> pruningMapForPartTable = Maps.newHashMap();

        while (iterator.hasNext()) {
            iterator.next();

            // pruning part table
            List<PartPrunedResult> partPrunedResults = cciPartInfo == null ?
                lv.getPartPrunedResults(contextForInPruning) :
                lv.getCciPartPrunedResults(contextForInPruning, cciPartInfo);

            // transform PartPrunedResult to bitset list
            List<BitSet> physicalPartBitSet = Lists.newArrayList();
            for (int i = 0; i < partPrunedResults.size(); i++) {
                physicalPartBitSet.add(partPrunedResults.get(i).getPhysicalPartBitSet());
            }

            // full scan check
            if (!checked) {
                checked = true;
                partPrunedResultsCache = partPrunedResults;

                int shardCount = partPrunedResults.get(0).getPartBitSet().cardinality();
                if (shardCount == partPrunedResults.get(0).getPartInfo().getAllPhysicalPartitionCount()) {
                    return null;
                }
            }

            // record pruning result by bitset list
            if (pruningMapForPartTable.get(physicalPartBitSet) == null) {
                pruningMapForPartTable.put(physicalPartBitSet, copy(parameters, pruningPairArr, true));
            } else {
                mergeRawStringParameters(pruningMapForPartTable.get(physicalPartBitSet), pruningPairArr);
            }
        }
        return Pair.of(pruningMapForPartTable, partPrunedResultsCache);
    }

    private static Parameters copy(Parameters parameters, int[] targetIndex, boolean clonePruning) {
        // deep copy
        Parameters newParams = parameters.clone();
        for (int index : targetIndex) {
            ParameterContext pc = newParams.getCurrentParameter().get(index);
            PruneRawString pruneRawString = (PruneRawString) pc.getValue();
            ParameterContext newContext = new ParameterContext();
            newContext.setParameterMethod(pc.getParameterMethod());
            newContext.setArgs(new Object[] {index, clonePruning ? pruneRawString.clone() : pruneRawString});
            newParams.getCurrentParameter().put(index, newContext);
        }
        return newParams;
    }

    private static Pair<Integer, Integer> couldSimpleShard(LogicalView lv, ExecutionContext executionContext) {
        // check rule
        if (lv.getTableNames().size() != 1) {
            return null;
        }
        String tableName = lv.getTableNames().get(0);
        TableRule tr =
            executionContext.getSchemaManager(lv.getSchemaName()).getTddlRuleManager().getTableRule(tableName);
        if (!ShardProcessor.isSimpleRule(tr)) {
            return null;
        }
        Map<String, Comparative> comparativeMap = lv.getComparative();
        if (comparativeMap.size() != 1) {
            return null;
        }
        if (tr.getShardColumns().size() != 1) {
            return null;
        }
        if (!comparativeMap.containsKey(tr.getShardColumns().get(0))) {
            return null;
        }

        // check comparative
        Comparative comparative = comparativeMap.values().iterator().next();
        if (comparative instanceof ComparativeOR) {
            ComparativeOR comparativeOR = (ComparativeOR) comparative;
            if (comparativeOR.getList().size() != 1) {
                return null;
            }
            Comparative comparativeSub = comparativeOR.getList().get(0);
            if (!(comparativeSub instanceof DynamicComparative)) {
                return null;
            }
            DynamicComparative dynamicComparative = (DynamicComparative) comparativeSub;
            if (dynamicComparative.getComparison() != Comparative.Equivalent) {
                return null;
            }
            Object value = dynamicComparative.getValue();
            if (!(value instanceof RexDynamicParam)) {
                return null;
            } else {
                return Pair.of(((RexDynamicParam) value).getIndex(), dynamicComparative.getSkIndex());
            }
        }
        return null;
    }

    private static Parameters copy(Parameters parameters, Pair<Integer, PruneRawString>[] pruningPairArr,
                                   boolean clonePruning) {
        // deep copy
        Parameters newParams = parameters.clone();
        if (pruningPairArr != null) {
            for (Pair<Integer, PruneRawString> p : pruningPairArr) {
                PruneRawString pruneRawString = p.getValue();
                ParameterContext pc = newParams.getCurrentParameter().get(p.getKey());
                ParameterContext newContext = new ParameterContext();
                newContext.setParameterMethod(pc.getParameterMethod());
                newContext.setArgs(new Object[] {p.getKey(), clonePruning ? pruneRawString.clone() : pruneRawString});
                newParams.getCurrentParameter().put(p.getKey(), newContext);
            }
        }
        return newParams;
    }

    public static PruningRawStringStep iterateRawStrings(Pair<Integer, RawString>[] rawStrings, int maxPruneTime) {
        if (rawStrings.length == 0) {
            return null;
        }
        if (rawStrings.length == 1) {
            Integer rawStringIndex = rawStrings[0].getKey();
            RawString rawString = rawStrings[0].getValue();
            int pruningTime = rawString.size();
            if (pruningTime > maxPruneTime) {
                return null;
            }
            return new SingleRawStringPruningIterator(rawString, rawStringIndex);
        }
        Map<Integer, RawString> rawStringPruningMap = new HashMap<>();
        int currentPruningTime = 1;
        for (int i = rawStrings.length - 1; i >= 0; i--) {
            Pair<Integer, RawString> pair = rawStrings[i];
            if (currentPruningTime * pair.getValue().size() > maxPruneTime) {
                return null;
            }
            currentPruningTime = currentPruningTime * pair.getValue().size();
            rawStringPruningMap.put(pair.getKey(), pair.getValue());
        }
        if (currentPruningTime == 1) {
            return null;
        }

        return new MultiRawStringPruningIterator(currentPruningTime, rawStringPruningMap);
    }

    static void mergeRawStringParameters(Parameters parameterContexts, Pair<Integer, PruneRawString>[] r) {
        for (Pair<Integer, PruneRawString> pair : r) {
            Integer index = pair.getKey();
            PruneRawString pruneRawString = pair.getValue();
            RawString rawString = (RawString) parameterContexts.getCurrentParameter().get(index).getValue();
            if (rawString instanceof PruneRawString) {
                ((PruneRawString) rawString).merge(pruneRawString);
            }
        }
    }

    public static Pair<Integer, RawString>[] findAllRawStrings(Set<Integer> shardIndexes,
                                                               ExecutionContext executionContext) {
        List<Pair<Integer, RawString>> allRawString = Lists.newArrayList();
        if (executionContext.getParams() != null && shardIndexes != null) {
            Map<Integer, ParameterContext> parameterContextMap = executionContext.getParams().getCurrentParameter();

            for (int shardIndex : shardIndexes) {
                ParameterContext parameterContext = parameterContextMap.get(shardIndex);
                if (parameterContext.getValue() instanceof RawString) {
                    RawString rawString = (RawString) parameterContext.getValue();
                    allRawString.add(Pair.of(shardIndex, rawString));
                } else {
                    // TODO warning
                }
            }
        }
        allRawString.sort(Comparator.comparingInt(o -> o.getValue().size()));
        return allRawString.toArray(new Pair[0]);
    }

    static public class DynamicDeepFinder extends RexVisitorImpl<Void> {
        private List<RexDynamicParam> scalar;

        public DynamicDeepFinder(List<RexDynamicParam> scalar) {
            super(true);
            this.scalar = scalar;
        }

        @Override
        public Void visitDynamicParam(RexDynamicParam dynamicParam) {
            if (dynamicParam.getIndex() == -2 || dynamicParam.getIndex() == -3) {
                scalar.add(dynamicParam);
            }
            return null;
        }

        public List<RexDynamicParam> getScalar() {
            return scalar;
        }
    }

    public static boolean hasSubquery(RelNode rootRel) {
        class SubqueryFinder extends RelVisitor {

            private List<RexDynamicParam> scalar = Lists.newArrayList();

            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {

                if (node == null) {
                    return;
                }

                if (node instanceof LogicalFilter) {
                    DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                    ((LogicalFilter) node).getCondition().accept(dynamicDeepFinder);
                } else if (node instanceof LogicalProject) {
                    for (RexNode r : ((LogicalProject) node).getProjects()) {
                        DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                        r.accept(dynamicDeepFinder);
                    }
                } else if (node instanceof LogicalView) {
                    if (((LogicalView) node).getCorrelateVariableScalar().size() > 0) {
                        throw Util.FoundOne.NULL;
                    }
                }
                if (scalar.size() > 0) {
                    throw Util.FoundOne.NULL;
                }

                super.visit(node, ordinal, parent);
            }

            boolean run(RelNode node) {
                try {
                    go(node);
                    return false;
                } catch (Util.FoundOne e) {
                    return true;
                }
            }
        }

        return new SubqueryFinder().run(rootRel);
    }

    public static boolean hasApply(RelNode rootRel) {
        class SubqueryFinder extends RelVisitor {

            private List<RexDynamicParam> scalar = Lists.newArrayList();

            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {

                if (node == null) {
                    return;
                }

                if (node instanceof LogicalFilter) {
                    DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                    ((LogicalFilter) node).getCondition().accept(dynamicDeepFinder);
                } else if (node instanceof LogicalProject) {
                    for (RexNode r : ((LogicalProject) node).getProjects()) {
                        DynamicDeepFinder dynamicDeepFinder = new DynamicDeepFinder(scalar);
                        r.accept(dynamicDeepFinder);
                    }
                } else if (node instanceof LogicalView) {
                    if (((LogicalView) node).getScalarList().size() > 0) {
                        throw Util.FoundOne.NULL;
                    }
                }
                if (scalar.size() > 0) {
                    throw Util.FoundOne.NULL;
                }

                super.visit(node, ordinal, parent);
            }

            boolean run(RelNode node) {
                try {
                    go(node);
                    return false;
                } catch (Util.FoundOne e) {
                    return true;
                }
            }
        }

        return new SubqueryFinder().run(rootRel);
    }

    public static boolean allowMultipleReadConns(ExecutionContext context, LogicalView logicalView) {
        boolean ret = useExplicitTransaction(context);
        if (ret) {
            boolean shareReadView = context.isShareReadView() && context.getTransaction().
                getTransactionClass().isA(SUPPORT_SHARE_READVIEW_TRANSACTION);
            if (!shareReadView && !context.isAutoCommit()) {
                return false;
            } else {
                if (!isSelectQuery(context)) {
                    return false;
                }
                if (logicalView != null) {
                    return ((IDistributedTransaction) context.getTransaction()).allowMultipleReadConns()
                        && logicalView.getLockMode() == SqlSelect.LockMode.UNDEF;
                } else {
                    return ((IDistributedTransaction) context.getTransaction()).allowMultipleReadConns();
                }
            }
        } else {
            return true;
        }
    }

    public static boolean isSelectQuery(ExecutionContext context) {
        if (context.getFinalPlan() == null || context.getFinalPlan().getAst() == null) {
            return context.getSqlType() == SqlType.SELECT;
        } else {
            return context.getFinalPlan().getAst().getKind().belongsTo(SqlKind.QUERY);
        }
    }

    public static boolean useExplicitTransaction(ExecutionContext context) {
        //Autocommit is true, but the GSI must be in transaction.
        boolean ret = context.getTransaction().getTransactionClass().isA(EXPLICIT_TRANSACTION);
        return ret && ConfigDataMode.isMasterMode() && !isMppMode(context);
    }

    public static boolean enableColumnarOptimizer(ParamManager paramManager) {
        if (paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER)) {
            return true;
        }
        return (paramManager.getBoolean(ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR)
            && DynamicConfig.getInstance().existColumnarNodes());
    }

    private static boolean isMppMode(ExecutionContext context) {
        return context.getExecuteMode() == ExecutorMode.MPP;
    }
}
