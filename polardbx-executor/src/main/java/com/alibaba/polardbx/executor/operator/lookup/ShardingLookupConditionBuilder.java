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

package com.alibaba.polardbx.executor.operator.lookup;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.google.common.collect.Iterables;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.partitionmanagement.PartitionUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.collect.Iterables;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder.getIdentifierByIndex;

/**
 * ShardingLookupConditionBuilder builds lookup condition for each shards
 *
 * @see LookupConditionBuilder
 */
public class ShardingLookupConditionBuilder extends LookupConditionBuilder {

    private static final String DEFAULT_TABLE = "DEFAULT_TABLE";

    private final List<ColumnMeta> shardingColumns;
    private final int[] shardingKeyPositions;
    private final List<String> joinKeyColumnNames;
    private final int[] shardingColumn2LookupSideInputRef;

    ShardingLookupConditionBuilder(List<EquiJoinKey> jk, LookupPredicate p, LogicalView v, ExecutionContext ec) {
        super(jk, p, v, ec);
        this.joinKeyColumnNames = collectJoinKeyColumns();
        this.shardingColumns = collectShardingColumnMeta();
        this.shardingKeyPositions = buildShardingKeyPositions(shardingColumns);
        this.shardingColumn2LookupSideInputRef = buildShardingColumn2LookupSideInputRef(shardingColumns);
    }

    public Map<String, Map<String, SqlNode>> buildShardedCondition(Chunk joinKeysChunk, ExecutionContext context) {
        final OptimizerContext oc = OptimizerContext.getContext(v.getSchemaName());
        TddlRuleManager ruleManager = context.getSchemaManager(v.getSchemaName()).getTddlRuleManager();
        /*
         * Lookup Join 的 equi-condition columns 必须包含 sharding columns，因此有以下几种情况
         * 1. J = {a} , S = {a}
         * 2. J = {a, b} , S = {a, b}
         * 3. J = {a, b} , S = {a}
         * 4. J = {a, b, c} , S = {a, b}
         * 5. J = {a}, S = {a, b} - 由于分表键 b 未覆盖，仅能进行分库
         * 6. J = {a, c}, S = {a, b} - 同上
         */
        if (isSinglePredicateShardingKey()) {
            if (canUseSimpleShard(ruleManager.getTableRule(v.getShardingTable()))) {
                SimpleShardProcessor shardProcessor = ShardProcessor.buildSimple(v, v.getShardingTable(), context);
                return buildShardedConditionBySimpleProcessor(
                    buildTupleIterable(joinKeysChunk),
                    shardProcessor,
                    context);
            }
            if (!isAntiJoin()) {
                Chunk lookupKeysChunk = extractLookupKeys(joinKeysChunk);
                Iterable<Tuple> distinctLookupKeys = distinctLookupKeysChunk(lookupKeysChunk);
                return buildSimpleShardedCondition(
                    p.getColumn(0),
                    extractSimpleValues(distinctLookupKeys),
                    shardingColumns.get(0),
                    ruleManager,
                    oc.getPartitionInfoManager(),
                    context
                );
            }
        }
        return buildGeneralShardedCondition(
            buildTupleIterable(joinKeysChunk),
            shardingColumns,
            ruleManager.getTableRule(v.getShardingTable()),
            ruleManager,
            oc.getPartitionInfoManager(),
            context
        );
    }

    /**
     * 谓词列与分片键均为一列且相同
     * 且符合简单分片规则
     */
    private boolean canUseSimpleShard(TableRule tableRule) {
        if (p.size() != 1 || shardingColumns.size() != 1) {
            return false;
        }
        // canShard() ensures joinkey contains shardingkey
        boolean joinKeysContainPredicate = false;
        for (String joinKey : joinKeyColumnNames) {
            if (joinKey.equalsIgnoreCase(p.getColumn(0).getSimple())) {
                joinKeysContainPredicate = true;
                break;
            }
        }
        if (!joinKeysContainPredicate) {
            return false;
        }

        return !v.isNewPartDbTbl() && ShardProcessor.isSimpleRule(tableRule);
    }

    /**
     * 谓词列与分片键均为一列且相同
     */
    private boolean isSinglePredicateShardingKey() {
        return p.size() == 1 && shardingColumns.size() == 1
            && p.getColumn(0).getSimple().equalsIgnoreCase(shardingColumns.get(0).getName());
    }

    private Map<String, Map<String, SqlNode>> buildShardedConditionBySimpleProcessor(Iterable<Tuple> joinKeyTuples,
                                                                                     SimpleShardProcessor shardProcessor,
                                                                                     ExecutionContext context) {
        assert shardingColumns.size() == 1;
        assert p.size() == 1;
        Map<String, Map<String, Set<Object>>> shardedValues = new HashMap<>();

        assert !v.isNewPartDbTbl();
        final Map<String, Set<String>> topology = shardProcessor.getTableRule().getActualTopology();

        topology.forEach((dbKey, tables) -> {
            Map<String, Set<Object>> tableValues = new HashMap<>();
            for (String table : tables) {
                tableValues.put(table, null);
            }
            shardedValues.put(dbKey, tableValues);
        });

        for (Tuple tuple : joinKeyTuples) {
            // since p.size() == 1, we can simplify null value condition
            Object lookupValue = tuple.get(0);
            if (lookupValue == null) {
                if (isAntiJoin()) {
                    return Collections.emptyMap();
                } else {
                    continue;
                }
            }
            Object shardValue = tuple.get(shardingKeyPositions[0]);
            // get lookup dbTable by sharding value
            Pair<String, String> dbAndTable = shardProcessor.shard(shardValue, context);
            Set<Object> valueSet = shardedValues.get(dbAndTable.getKey())
                .computeIfAbsent(dbAndTable.getValue(), (table) -> new HashSet<>());
            valueSet.add(lookupValue);
        }

        Map<String, Map<String, SqlNode>> shardedCondition = new HashMap<>();
        SqlIdentifier key = p.getColumn(0);

        for (Map.Entry<String, Map<String, Set<Object>>> shardedValueEntry : shardedValues.entrySet()) {
            String dbIndex = shardedValueEntry.getKey();
            for (Map.Entry<String, Set<Object>> tableValueEntry : shardedValueEntry.getValue().entrySet()) {
                String tbName = tableValueEntry.getKey();
                Set<Object> valueSet = tableValueEntry.getValue();
                SqlNode node = null;
                if (valueSet != null && valueSet.size() != 0) {
                    SqlNodeList sqlNodeList = new SqlNodeList(SqlParserPos.ZERO);
                    for (Object value : valueSet) {
                        sqlNodeList.add(createLiteralValue(value));
                    }
                    node = new SqlBasicCall(p.getOperator(), new SqlNode[] {key, sqlNodeList}, SqlParserPos.ZERO);
                } else if (isAntiJoin()) {
                    node = buildNotNullCondition(key);
                }

                if (node != null) {
                    shardedCondition.computeIfAbsent(dbIndex, (db) -> new HashMap<>()).put(tbName, node);
                }
            }
        }
        return shardedCondition;
    }

    private Map<String, Map<String, SqlNode>> buildSimpleShardedCondition(
        SqlIdentifier key, List<Object> values,
        ColumnMeta shardingKeyMeta, TddlRuleManager tddlRuleManager, PartitionInfoManager partitionInfoManager,
        ExecutionContext context) {

        Map<Integer, ParameterContext> params =
            context.getParams() == null ? null : context.getParams().getCurrentParameter();
        final List<TargetDB> targetDbList;
        if (!v.isNewPartDbTbl()) {
            Map<String, Comparative> comparatives = TddlRuleManager.getComparativeORWithSingleColumn(
                shardingKeyMeta, values, shardingKeyMeta.getName());

            // fullComparative保障了分表条件可见
            Map<String, Comparative> fullComparative = TddlRuleManager.getInsertFullComparative(comparatives);
            Map<String, Object> calcParams = new HashMap<>();
            calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
            calcParams.put(CalcParamsAttribute.COM_DB_TB, fullComparative);
            calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, context.getTimeZone());

            targetDbList = tddlRuleManager.shard(v.getShardingTable(), false, false,
                comparatives, params, calcParams, context);
        } else {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(v.getLogicalTableName());

            RexBuilder rexBuilder = v.getCluster().getRexBuilder();
            int targetIndex = shardingColumn2LookupSideInputRef[0];
            List<RexNode> rexValues = values.stream().map(
                x -> rexBuilder.makeLiteral(shardingKeyMeta.getDataType().convertJavaFrom(x),
                    v.getRowType().getFieldList().get(targetIndex).getType(),
                    false)).collect(Collectors.toList());
            RexNode inRow = rexBuilder.makeCall(TddlOperatorTable.ROW, rexValues);
            RexNode in = rexBuilder.makeCall(TddlOperatorTable.IN, rexBuilder.makeInputRef(v, targetIndex), inRow);

            PartitionPruneStep
                pruneStepInfo = PartitionPruner.generatePartitionPrueStepInfo(partitionInfo, v, in, context);
            ExecutionContext tmpContext = context.copy();
            PartPrunedResult partPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);

            targetDbList = PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(partPrunedResult);
        }

        Map<String, Map<String, SqlNode>> shardedConditions = new HashMap<>(targetDbList.size());
        for (TargetDB targetDB : targetDbList) {
            Map<String, SqlNode> tableConditions = new HashMap<>();
            for (Map.Entry<String, Field> tableNameField : targetDB.getTableNameMap().entrySet()) {
                Field field = tableNameField.getValue();
                SqlNode sqlNode;
                if (field == null) {
                    // 考虑不带sourceKey的情况
                    sqlNode = buildSimpleCondition(key, values);
                } else {
                    sqlNode = buildSimpleCondition(key, field.getSourceKeys().get(key.getSimple()));
                }
                if (sqlNode == FALSE_CONDITION) {
                    continue;
                }
                tableConditions.put(tableNameField.getKey(), sqlNode);
            }
            shardedConditions.put(targetDB.getDbIndex(), tableConditions);
        }
        return shardedConditions;
    }

    private Map<String, Map<String, SqlNode>> buildGeneralShardedCondition(
        Iterable<Tuple> joinKeyTuples, List<ColumnMeta> shardingKeyMetas,
        TableRule rule, TddlRuleManager tddlRuleManager, PartitionInfoManager partitionInfoManager,
        ExecutionContext context) {

        PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(v.getLogicalTableName());

        final boolean shardByTable;
        if (!v.isNewPartDbTbl()) {
            // ensured by `canShard()`
            assert containsAllIgnoreCase(joinKeyColumnNames, rule.getDbPartitionKeys());

            // If shard-by-table is unavailable, we could use a 'DEFAULT_TABLE' as placeholder to represent any
            // table in one group, and unfold it to the actual topology later. This optimization helps deduce memory
            // usage while number of table partitions is very high
            shardByTable = containsAllIgnoreCase(joinKeyColumnNames, rule.getTbPartitionKeys());
        } else {
            // ensured by `canShard()`
            assert containsAllIgnoreCase(joinKeyColumnNames, partitionInfo.getPartitionColumns());
            shardByTable = true;
        }

        Map<Integer, ParameterContext> params =
            context.getParams() == null ? null : context.getParams().getCurrentParameter();

        // reused objects
        List<Object> shardingKeyValues = new ArrayList<>(jk.size());
        for (int i = 0; i < shardingKeyMetas.size(); i++) {
            shardingKeyValues.add(new Object());
        }

        // db_name -> tb_name -> list of tuples
        Map<String, Map<String, List<Tuple>>> shardedTuples = new HashMap<>();
        final Map<String, Set<String>> topology;

        if (!v.isNewPartDbTbl()) {
            topology = rule.getActualTopology();
        } else {
            topology = PartitionUtils.partitionInfoToTopology(partitionInfo);
        }

        topology.forEach((dbKey, tables) -> {
            Map<String, List<Tuple>> tableValues = new HashMap<>();
            if (shardByTable) {
                for (String table : tables) {
                    tableValues.put(table, new ArrayList<>());
                }
            } else {
                tableValues.put(DEFAULT_TABLE, new ArrayList<>());
            }
            shardedTuples.put(dbKey, tableValues);
        });
        Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
        calcParams.put(CalcParamsAttribute.COM_DB_TB, new Object());
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, context.getTimeZone());

        for (Tuple tuple : joinKeyTuples) {
            for (int i = 0; i < shardingKeyPositions.length; i++) {
                shardingKeyValues.set(i, tuple.get(shardingKeyPositions[i]));
            }

            if (isAntiJoin() && containsNull(shardingKeyValues)) {
                /*
                 * For Anti-Join, the result set must be empty if the predicate contains only one column,
                 *
                 *   select * from t where x not in (..., null, ...)
                 *
                 * otherwise, the values with NULL must be preserved in all shards, for example,
                 *
                 *   select * from t where x not in (NULL, 1, 2)  --> returns 0 rows
                 *
                 * must be sharded as
                 *
                 *   select * from t_0 where x not in (NULL, 2)  --> return 0 rows
                 *   select * from t_1 where x not in (NULL, 1)  --> return 0 rows
                 *
                 * instead of
                 *
                 *   select * from t_0 where x not in (NULL, 2)  --> return 0 rows
                 *   select * from t_0 where x not in (1)        --> return some rows which is incorrect
                 */
                if (p.size() == 1) {
                    return Collections.emptyMap();
                }
                for (Map<String, List<Tuple>> tableMap : shardedTuples.values()) {
                    for (List<Tuple> tableTuples : tableMap.values()) {
                        tableTuples.add(tuple);
                    }
                }
                continue;
            }

            final List<TargetDB> targetDbs;

            if (!v.isNewPartDbTbl()) {
                Map<String, Comparative> comparatives =
                    TddlRuleManager.getLookupComparative(shardingKeyValues, shardingKeyMetas);

                // fullComparative保障了分表条件可见
                Map<String, Comparative> fullComparative = TddlRuleManager.getInsertFullComparative(comparatives);
                calcParams.put(CalcParamsAttribute.COM_DB_TB, fullComparative);

                targetDbs = tddlRuleManager.shard(v.getShardingTable(), false, false,
                    comparatives, params, calcParams, context);
            } else {
                RexBuilder rexBuilder = v.getCluster().getRexBuilder();

                RexNode condition;
                List<RexNode> equals = new ArrayList<>();
                for (int k = 0; k < shardingKeyMetas.size(); k++) {
                    Object convertedValue =
                        shardingKeyMetas.get(k).getDataType().convertJavaFrom(shardingKeyValues.get(k));
                    RexNode equal = rexBuilder.makeCall(TddlOperatorTable.EQUALS,
                        rexBuilder.makeInputRef(v, shardingColumn2LookupSideInputRef[k]),
                        rexBuilder.makeLiteral(convertedValue,
                            v.getRowType().getFieldList()
                                .get(shardingColumn2LookupSideInputRef[k]).getType(),
                            false));
                    equals.add(equal);
                }
                if (equals.size() == 1) {
                    condition = equals.get(0);
                } else {
                    condition = rexBuilder.makeCall(TddlOperatorTable.AND, equals);
                }
                PartitionPruneStep
                    pruneStepInfo =
                    PartitionPruner.generatePartitionPrueStepInfo(partitionInfo, v, condition, context);
                PartPrunedResult partPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);

                targetDbs = PartitionPrunerUtils.buildTargetDbsByPartPrunedResults(partPrunedResult);
            }

            if (!v.isNewPartDbTbl()) {
                if (targetDbs.size() != 1) {
                    throw new RuntimeException("expect one target db"); // see canShard()
                }
            } else {
                if (targetDbs.isEmpty()) {
                    continue;
                }
            }

            for ( TargetDB targetDb : targetDbs) {
                Collection<String> targetTables;
                if (shardByTable) {
                    targetTables = targetDb.getTableNames();
                } else {
                    targetTables = Collections.singleton(DEFAULT_TABLE);
                }
                for (String targetTable : targetTables) {
                    shardedTuples.compute(targetDb.getDbIndex(), (db, tableMap) -> {
                        assert tableMap != null;
                        tableMap.compute(targetTable, (tb, values) -> {
                            assert values != null;
                            values.add(tuple);
                            return values;
                        });
                        return tableMap;
                    });
                }
            }
        }

        // db_name -> tb_name -> condition
        Map<String, Map<String, SqlNode>> shardedCondition = new HashMap<>();
        shardedTuples.forEach((db, tableMap) -> {
            Map<String, SqlNode> tableCondMap = new HashMap<>();
            tableMap.forEach((tb, tableTuples) -> {
                if (!tableTuples.isEmpty()) {
                    SqlNode condition = buildCondition(tableTuples);
                    if (condition != FALSE_CONDITION) {
                        tableCondMap.put(tb, condition);
                    }
                } else if (isAntiJoin()) {
                    assert p.size() == 1; // see 'canShard()'
                    tableCondMap.put(tb, buildNotNullCondition(p.getColumn(0)));
                }
            });
            if (!tableCondMap.isEmpty()) {
                shardedCondition.put(db, tableCondMap);
            }
        });

        if (!shardByTable) {
            // unfold groups -> DEFAULT_TABLE -> sharded condition
            // to groups -> actual tables -> sharded condition
            Map<String, Map<String, SqlNode>> unfoldedShardedCondition = new HashMap<>(shardedCondition.size());
            topology.forEach((dbKey, tables) -> {
                Map<String, SqlNode> t = shardedCondition.get(dbKey);
                if (t != null) {
                    final SqlNode cond = t.get(DEFAULT_TABLE);
                    Map<String, SqlNode> tableCond = new HashMap<>(tables.size());
                    for (String table : tables) {
                        tableCond.put(table, cond);
                    }
                    unfoldedShardedCondition.put(dbKey, tableCond);
                }
            });
            return unfoldedShardedCondition;
        }
        return shardedCondition;
    }

    private SqlNode buildCondition(Collection<Tuple> joinKeyTuples) {
        Iterable<Tuple> lookupKeys = extractLookupKeys(joinKeyTuples);
        Collection<Tuple> distinctLookupKeys = distinctLookupKeysChunk(lookupKeys);

        if (p.size() == 1) {
            List<Object> flattedValues = distinctLookupKeys.stream()
                .map(b -> b.get(0))
                .collect(Collectors.toList());
            return buildSimpleCondition(p.getColumn(0), flattedValues);
        } else {
            return buildMultiCondition(distinctLookupKeys);
        }
    }

    private List<Object> extractSimpleValues(Iterable<Tuple> chunk) {
        assert p.size() == 1;
        List<Object> values = new ArrayList<>();
        // Pick the target values from chunk and convert to row-oriented layout
        for (Tuple tuple : chunk) {
            values.add(tuple.get(0));
        }
        return values;
    }

    private int[] buildShardingKeyPositions(List<ColumnMeta> shardingKeyMetas) {
        int[] shardingKeyPositionInValue = new int[shardingKeyMetas.size()];
        for (int i = 0; i < shardingKeyMetas.size(); i++) {
            int position = -1;
            for (int j = 0; j < jk.size(); j++) {
                int joinKeyPosition;
                RelNode lookupSide;
                if (isMaterializedSemiJoin()) {
                    joinKeyPosition = jk.get(j).getOuterIndex();
                    lookupSide = v.getJoin().getLeft();
                } else {
                    joinKeyPosition = jk.get(j).getInnerIndex();
                    lookupSide = v.getJoin().getInner();
                }
                String joinColumnName = getIdentifierByIndex(lookupSide, joinKeyPosition).getSimple();
                if (shardingKeyMetas.get(i).getName().equalsIgnoreCase(joinColumnName)) {
                    position = j;
                    break;
                }
            }
            if (position == -1) {
                throw new AssertionError("impossible: sharding column not found");
            }
            shardingKeyPositionInValue[i] = position;
        }
        return shardingKeyPositionInValue;
    }

    private int[] buildShardingColumn2LookupSideInputRef(List<ColumnMeta> shardingKeyMetas) {
        int[] shardingColumn2LookupSideInputRef = new int[shardingKeyMetas.size()];
        for (int i = 0; i < shardingKeyMetas.size(); i++) {
            int position = -1;
            for (int j = 0; j < jk.size(); j++) {
                int joinKeyPosition;
                RelNode lookupSide;
                if (isMaterializedSemiJoin()) {
                    joinKeyPosition = jk.get(j).getOuterIndex();
                    lookupSide = v.getJoin().getLeft();
                } else {
                    joinKeyPosition = jk.get(j).getInnerIndex();
                    lookupSide = v.getJoin().getInner();
                }
                String joinColumnName = getIdentifierByIndex(lookupSide, joinKeyPosition).getSimple();
                if (shardingKeyMetas.get(i).getName().equalsIgnoreCase(joinColumnName)) {
                    position = joinKeyPosition;
                    break;
                }
            }
            if (position == -1) {
                throw new AssertionError("impossible: sharding column not found");
            }
            shardingColumn2LookupSideInputRef[i] = position;
        }
        return shardingColumn2LookupSideInputRef;
    }

    private static boolean containsNull(List<Object> values) {
        for (Object value : values) {
            if (value == null) {
                return true;
            }
        }
        return false;
    }

    private Iterable<Tuple> extractLookupKeys(Iterable<Tuple> joinKeysTuple) {
        return Iterables.transform(joinKeysTuple, t -> {
            if (t == null) {
                return null;
            } else {
                Object[] data = new Object[p.size()];
                for (int i = 0; i < p.size(); i++) {
                    data[i] = t.get(lookupColumnPositions[i]);
                }
                return new Tuple(data);
            }
        });
    }

    private Collection<Tuple> distinctLookupKeysChunk(Iterable<Tuple> lookupKeysTuples) {
        // Distinct by HashSet
        Set<Tuple> distinctLookupKeys = new HashSet<>();
        for (Tuple tuple : lookupKeysTuples) {
            distinctLookupKeys.add(tuple);
        }
        return distinctLookupKeys;
    }

    private List<ColumnMeta> collectShardingColumnMeta() {
        final OptimizerContext oc = OptimizerContext.getContext(v.getSchemaName());
        final TableMeta tableMeta = ec.getSchemaManager(v.getSchemaName()).getTable(v.getShardingTable());
        List<String> shardColumns = oc.getRuleManager().getSharedColumns(v.getShardingTable());

        return shardColumns.stream()
            // filter the sharding columns covered by lookup predicate
            .filter(c -> containsIgnoreCase(joinKeyColumnNames, c))
            .map(tableMeta::getColumnIgnoreCase)
            .collect(Collectors.toList());
    }

    private static boolean containsIgnoreCase(Collection<String> a, String b) {
        List<String> ca = a.stream().map(String::toLowerCase).collect(Collectors.toList());
        return ca.contains(b.toLowerCase());
    }
}
