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

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.ChunkHashSet;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder.getIdentifierByIndex;

/**
 * LookupConditionBuilder builds the filter condition (SqlNode) according to provided LookupPredicate
 * Depending on the columns of predicates, the built result can be in following forms:
 * <p>
 * 1. simple condition  e.g. a IN (1,2,3)
 * <p>
 * 2. multi conditions  e.g. (a, b) IN ((1,2), (3,4), (5,6))
 * <p>
 * 3. boolean constants e.g. TRUE, FALSE
 */
public class LookupConditionBuilder {

    static final SqlNode TRUE_CONDITION = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    static final SqlNode FALSE_CONDITION = SqlLiteral.createBoolean(false, SqlParserPos.ZERO);

    final List<EquiJoinKey> jk;
    final LookupPredicate p;
    final LogicalView v;

    final int[] lookupColumnPositions;
    protected final ExecutionContext ec;

    /**
     * Join keys (jk) are used in sharding, while lookup predicates (p) are used in building IN expression.
     * <p>
     * Lookup predicates (p) must be a subset of join keys (jk).
     */
    public LookupConditionBuilder(List<EquiJoinKey> jk, LookupPredicate p, LogicalView v, ExecutionContext ec) {
        this.jk = jk;
        this.p = p;
        this.v = v;
        this.lookupColumnPositions = buildLookupColumnPositions();
        this.ec = ec;
    }

    public SqlNode buildCondition(Chunk joinKeysChunk) {
        Chunk lookupKeys = extractLookupKeys(joinKeysChunk);
        Iterable<Tuple> distinctLookupKeys = distinctLookupKeysChunk(lookupKeys);
        if (p.size() == 1) {
            return buildSimpleCondition(p.getColumn(0), extractSimpleValues(distinctLookupKeys));
        } else {
            return buildMultiCondition(distinctLookupKeys);
        }
    }

    /**
     * Build SQL expression via IN or NOT IN expression  e.g. a IN (1,2,3)
     */
    SqlNode buildSimpleCondition(SqlIdentifier key, Collection<Object> distinctValues) {
        SqlNodeList inValues = new SqlNodeList(SqlParserPos.ZERO);
        for (Object value : distinctValues) {
            if (value == null && !isAntiJoin()) {
                continue; // leave out NULLs
            }
            inValues.add(createLiteralValue(value));
        }
        if (inValues.size() > 0) {
            return new SqlBasicCall(p.getOperator(), new SqlNode[] {key, inValues}, SqlParserPos.ZERO);
        } else {
            return isAntiJoin() ? buildNotNullCondition(key) : FALSE_CONDITION;
        }
    }

    SqlNode buildNotNullCondition(SqlIdentifier key) {
        return new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, new SqlNode[] {key}, SqlParserPos.ZERO);
    }

    /**
     * Build SQL expression via Row-expression IN or NOT IN expression  e.g. (a, b) IN ((1,2), (3,4), (5,6))
     */
    SqlNode buildMultiCondition(Iterable<Tuple> distinctLookupKeys) {
        SqlNode[] names = new SqlNode[p.size()];
        for (int i = 0; i < p.size(); i++) {
            names[i] = p.getColumn(i);
        }
        SqlBasicCall left = new SqlBasicCall(TddlOperatorTable.ROW, names, SqlParserPos.ZERO);

        List<SqlNode[]> allValues = new ArrayList<>();
        for (Tuple tuple : distinctLookupKeys) {
            SqlNode[] inValues = new SqlNode[tuple.size()];
            for (int i = 0; i < tuple.size(); i++) {
                inValues[i] = createLiteralValue(tuple.get(i));
            }
            allValues.add(inValues);
        }

        SqlNode[] rows = new SqlNode[allValues.size()];
        for (int i = 0; i < allValues.size(); i++) {
            rows[i] = new SqlBasicCall(TddlOperatorTable.ROW, allValues.get(i), SqlParserPos.ZERO);
        }
        SqlNodeList right = new SqlNodeList(Arrays.asList(rows), SqlParserPos.ZERO);
        return new SqlBasicCall(p.getOperator(), new SqlNode[] {left, right}, SqlParserPos.ZERO);
    }

    /**
     * Can do shard if join predicate contains (all) DB sharding key(s)
     */
    public boolean canShard() {
        // Fix #32619857: For MaterializedSemiJoin with NOT_IN operator, the equi-predicate must be one column
        if (isAntiJoin() && p.size() > 1) {
            return false;
        }

        String logTbNale = v.getShardingTable();
        String schemaName = v.getSchemaName();
        TddlRuleManager tddlRuleManager = OptimizerContext.getContext(schemaName).getRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        boolean isPartedTb = partitionInfoManager.isNewPartDbTable(logTbNale);

        List<String> joinKeyColumnNames = collectJoinKeyColumns();
        if (!isPartedTb) {
            TableRule rule = tddlRuleManager.getTableRule(v.getShardingTable());
            if (rule == null) {
                return false;
            }
            // Collect original name of join keys on the lookup table
            if (CollectionUtils.isEmpty(rule.getDbPartitionKeys())) {
                return containsAllIgnoreCase(joinKeyColumnNames, rule.getTbPartitionKeys());
            }
            return containsAllIgnoreCase(joinKeyColumnNames, rule.getDbPartitionKeys());
        } else {
            PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logTbNale);
            List<String> allPartCols = partInfo.getPartitionColumns();
            return containsAllIgnoreCase(joinKeyColumnNames, allPartCols);
        }

    }

    public ShardingLookupConditionBuilder createSharding() {
        return new ShardingLookupConditionBuilder(jk, p, v, ec);
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

    private int[] buildLookupColumnPositions() {
        int[] lookupColumnPositions = new int[p.size()];
        for (int i = 0; i < p.size(); i++) {
            int position = -1;
            for (int j = 0; j < jk.size(); j++) {
                int targetIndex = isMaterializedSemiJoin() ? jk.get(j).getInnerIndex() : jk.get(j).getOuterIndex();
                if (targetIndex == p.getTargetIndex(i)) {
                    position = j;
                    break;
                }
            }
            if (position == -1) {
                throw new AssertionError("impossible: lookup column not found");
            }
            lookupColumnPositions[i] = position;
        }
        return lookupColumnPositions;
    }

    static SqlNode createLiteralValue(Object value) {
        if (value == null) {
            return SqlLiteral.createNull(SqlParserPos.ZERO);
        } else if (value instanceof Boolean) {
            return SqlLiteral.createBoolean((Boolean) value, SqlParserPos.ZERO);
        } else if (value instanceof byte[]) {
            return SqlLiteral.createBinaryString((byte[]) value, SqlParserPos.ZERO);
        }
        String strValue = DataTypes.StringType.convertFrom(value);
        if (value instanceof Number) {
            return SqlLiteral.createExactNumeric(strValue, SqlParserPos.ZERO);
        } else {
            return SqlLiteral.createCharString(strValue, SqlParserPos.ZERO);
        }
    }

    /**
     * Is MaterializedSemiJoin and joinType == ANTI ?
     */
    boolean isAntiJoin() {
        return p.getOperator() == SqlStdOperatorTable.NOT_IN;
    }

    /**
     * Is MaterializedSemiJoin?
     */
    boolean isMaterializedSemiJoin() {
        return v.getJoin() instanceof MaterializedSemiJoin;
    }

    /**
     * Extract lookup columns from join key chunk
     */
    Chunk extractLookupKeys(Chunk joinKeysChunk) {
        Block[] lookupColumns = new Block[p.size()];
        for (int i = 0; i < p.size(); i++) {
            lookupColumns[i] = joinKeysChunk.getBlock(lookupColumnPositions[i]);
        }
        return new Chunk(lookupColumns);
    }

    Iterable<Tuple> distinctLookupKeysChunk(Chunk lookupKeysChunk) {
        ChunkHashSet set = new ChunkHashSet(p.getDataTypes(), lookupKeysChunk.getPositionCount(), 1024, ec);
        set.addChunk(lookupKeysChunk);
        List<Chunk> chunks = set.buildChunks();

        // flatten list of chunks to list of rows
        return () -> {
            List<Iterator<Tuple>> iterators = chunks.stream()
                .map(Chunk::iterator)
                .map(it -> Iterators.transform(it, row -> row != null ? Tuple.from(row) : null))
                .collect(Collectors.toList());
            return Iterators.concat(iterators.iterator());
        };
    }

    static class Tuple {

        private final Object[] data;

        Tuple(Object... data) {
            this.data = Preconditions.checkNotNull(data);
        }

        static Tuple from(Row row) {
            Object[] data = new Object[row.getColNum()];
            for (int i = 0; i < row.getColNum(); i++) {
                data[i] = row.getObject(i);
            }
            return new Tuple(data);
        }

        public Object get(int i) {
            return data[i];
        }

        public int size() {
            return data.length;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Tuple tuple = (Tuple) o;
            return Arrays.equals(data, tuple.data);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }
    }

    static Iterable<Tuple> buildTupleIterable(Chunk chunk) {
        return Iterables.transform(chunk, row -> row != null ? Tuple.from(row) : null);
    }

    List<String> collectJoinKeyColumns() {
        return jk.stream().map(key -> {
            int joinKeyPosition;
            RelNode lookupSide;
            if (isMaterializedSemiJoin()) {
                joinKeyPosition = key.getOuterIndex();
                lookupSide = v.getJoin().getLeft();
            } else {
                joinKeyPosition = key.getInnerIndex();
                lookupSide = v.getJoin().getInner();
            }
            return getIdentifierByIndex(lookupSide, joinKeyPosition).getSimple();
        }).collect(Collectors.toList());
    }

    static boolean containsAllIgnoreCase(Collection<String> a, Collection<String> b) {
        List<String> ca = a.stream().map(String::toLowerCase).collect(Collectors.toList());
        List<String> cb = b.stream().map(String::toLowerCase).collect(Collectors.toList());
        return ca.containsAll(cb);
    }
}
