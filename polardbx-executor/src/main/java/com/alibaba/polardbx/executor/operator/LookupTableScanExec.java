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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.operator.lookup.LookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.lookup.ShardingLookupConditionBuilder;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTE_ON_MYSQL;

public class LookupTableScanExec extends TableScanExec implements LookupTableExec {

    private List<Split> reservedSplits;

    private final boolean shardEnabled;

    /**
     * 开启裁剪后 每个分片发送查询in值的上限
     */
    private final int inValueCountLimit;

    private final boolean mppMode;

    private final LookupPredicate predicate;

    private final List<EquiJoinKey> allJoinKeys; // including null-safe equal columns (`<=>`)

    /**
     * for building {@code SqlNodeList}
     */
    MemoryAllocatorCtx conditionMemoryAllocator;

    private long allocatedMem = 0;

    public LookupTableScanExec(LogicalView logicalView, ExecutionContext context, TableScanClient scanClient,
                               boolean shardEnabled, SpillerFactory spillerFactory,
                               LookupPredicate predicate, List<EquiJoinKey> allJoinKeys, List<DataType> dataTypeList) {
        super(logicalView, context, scanClient, Long.MAX_VALUE, spillerFactory, dataTypeList);
        this.shardEnabled = shardEnabled;
        this.inValueCountLimit = context.getParamManager().getInt(ConnectionParams.LOOKUP_IN_VALUE_LIMIT);
        this.mppMode = ExecUtils.isMppMode(context);
        this.predicate = predicate;
        this.allJoinKeys = allJoinKeys;
    }

    @Override
    public void updateLookupPredicate(Chunk chunk) {
        if (!scanClient.noMoreSplit()) {
            throw new TddlRuntimeException(ERR_EXECUTE_ON_MYSQL, "input split not ready");
        }

        if (reservedSplits == null) {
            reservedSplits = new ArrayList<>();
            if (mppMode) {
                //mpp模式下 对split进行乱序处理，分担mysql压力
                Collections.shuffle(scanClient.getSplitList());
            }
            reservedSplits.addAll(scanClient.getSplitList());
        }

        scanClient.getSplitList().clear();
        if (shardEnabled) {
            updateShardedWhereSql(chunk);
        } else {
            // 不分片 把值全部下推
            updateNoShardedWhereSql(chunk);
        }
    }

    @Override
    public void setMemoryAllocator(MemoryAllocatorCtx memoryAllocator) {
        this.conditionMemoryAllocator = memoryAllocator;
    }

    @Override
    public void releaseConditionMemory() {
        conditionMemoryAllocator.releaseReservedMemory(allocatedMem, true);
        allocatedMem = 0;
    }

    @Override
    public boolean shardEnabled() {
        return shardEnabled;
    }

    /**
     * 不分片，把值全部下推
     */
    private void updateNoShardedWhereSql(Chunk chunk) {
        LookupConditionBuilder builder = new LookupConditionBuilder(allJoinKeys, predicate, logicalView, this.context);
        SqlNode lookupCondition = builder.buildCondition(chunk);
        long reserveSize = estimateConditionSize(lookupCondition, reservedSplits.size());
        reserveMemory(reserveSize);
        for (Split split : reservedSplits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            DynamicJdbcSplit dynamicSplit = new DynamicJdbcSplit(jdbcSplit, lookupCondition);
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
    }

    /**
     * 当判断可以将value按分片裁剪后
     * 为每一个 DynamicJdbcSplit 分配对应的where条件
     */
    private void updateShardedWhereSql(Chunk chunk) {
        // 获取到每个sqlNode对应的分片
        ShardingLookupConditionBuilder builder =
            new LookupConditionBuilder(allJoinKeys, predicate, logicalView, this.context).createSharding();
        Map<String, Map<String, SqlNode>> targetDBSqlNodeMap = builder.buildShardedCondition(chunk, context);
        boolean isAntiJoin = predicate.getOperator() == SqlStdOperatorTable.NOT_IN;
        for (Split split : reservedSplits) {
            updateShardedWhereSql(split, targetDBSqlNodeMap, isAntiJoin);
        }
    }

    private void updateShardedWhereSql(Split split,
                                       Map<String, Map<String, SqlNode>> targetDBSqlNodeMap,
                                       boolean isAntiJoin) {
        JdbcSplit jdbcSplit = (JdbcSplit) (split.getConnectorSplit());

        List<List<String>> shardedTableNameList = jdbcSplit.getTableNames();
        String dbIndex = jdbcSplit.getDbIndex();
        Map<String, SqlNode> targetDbSqlNode = targetDBSqlNodeMap.get(dbIndex);
        if (targetDbSqlNode == null) {
            // 分库不存在的情况就不考虑
            return;
        }

        // 由于BKA plan在同一个split中使用了UNION
        // 需要记录下向每个分表发送的sql
        final int numTable = shardedTableNameList.size();
        List<SqlNode>[] remainConditions = new List[numTable];

        List<SqlNode> conditions = new ArrayList<>(numTable);
        boolean valid = false;
        long reserveSize = 0;
        for (int tableIndex = 0; tableIndex < numTable; tableIndex++) {
            String tableName = shardedTableNameList.get(tableIndex).get(0);
            SqlNode shardedCondition = targetDbSqlNode.get(tableName);
            reserveSize += estimateConditionSize(shardedCondition, 1);
            remainConditions[tableIndex] = new ArrayList<>();

            if (shardedCondition != null) {
                if (!isAntiJoin) {
                    // Divide into small batches, except for Anti-Join
                    shardedCondition = divideIntoBatches(shardedCondition, remainConditions[tableIndex]);
                }
                conditions.add(shardedCondition);
                valid = true;
            } else {
                // nothing to lookup for this table
                conditions.add(null);
            }
        }

        reserveMemory(reserveSize);
        if (valid) {
            DynamicJdbcSplit dynamicSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }

        // 如果当前batch不足以查完所有的condition，则需要用更多的batch
        handleRemainConditions(split, remainConditions);
    }

    private SqlNode divideIntoBatches(SqlNode inputCondition, List<SqlNode> remainConditions) {
        if (inputCondition == null || inputCondition instanceof SqlLiteral) {
            return inputCondition;
        }

        assert inputCondition instanceof SqlCall && ((SqlCall) inputCondition).getOperator() == SqlStdOperatorTable.IN;
        final SqlNode key = ((SqlCall) inputCondition).operand(0);
        final SqlNodeList sqlNodeList = ((SqlCall) inputCondition).operand(1);
        final List<SqlNode> values = sqlNodeList.getList();

        // 对sqlNode的operand进行裁剪 防止where in的值过多
        if (values.size() <= inValueCountLimit) {
            return inputCondition;
        } else {
            // 分批次发送
            int batchCount = MathUtils.ceilDiv(values.size(), inValueCountLimit);
            SqlNode condition = createPrunedInCondition(key, values, 0, inValueCountLimit);
            // 开始添加剩余的in条件
            for (int j = 1; j < batchCount - 1 /*不包括头尾*/; j++) {
                remainConditions.add(createPrunedInCondition(key, values,
                    inValueCountLimit * j, inValueCountLimit * (j + 1)));
            }
            // 添加尾部
            remainConditions.add(createPrunedInCondition(key, values,
                inValueCountLimit * (batchCount - 1), values.size()));
            return condition;
        }
    }

    private void handleRemainConditions(Split split, List<SqlNode>[] remainConditions) {
        JdbcSplit jdbcSplit = (JdbcSplit) (split.getConnectorSplit());

        final int numTable = remainConditions.length;
        int maxRemainBatchCount = Arrays.stream(remainConditions).mapToInt(List::size).max().getAsInt();

        // 每多一批剩余的in value 加一条Split
        for (int batchIndex = 0; batchIndex < maxRemainBatchCount; batchIndex++) {
            List<SqlNode> conditions = new ArrayList<>(numTable);
            for (List<SqlNode> remain : remainConditions) {
                SqlNode condition = remain.size() > batchIndex ? remain.get(batchIndex) : null;
                conditions.add(condition);
            }
            DynamicJdbcSplit dynamicSplit = new DynamicJdbcSplit(jdbcSplit, conditions);
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
    }

    private static SqlNode createPrunedInCondition(SqlNode key, List<SqlNode> values, int fromIndex, int toIndex) {
        SqlNodeList prunedValues = new SqlNodeList(values.subList(fromIndex, toIndex), SqlParserPos.ZERO);
        SqlBasicCall sqlBasicCall =
            new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {key, prunedValues}, SqlParserPos.ZERO);
        return sqlBasicCall;
    }

    /**
     * 当in value不裁剪全下推时
     * SqlNode是复用的, 但最后生成hintSql是独立的
     */
    private long estimateConditionSize(SqlNode condition, int splitCount) {
        if (condition == null || condition.getKind() != SqlKind.IN) {
            return 0;
        }
        SqlNodeList sqlNodeList = ((SqlCall) condition).operand(1);
        long conditionSize = sqlNodeList.estimateSize();
        long hintSqlSize = conditionSize * splitCount;
        return conditionSize + hintSqlSize;
    }

    private void reserveMemory(long reservedSize) {
        if (reservedSize != 0) {
            conditionMemoryAllocator.allocateReservedMemory(reservedSize);
            allocatedMem += reservedSize;
        }
    }

    @Override
    public synchronized boolean resume() {
        this.isFinish = false;
        if (consumeResultSet != null) {
            consumeResultSet.close(true);
            consumeResultSet = null;
        }
        scanClient.reset();
        try {
            scanClient.executePrefetchThread(false);
        } catch (Throwable e) {
            TddlRuntimeException exception =
                new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e, e.getMessage());
            this.isFinish = true;
            scanClient.setException(exception);
            scanClient.throwIfFailed();
        }
        return true;
    }

    @Override
    public boolean shouldSuspend() {
        return isFinish;
    }

    @Override
    public void doSuspend() {
        if (consumeResultSet != null) {
            consumeResultSet.close(true);
            consumeResultSet = null;
        }
        scanClient.cancelAllThreads();
    }

    @Override
    synchronized void doClose() {
        super.doClose();
        releaseConditionMemory();
        this.conditionMemoryAllocator = null;
        if (reservedSplits != null) {
            reservedSplits.clear();
        }
    }

    static class DynamicJdbcSplit extends JdbcSplit {

        /**
         * 对应同个split下的多张分表，如果某一项为 null 则表示不需要该分表
         */
        private List<SqlNode> lookupConditions;

        public DynamicJdbcSplit(JdbcSplit jdbcSplit, SqlNode lookupCondition) {
            this(jdbcSplit, Collections.nCopies(jdbcSplit.getTableNames().size(), lookupCondition));
        }

        public DynamicJdbcSplit(JdbcSplit jdbcSplit, List<SqlNode> lookupConditions) {
            super(jdbcSplit);
            Preconditions.checkArgument(jdbcSplit.getTableNames().size() == lookupConditions.size());
            this.lookupConditions = lookupConditions;
        }

        @Override
        public String getHintSql(boolean ignore) {
            if (ignore) {
                int num = 0;
                for (SqlNode condition : lookupConditions) {
                    // Build physical query ignoring the FALSE splits
                    if (condition != null) {
                        num++;
                    }
                }
                String query = PhyTableScanBuilder.buildPhysicalQuery(num, sqlTemplate, orderBy, hint, -1);
                return query;
            }
            if (hintSql == null) {
                int num = 0;
                for (SqlNode condition : lookupConditions) {
                    // Build physical query ignoring the FALSE splits
                    if (condition != null) {
                        num++;
                    }
                }
                String query = PhyTableScanBuilder.buildPhysicalQuery(num, sqlTemplate, orderBy, hint, -1);
                for (SqlNode condition : lookupConditions) {
                    if (condition != null) {
                        query = StringUtils.replace(query, "'bka_magic' = 'bka_magic'",
                            RelUtils.toNativeSql(condition), 1);
                    }
                }
                hintSql = query;
            }
            return hintSql;
        }

        @Override
        public List<ParameterContext> getFlattedParams() {
            if (flattenParams == null) {
                List<List<ParameterContext>> params = getParams();
                flattenParams = new ArrayList<>(params.size() > 0 ? params.get(0).size() : 0);
                for (int i = 0; i < lookupConditions.size(); i++) {
                    final SqlNode cond = lookupConditions.get(i);
                    // Build physical parameters ignoring the FALSE splits
                    if (cond != null) {
                        flattenParams.addAll(params.get(i));
                    }
                }
            }
            return flattenParams;
        }

        @Override
        public void reset() {
            this.hintSql = null;
            this.lookupConditions = null;
        }
    }
}
