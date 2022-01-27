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

package com.alibaba.polardbx.executor.corrector;

import com.alibaba.polardbx.executor.backfill.Extractor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionLocation;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;

/**
 * @version 1.0
 */
public class Checker {

    private static final Logger logger = LoggerFactory.getLogger(Checker.class);
    private static final int TASK_CANCELLED = -1;

    private final String schemaName;
    private final String tableName;
    private final String indexName;
    private final TableMeta primaryTableMeta;
    private final TableMeta gsiTableMeta;
    private final long batchSize;
    private volatile RateLimiter rateLimiter;
    private Throttle t;
    private volatile long nowSpeedLimit;
    private final long parallelism;

    /* Templates, built once, used every time. */
    private final SqlSelect.LockMode primaryLock;
    private final SqlSelect.LockMode gsiLock;
    /**
     * Scan on primary table or GSI table.
     * <p>
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_table}
     * WHERE (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE (or no lock)
     * </pre>
     */
    private final PhyTableOperation planSelectWithMaxPrimary;
    private final PhyTableOperation planSelectWithMaxGsi;
    /**
     * Scan on primary table or GSI table.
     * <p>
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?) => DNF
     *   AND (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE (or no lock)
     * </pre>
     */
    private final PhyTableOperation planSelectWithMinAndMaxPrimary;
    private final PhyTableOperation planSelectWithMinAndMaxGsi;
    /**
     * Get rows within pk list.
     * <p>
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_table}
     * WHERE (pk0, ... , pkn) in ((?, ... , ?), ...) -- Dynamic generated.
     * ORDER BY pk0, ... , pkn
     * -- No shared lock on checked table
     * </pre>
     */
    private final SqlSelect planSelectWithInTemplate;
    private final PhyTableOperation planSelectWithIn;
    /**
     * Select max pks from primary table or GSI table.
     * <p>
     * <pre>
     * SELECT IFNULL(MAX(pk0), 0), ... , IFNULL(MAX(pkn), 0)
     * FROM (
     *  SELECT pk0, ... , pkn
     *  FROM {physical_table}
     *  ORDER BY pk0 DESC, ... , pkn DESC LIMIT 1
     * ) T1
     * </pre>
     */
    private final PhyTableOperation planSelectMaxPk;

    private final List<String> indexColumns;
    List<Integer> primaryKeysId;
    private final Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator;

    private final ITransactionManager tm;

    private final CheckerManager manager;

    // For check select PK pruning.
    private final BitSet primaryShardingKey;
    private final List<ColumnMeta> primaryShardingKeyMetas;
    private final List<String> primaryRelShardingKeyNames;
    private final BitSet gsiShardingKey;
    private final List<ColumnMeta> gsiShardingKeyMetas;
    private final List<String> gsiRelShardingKeyNames;
    protected boolean isGetShardResultForReplicationTable = false;
    protected boolean isomorphic = false;

    // Flags.
    private boolean inBackfill = false;
    private long jobId = 0; // Set to jobId if in async ddl task or generate one.

    public Checker(String schemaName, String tableName, String indexName, TableMeta primaryTableMeta,
                   TableMeta gsiTableMeta, long batchSize, long speedMin, long speedLimit, long parallelism,
                   SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                   PhyTableOperation planSelectWithMaxPrimary, PhyTableOperation planSelectWithMaxGsi,
                   PhyTableOperation planSelectWithMinAndMaxPrimary, PhyTableOperation planSelectWithMinAndMaxGsi,
                   SqlSelect planSelectWithInTemplate, PhyTableOperation planSelectWithIn,
                   PhyTableOperation planSelectMaxPk, List<String> indexColumns, List<Integer> primaryKeysId,
                   Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
        this.primaryTableMeta = primaryTableMeta;
        this.gsiTableMeta = gsiTableMeta;
        this.batchSize = batchSize;
        this.rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        SQLRecorderLogger.ddlLogger.warn("checker args: speedMin: " + speedMin + ", speedLimit: " + speedLimit);
        this.t = new Throttle(speedMin, speedLimit, schemaName);
        this.nowSpeedLimit = speedLimit;
        this.parallelism = parallelism;
        this.primaryLock = primaryLock;
        this.gsiLock = gsiLock;
        this.planSelectWithMaxPrimary = planSelectWithMaxPrimary;
        this.planSelectWithMaxGsi = planSelectWithMaxGsi;
        this.planSelectWithMinAndMaxPrimary = planSelectWithMinAndMaxPrimary;
        this.planSelectWithMinAndMaxGsi = planSelectWithMinAndMaxGsi;
        this.planSelectWithInTemplate = planSelectWithInTemplate;
        this.planSelectWithIn = planSelectWithIn;
        this.planSelectMaxPk = planSelectMaxPk;
        this.indexColumns = indexColumns;
        this.primaryKeysId = primaryKeysId;
        this.rowComparator = rowComparator;
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        this.manager = new CheckerManager(schemaName);

        //
        // Generate pruning info.
        //

        List<String> primaryShardingKeyNames = GlobalIndexMeta.getShardingKeys(primaryTableMeta, schemaName);
        primaryShardingKey = new BitSet(primaryShardingKeyNames.size());
        for (String shardingKey : primaryShardingKeyNames) {
            // Use idx of indexColumns.
            for (int i = 0; i < indexColumns.size(); i++) {
                if (shardingKey.equalsIgnoreCase(indexColumns.get(i))) {
                    primaryShardingKey.set(i);
                }
            }
        }
        // Change the order to indexColumns.
        primaryShardingKeyNames = primaryShardingKey.stream().mapToObj(indexColumns::get).collect(Collectors.toList());
        primaryShardingKeyMetas = primaryShardingKeyNames.stream()
            .map(primaryTableMeta::getColumnIgnoreCase)
            .collect(Collectors.toList());
        primaryRelShardingKeyNames = BuildPlanUtils.getRelColumnNames(primaryTableMeta, primaryShardingKeyMetas);

        List<String> gsiShardingKeyNames = GlobalIndexMeta.getShardingKeys(gsiTableMeta, schemaName);
        gsiShardingKey = new BitSet(gsiShardingKeyNames.size());
        for (String shardingKey : gsiShardingKeyNames) {
            // Use idx of indexColumns.
            for (int i = 0; i < indexColumns.size(); i++) {
                if (shardingKey.equalsIgnoreCase(indexColumns.get(i))) {
                    gsiShardingKey.set(i);
                }
            }
        }
        // Change the order to indexColumns.
        gsiShardingKeyNames = gsiShardingKey.stream().mapToObj(indexColumns::get).collect(Collectors.toList());
        gsiShardingKeyMetas = gsiShardingKeyNames.stream()
            .map(gsiTableMeta::getColumnIgnoreCase)
            .collect(Collectors.toList());
        gsiRelShardingKeyNames = BuildPlanUtils.getRelColumnNames(gsiTableMeta, gsiShardingKeyMetas);
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getIndexName() {
        return indexName;
    }

    public SqlSelect.LockMode getPrimaryLock() {
        return primaryLock;
    }

    public SqlSelect.LockMode getGsiLock() {
        return gsiLock;
    }

    public List<String> getIndexColumns() {
        return indexColumns;
    }

    public List<Integer> getPrimaryKeys() {
        return primaryKeysId;
    }

    public Comparator<List<Pair<ParameterContext, byte[]>>> getRowComparator() {
        return rowComparator;
    }

    public CheckerManager getManager() {
        return manager;
    }

    public boolean isInBackfill() {
        return inBackfill;
    }

    public void setInBackfill(boolean inBackfill) {
        this.inBackfill = inBackfill;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public static void validate(String schemaName, String tableName, String indexName,
                                ExecutionContext executionContext) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta indexTableMeta = sm.getTable(indexName);
        if (null == indexTableMeta || !indexTableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI table.");
        }

        if (null == tableName) {
            tableName = indexTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        final TableMeta baseTableMeta = sm.getTable(tableName);

        if (null == baseTableMeta || !baseTableMeta.withGsi() || !indexTableMeta.isGsi()
            || !baseTableMeta.getGsiTableMetaBean().indexMap.containsKey(indexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI relationship.");
        }
    }

    public static Checker create(String schemaName, String tableName, String indexName, long batchSize, long speedMin,
                                 long speedLimit,
                                 long parallelism, SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                                 ExecutionContext checkerEc) {
        // Build select plan
        // Caution: This should get latest schema to check column which newly added.
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta indexTableMeta = sm.getTable(indexName);
        if (null == indexTableMeta || !indexTableMeta.isGsi()) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI table.");
        }

        if (null == tableName) {
            tableName = indexTableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
        }
        final TableMeta baseTableMeta = sm.getTable(tableName);

        if (null == baseTableMeta || !baseTableMeta.withGsi() || !indexTableMeta.isGsi()
            || !baseTableMeta.getGsiTableMetaBean().indexMap.containsKey(indexName)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER, "Incorrect GSI relationship.");
        }

        Extractor.ExtractorInfo info = Extractor.buildExtractorInfo(checkerEc, schemaName, tableName, indexName);
        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, checkerEc);

        final Pair<SqlSelect, PhyTableOperation> selectWithIn = builder
            .buildSelectWithInForChecker(baseTableMeta, info.getTargetTableColumns(), info.getPrimaryKeys(), true);

        final List<DataType> columnTypes = indexTableMeta.getAllColumns()
            .stream()
            .map(ColumnMeta::getDataType)
            .collect(Collectors.toList());
        final Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator = (o1, o2) -> {
            for (int idx : info.getPrimaryKeysId()) {
                int n = ExecUtils
                    .comp(o1.get(idx).getKey().getValue(), o2.get(idx).getKey().getValue(), columnTypes.get(idx), true);
                if (n != 0) {
                    return n;
                }
            }
            return 0;
        };

        return new Checker(schemaName,
            tableName,
            indexName,
            baseTableMeta,
            indexTableMeta,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            primaryLock,
            gsiLock,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                false, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                false, true, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                true, true, gsiLock),
            selectWithIn.getKey(),
            selectWithIn.getValue(),
            builder.buildSelectMaxPkForBackfill(baseTableMeta, info.getPrimaryKeys()),
            info.getTargetTableColumns(),
            info.getPrimaryKeysId(),
            rowComparator);
    }

    private PhyTableOperation buildSelectPlanWithParam(String dbIndex, String phyTable, long batchSize,
                                                       List<ParameterContext> params, boolean withUpperBoundOnly,
                                                       boolean primaryToGsi) {
        final Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Get Plan
        final PhyTableOperation plan;
        if (withUpperBoundOnly) {
            plan = new PhyTableOperation(primaryToGsi ? planSelectWithMaxPrimary : planSelectWithMaxGsi);
        } else {
            plan = new PhyTableOperation(primaryToGsi ? planSelectWithMinAndMaxPrimary : planSelectWithMinAndMaxGsi);
        }

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / (withUpperBoundOnly ? 1 : 2);
        if (!withUpperBoundOnly) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }
        final int base = withUpperBoundOnly ? 0 : pkNumber;
        for (int i = 0; i < pkNumber; ++i) {
            for (int j = 0; j <= i; ++j) {
                planParams.put(nextParamIndex,
                    new ParameterContext(params.get(base + j).getParameterMethod(),
                        new Object[] {nextParamIndex, params.get(base + j).getArgs()[1]}));
                nextParamIndex++;
            }
        }

        // Parameters for limit
        planParams.put(nextParamIndex,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));

        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(planParams);
        return plan;
    }

    private static SqlNode value2node(Object value) {
        if (value instanceof Boolean) {
            return SqlLiteral.createBoolean(DataTypes.BooleanType.convertFrom(value) == 1, SqlParserPos.ZERO);
        } else {
            final String strValue = DataTypes.StringType.convertFrom(value);
            if (value instanceof Number) {
                return SqlLiteral.createExactNumeric(strValue, SqlParserPos.ZERO);
            } else if (value instanceof byte[]) {
                return SqlLiteral.createBinaryString((byte[]) value, SqlParserPos.ZERO);
            } else {
                return SqlLiteral.createCharString(strValue, SqlParserPos.ZERO);
            }
        }
    }

    private PhyTableOperation buildSelectWithIn(String dbIndex, String phyTable,
                                                List<List<Pair<ParameterContext, byte[]>>> rows, boolean toCNF,
                                                SqlSelect.LockMode lock) {
        final Map<Integer, ParameterContext> planParams = new HashMap<>(1);
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        final PhyTableOperation plan = new PhyTableOperation(planSelectWithIn);

        SqlNode condition = null;
        final SqlNodeList inValues = new SqlNodeList(SqlParserPos.ZERO);
        if (toCNF) {
            if (1 == primaryKeysId.size()) {
                // pk=? or pk=? ...
                final int pkIndex = primaryKeysId.get(0);
                final SqlIdentifier pk = new SqlIdentifier(indexColumns.get(pkIndex), SqlParserPos.ZERO);
                for (List<Pair<ParameterContext, byte[]>> row : rows) {
                    final SqlNode conditionItem = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                        new SqlNode[] {pk, value2node(row.get(pkIndex).getKey().getValue())},
                        SqlParserPos.ZERO);
                    if (null == condition) {
                        condition = conditionItem;
                    } else {
                        condition = new SqlBasicCall(SqlStdOperatorTable.OR,
                            new SqlNode[] {condition, conditionItem},
                            SqlParserPos.ZERO);
                    }
                }
            } else {
                // (pk0=? and pk1=?) or ...
                final SqlIdentifier[] pks = primaryKeysId.stream().mapToInt(idx -> idx)
                    .mapToObj(idx -> new SqlIdentifier(indexColumns.get(idx), SqlParserPos.ZERO))
                    .toArray(SqlIdentifier[]::new);
                for (List<Pair<ParameterContext, byte[]>> row : rows) {
                    final SqlNode[] values = primaryKeysId.stream().mapToInt(idx -> idx)
                        .mapToObj(idx -> value2node(row.get(idx).getKey().getValue()))
                        .toArray(SqlNode[]::new);
                    SqlNode block = null;
                    for (int pkId = 0; pkId < pks.length; ++pkId) {
                        final SqlNode item = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                            new SqlNode[] {pks[pkId], values[pkId]},
                            SqlParserPos.ZERO);
                        if (null == block) {
                            block = item;
                        } else {
                            block = new SqlBasicCall(SqlStdOperatorTable.AND,
                                new SqlNode[] {block, item},
                                SqlParserPos.ZERO);
                        }
                    }
                    if (null == condition) {
                        condition = block;
                    } else {
                        condition = new SqlBasicCall(SqlStdOperatorTable.OR,
                            new SqlNode[] {condition, block},
                            SqlParserPos.ZERO);
                    }
                }
            }
        } else {
            // Generate in XXX.
            if (1 == primaryKeysId.size()) {
                final int pkIndex = primaryKeysId.get(0);
                rows.forEach(row -> inValues.add(value2node(row.get(pkIndex).getKey().getValue())));
            } else {
                rows.forEach(row -> {
                    final SqlNode[] rowSet = primaryKeysId.stream().mapToInt(idx -> idx)
                        .mapToObj(i -> row.get(i).getKey().getValue())
                        .map(Checker::value2node)
                        .toArray(SqlNode[]::new);
                    inValues.add(new SqlBasicCall(SqlStdOperatorTable.ROW, rowSet, SqlParserPos.ZERO));
                });
            }
        }

        // Generate template.
        synchronized (planSelectWithInTemplate) {
            planSelectWithInTemplate.setLockMode(lock);
            if (toCNF) {
                planSelectWithInTemplate.setWhere(condition);
            } else {
                ((SqlBasicCall) planSelectWithInTemplate.getWhere()).getOperands()[1] = inValues;
            }
            plan.setSqlTemplate(RelUtils.toNativeSql(planSelectWithInTemplate));
        }

        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(planParams);
        return plan;
    }

    // Reuse the physical table operation and scan another physical table.
    private static PhyTableOperation resetSelectWithIn(String dbIndex, String phyTable, PhyTableOperation plan) {
        final Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(planParams);
        return plan;
    }

    private Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> shardRows(ExecutionContext baseEc,
                                                                                           boolean primaryToGsi,
                                                                                           List<List<Pair<ParameterContext, byte[]>>> rows) {
        final Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> shardResults = new HashMap<>();
        PartitionInfoManager partitionInfoManager =
            OptimizerContext.getContext(schemaName).getRuleManager().getPartitionInfoManager();
        if (primaryToGsi) {
            // Shard with GSI table.
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(indexName);
            //it is new broadcast table
            boolean isNewBroadCastTable = partitionInfoManager.isBroadcastTable(indexName);

            for (List<Pair<ParameterContext, byte[]>> row : rows) {
                Pair<String, String> dbAndTable;
                if (!isNewBroadCastTable) {
                    final List<Object> shardObjects = gsiShardingKey.stream()
                        .mapToObj(idx -> row.get(idx).getKey().getValue())
                        .collect(Collectors.toList());
                    dbAndTable = BuildPlanUtils.shardSingleRow(shardObjects,
                        gsiShardingKeyMetas,
                        gsiRelShardingKeyNames,
                        indexName,
                        schemaName,
                        baseEc,
                        isGetShardResultForReplicationTable,
                        tableMeta);
                } else {
                    PartitionLocation location =
                        partitionInfoManager.getPartitionInfo(indexName).getPartitionBy().getPartitions().get(0)
                            .getLocation();
                    dbAndTable = new Pair<>(location.getGroupKey(), location.getPhyTableName());
                }
                shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                    .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                    .add(row);
            }
        } else {
            // Shard with primary table.
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            boolean isBroadCastTable = partitionInfoManager.isBroadcastTable(tableName);
            for (List<Pair<ParameterContext, byte[]>> row : rows) {
                Pair<String, String> dbAndTable;
                if (!isBroadCastTable) {
                    final List<Object> shardObjects = primaryShardingKey.stream()
                        .mapToObj(idx -> row.get(idx).getKey().getValue())
                        .collect(Collectors.toList());
                    dbAndTable = BuildPlanUtils.shardSingleRow(shardObjects,
                        primaryShardingKeyMetas,
                        primaryRelShardingKeyNames,
                        tableName,
                        schemaName,
                        baseEc,
                        false,
                        tableMeta);
                } else {
                    PartitionLocation location =
                        partitionInfoManager.getPartitionInfo(tableName).getPartitionBy().getPartitions().get(0)
                            .getLocation();
                    dbAndTable = new Pair<>(location.getGroupKey(), location.getPhyTableName());
                }
                shardResults.computeIfAbsent(dbAndTable.getKey(), b -> new HashMap<>())
                    .computeIfAbsent(dbAndTable.getValue(), b -> new ArrayList<>())
                    .add(row);
            }
        }
        return shardResults;
    }

    public boolean checkShard(String dbIndex, String phyTable, ExecutionContext baseEc, boolean isPrimary,
                              List<Pair<ParameterContext, byte[]>> row) {
        if (isMoveTable()) {
            return true;
        }
        if (isPrimary) {
            boolean isBroadCastTable = OptimizerContext.getContext(schemaName).getRuleManager()
                .getPartitionInfoManager()
                .isBroadcastTable(tableName);
            if (isBroadCastTable) {
                return true;
            }
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
            final List<Object> shardObjects = primaryShardingKey.stream()
                .mapToObj(idx -> row.get(idx).getKey().getValue())
                .collect(Collectors.toList());
            final Pair<String, String> dbAndTable = BuildPlanUtils.shardSingleRow(shardObjects,
                primaryShardingKeyMetas,
                primaryRelShardingKeyNames,
                tableName,
                schemaName,
                baseEc,
                false,
                tableMeta);
            return dbAndTable.getKey().equals(dbIndex) && dbAndTable.getValue().equals(phyTable);
        } else {
            boolean isBroadCastTable = OptimizerContext.getContext(schemaName).getRuleManager()
                .getPartitionInfoManager()
                .isBroadcastTable(indexName);
            if (isBroadCastTable) {
                return true;
            }
            TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(indexName);
            final List<Object> shardObjects = gsiShardingKey.stream()
                .mapToObj(idx -> row.get(idx).getKey().getValue())
                .collect(Collectors.toList());
            final Pair<String, String> dbAndTable = BuildPlanUtils.shardSingleRow(shardObjects,
                gsiShardingKeyMetas,
                gsiRelShardingKeyNames,
                indexName,
                schemaName,
                baseEc,
                isGetShardResultForReplicationTable,
                tableMeta);
            return dbAndTable.getKey().equals(dbIndex) && dbAndTable.getValue().equals(phyTable);
        }
    }

    // Recheck rows.
    public boolean recheckRow(String dbIndex, String phyTable, ExecutionContext baseEc, boolean primaryToGsi,
                              List<List<Pair<ParameterContext, byte[]>>> baseRows, SqlSelect.LockMode lock) {
        final List<List<Pair<ParameterContext, byte[]>>> recheckBaseRows = new ArrayList<>();
        final PhyTableOperation recheckBasePlan = buildSelectWithIn(dbIndex, phyTable, baseRows, false, lock);
        return GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.ALLOW_READ_CROSS_DB, baseEc, recheckEc -> {
            final Cursor cursor = ExecutorHelper.executeByCursor(recheckBasePlan, recheckEc, false);
            try {
                Row row;
                while ((row = cursor.next()) != null) {
                    recheckBaseRows.add(row2objects(row));
                }
            } finally {
                cursor.close(new ArrayList<>());
            }

            // Close connection if no lock held.
            if (lock == SqlSelect.LockMode.UNDEF) {
                recheckEc.getTransaction().close();
            }

            // Ignore if empty.
            if (recheckBaseRows.isEmpty()) {
                return true;
            }

            Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> shardedRowGroups =
                getContrastShardRows(dbIndex, phyTable, recheckBaseRows, baseEc, primaryToGsi);

            final List<List<Pair<ParameterContext, byte[]>>> recheckCheckRows = new ArrayList<>();
            shardedRowGroups.forEach((checkDbIndex, v) -> v.forEach((checkPhyTable, shardedRows) -> {
                final PhyTableOperation checkPlan = buildSelectWithIn(checkDbIndex,
                    checkPhyTable,
                    shardedRows,
                    false,
                    SqlSelect.LockMode.UNDEF);
                GsiUtils.wrapWithNoTrx(tm, baseEc, checkEc -> {
                    final Cursor checkCursor = ExecutorHelper.executeByCursor(checkPlan, checkEc, false);
                    try {
                        Row checkRow;
                        while ((checkRow = checkCursor.next()) != null) {
                            recheckCheckRows.add(row2objects(checkRow));
                        }
                    } finally {
                        checkCursor.close(new ArrayList<>());
                    }
                    return null;
                });
            }));

            // Fetch data done, and commit if necessary.
            if (lock != SqlSelect.LockMode.UNDEF) {
                try {
                    recheckEc.getTransaction().commit();
                } catch (Exception e) {
                    logger.error("Close extract statement failed!", e);
                    return false;
                }
            }

            // Sort.
            recheckCheckRows.sort(rowComparator);

            // Compare again, and just result.
            if (recheckBaseRows.size() != recheckCheckRows.size()) {
                return false;
            }
            for (int i = 0; i < recheckBaseRows.size(); ++i) {
                if (!equalAll(recheckBaseRows.get(i), recheckCheckRows.get(i))) {
                    return false;
                }
            }
            return true;
        });
    }

    private List<ParameterContext> getUpperBound(ExecutionContext baseEc, String dbIndex, String phyTable) {
        // Build parameter
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        // Build plan
        final PhyTableOperation plan = new PhyTableOperation(this.planSelectMaxPk);
        plan.setDbIndex(dbIndex);
        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
        plan.setParam(params);

        // Execute query
        return GsiUtils.wrapWithSingleDbTrx(tm, baseEc, (ec) -> {
            final Cursor cursor = ExecutorHelper.executeByCursor(plan, ec, false);

            List<Pair<ParameterContext, byte[]>> rowData = null;
            try {
                Row row;
                while ((row = cursor.next()) != null) {
                    // Fetch first line.
                    rowData = null == rowData ? row2objects(row) : rowData;
                }
            } finally {
                cursor.close(new ArrayList<>());
            }

            if (null == rowData) {
                return null;
            } else {
                return rowData.stream().map(Pair::getKey).collect(Collectors.toList());
            }
        });
    }

    // Check from one physical table to others(GSI or primary table).
    private void foreachPhyTableCheck(String dbIndex, String phyTable, ExecutionContext baseEc, boolean primaryToGsi,
                                      CheckerCallback cb, AtomicIntegerArray progresses, int taskId) {
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Checker start phy for {1}[{2}][{3}]",
            baseEc.getTraceId(),
            dbIndex,
            phyTable,
            primaryToGsi ? "P->G" : "G->P"));

        long totalRowsDealing = 0;

        List<ParameterContext> upperBound = getUpperBound(baseEc, dbIndex, phyTable);
        if (null == upperBound) {
            // Fill progress.
            progresses.set(taskId, 100);

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Checker finish phy for {1}[{2}][{3}][{4}]",
                baseEc.getTraceId(),
                dbIndex,
                phyTable,
                primaryToGsi ? "P->G" : "G->P",
                totalRowsDealing));
            return; // Empty table.
        }

        List<ParameterContext> lowerBound = null;
        do {
            // Dynamic adjust lower bound of rate.
            final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
            if (dynamicRate > 0 && dynamicRate != t.getMaxRate()) {
                t.resetMaxRate(dynamicRate);
                SQLRecorderLogger.ddlLogger.warn("t.resetMaxRate(dynamicRate)" + dynamicRate);
            }

            long start = System.currentTimeMillis();

            PhyTableOperation selectBaseTable = buildSelectPlanWithParam(dbIndex,
                phyTable,
                batchSize,
                null == lowerBound ? upperBound : Stream.concat(lowerBound.stream(), upperBound.stream())
                    .collect(Collectors.toList()),
                null == lowerBound,
                primaryToGsi);

            final AtomicInteger rowsDealing = new AtomicInteger(0);

            // Read base rows.
            lowerBound = GsiUtils.retryOnException(() -> GsiUtils.wrapWithTransaction(tm,
                (primaryToGsi ? primaryLock : gsiLock) != SqlSelect.LockMode.UNDEF ?
                    ITransactionPolicy.ALLOW_READ_CROSS_DB : ITransactionPolicy.NO_TRANSACTION,
                baseEc,
                (selectEc) -> {
                    final Cursor cursor = ExecutorHelper.executeByCursor(selectBaseTable, selectEc, false);

                    // Scan base rows.
                    final List<List<Pair<ParameterContext, byte[]>>> baseRows = new ArrayList<>();
                    try {
                        Row row;
                        while ((row = cursor.next()) != null) {
                            baseRows.add(row2objects(row));
                        }
                    } finally {
                        cursor.close(new ArrayList<>());
                    }

                    // Close connection if no lock held.
                    if (SqlSelect.LockMode.UNDEF == (primaryToGsi ? primaryLock : gsiLock)) {
                        selectEc.getTransaction().close();
                    }

                    if (!baseRows.isEmpty()) {
                        Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> shardedRowGroups =
                            getContrastShardRows(dbIndex, phyTable, baseRows, baseEc, primaryToGsi);

                        // Get check rows with same PKs.
                        final List<List<Pair<ParameterContext, byte[]>>> checkRows = new ArrayList<>();
                        shardedRowGroups.forEach((checkDbIndex, v) -> v.forEach((checkPhyTable, shardedRows) -> {
                            final PhyTableOperation checkPlan = buildSelectWithIn(checkDbIndex,
                                checkPhyTable,
                                shardedRows,
                                false, // Use CNF can't prevent table scan on MySQL, so still use in list.
                                SqlSelect.LockMode.UNDEF);
                            GsiUtils.wrapWithNoTrx(tm, baseEc, checkEc -> {
                                final Cursor checkCursor = ExecutorHelper.executeByCursor(checkPlan, checkEc, false);
                                try {
                                    Row checkRow;
                                    while ((checkRow = checkCursor.next()) != null) {
                                        checkRows.add(row2objects(checkRow));
                                    }
                                } finally {
                                    checkCursor.close(new ArrayList<>());
                                }
                                return null;
                            });
                        }));

                        checkRows.sort(rowComparator);

                        if (!cb.batch(dbIndex, phyTable, selectEc, this, primaryToGsi, baseRows, checkRows)) {
                            // Callback cancel the current batch of checking.
                            throw GeneralUtil.nestedException("Checker retry batch");
                        }

                        (primaryToGsi ? cb.getPrimaryCounter() : cb.getGsiCounter()).getAndAdd(baseRows.size());
                        baseEc.getStats().checkedRows.addAndGet(baseRows.size());

                        // Check DDL is ongoing.
                        if (CrossEngineValidator.isJobInterrupted(baseEc)) {
                            long jobId = baseEc.getDdlJobId();
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                "The job '" + jobId + "' has been cancelled");
                        }

                        // Record number of rows.
                        rowsDealing.set(baseRows.size());
                    }

                    if (baseRows.size() < batchSize) {
                        return null;
                    } else {
                        return primaryKeysId.stream().mapToInt(idx -> idx)
                            .mapToObj(i -> baseRows.get(baseRows.size() - 1).get(i).getKey())
                            .collect(Collectors.toList());
                    }
                }), (e) -> {
                if (e.getMessage().equals("Checker retry batch")) {
                    return true;
                } else if (e.getSQLState() != null && e.getSQLState().equals(SQLSTATE_DEADLOCK)
                    && ER_LOCK_DEADLOCK == e.getErrorCode()) {
                    // Deadlock and retry.
                    return true;
                }
                return false;
            }, (e, retryCount) -> {
                if (retryCount < 3) {
                    if (!e.getMessage().equals("Checker retry batch")) {
                        // Only sleep on no retry operation(dead lock).
                        try {
                            TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
                        } catch (InterruptedException ex) {
                            // Throw it out, because this may caused by user interrupt.
                            throw GeneralUtil.nestedException(ex);
                        }
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CHECKER,
                        "Check GSI max retry times exceeded: " + e.getMessage());
                }
            });

            // Limit the checking speed.
            if (rateLimiter != null && rowsDealing.get() > 0) {
                t.feedback(new Throttle.FeedbackStats(System.currentTimeMillis() - start, start, rowsDealing.get()));
                rateLimiter.setRate(t.getNewRate());
                rateLimiter.acquire(rowsDealing.get());
            }

            // Record rows.
            totalRowsDealing += rowsDealing.get();

            // Calculate the progress.
            if (null == lowerBound) {
                progresses.set(taskId, 100);
            } else {
                try {
                    final Object arg = lowerBound.get(0).getValue();
                    final DataType type = DataTypeUtil.getTypeOfObject(arg);

                    if (DataTypeUtil.isNumberSqlType(type)) {
                        final BigDecimal current = (BigDecimal) DataTypes.DecimalType.convertJavaFrom(arg);
                        final BigDecimal max =
                            (BigDecimal) DataTypes.DecimalType.convertJavaFrom(upperBound.get(0).getValue());

                        final int progress = current.divide(max, 4, RoundingMode.HALF_UP)
                            .multiply(BigDecimal.valueOf(100L))
                            .intValue();
                        progresses.set(taskId, progress);
                    }
                } catch (Exception ignore) {
                }
            }

            // Summarize the progress.
            int total = 0;
            for (int id = 0; id < progresses.length(); ++id) {
                total += progresses.get(id);
            }
            final int progress = total / progresses.length();
            if (inBackfill) {
                manager.updateBackfillProgress(baseEc, progress);
            } else {
                manager.updateProgress(baseEc, progress);
            }
        } while (lowerBound != null);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Checker finish phy for {1}[{2}][{3}][{4}]",
            baseEc.getTraceId(),
            dbIndex,
            phyTable,
            primaryToGsi ? "P->G" : "G->P",
            totalRowsDealing));
    }

    public Exception runTasks(List<FutureTask<Void>> futures, BlockingQueue<Object> blockingQueue) {
        AtomicReference<Exception> excep = new AtomicReference<>(null);
        if (parallelism <= 0) {
            futures.forEach(task -> PriorityWorkQueue.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_CHECK_TASK));
        } else {
            futures.forEach(task -> {
                try {
                    blockingQueue.put(task); // Just put an object to get blocked when full.
                } catch (Exception e) {
                    excep.set(e);
                }
                if (null == excep.get()) {
                    PriorityWorkQueue.getInstance()
                        .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_CHECK_TASK);
                }
            });
        }

        if (excep.get() != null) {
            // Interrupt all.
            futures.forEach(f -> {
                try {
                    f.cancel(true);
                } catch (Throwable ignore) {
                }
            });
        }

        for (FutureTask<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
                if (null == excep.get()) {
                    excep.set(e);
                }
            }
        }

        return excep.get();
    }

    public void check(ExecutionContext baseEc, CheckerCallback cb) {
        // Force master first and following will copy this EC.
        baseEc.getExtraCmds().put(ConnectionProperties.MASTER, true);

        final Map<String, Set<String>> primaryPhyTables = getSourcePhysicalTables();
        final Map<String, Set<String>> gsiPhyTables = getTargetPhysicalTables();

        // Count tasks for calculate progress.
        int primaryTaskCount = 0;
        for (Set<String> phyTables : primaryPhyTables.values()) {
            primaryTaskCount += phyTables.size();
        }
        int gsiTaskCount = 0;
        for (Set<String> phyTables : gsiPhyTables.values()) {
            gsiTaskCount += phyTables.size();
        }

        // Progress array.
        final AtomicIntegerArray progresses = new AtomicIntegerArray(primaryTaskCount + gsiTaskCount);
        final List<FutureTask<Void>> futuresForward = new ArrayList<>(primaryTaskCount),
            futuresBackward = new ArrayList<>(gsiTaskCount);

        final AtomicInteger startedTaskCount = new AtomicInteger(0);
        final CountDownLatch taskFinishBarrier = new CountDownLatch(primaryTaskCount + gsiTaskCount);

        final AtomicInteger taskId = new AtomicInteger(0);
        // Use a bounded blocking queue to control the parallelism.
        final BlockingQueue<Object> blockingQueue = parallelism <= 0 ? null : new ArrayBlockingQueue<>(
            (int) parallelism);
        primaryPhyTables.forEach((dbIndex, v) -> v.forEach(phyTable -> futuresForward.add(new FutureTask<>(() -> {
            int ret = startedTaskCount.getAndUpdate(origin -> (origin == TASK_CANCELLED) ? origin : (origin + 1));
            if (ret == TASK_CANCELLED) {
                return;
            }
            try {
                foreachPhyTableCheck(dbIndex, phyTable, baseEc, true, cb, progresses, taskId.getAndIncrement());
            } finally {
                taskFinishBarrier.countDown();
                // Poll in finally to prevent dead lock on putting blockingQueue.
                if (blockingQueue != null) {
                    blockingQueue.poll(); // Parallelism control notify.
                }
            }
        }, null))));
        gsiPhyTables.forEach((dbIndex, v) -> v.forEach(phyTable -> futuresBackward.add(new FutureTask<>(() -> {
            try {
                foreachPhyTableCheck(dbIndex, phyTable, baseEc, false, cb, progresses, taskId.getAndIncrement());
            } finally {
                taskFinishBarrier.countDown();
                // Poll in finally to prevent dead lock on putting blockingQueue.
                if (blockingQueue != null) {
                    blockingQueue.poll(); // Parallelism control notify.
                }
            }
        }, null))));

        // Shuffle and queued to run with worker separately.
        Collections.shuffle(futuresForward);
        Collections.shuffle(futuresBackward);

        cb.start(baseEc, this);

        Exception exception;
        if (SqlSelect.LockMode.UNDEF == primaryLock && SqlSelect.LockMode.UNDEF == gsiLock) {
            // No lock so run with full parallel.
            exception = runTasks(
                Stream.concat(futuresForward.stream(), futuresBackward.stream()).collect(Collectors.toList()),
                blockingQueue);
        } else {
            exception = runTasks(futuresForward, blockingQueue);
            if (null == exception) {
                exception = runTasks(futuresBackward, blockingQueue);
            }
        }

        // To ensure that all tasks have finished at this moment, otherwise we may leak resources in execution context,
        // such as memory pool.
        try {
            int notStartedTaskCount = (gsiTaskCount + primaryTaskCount)
                - startedTaskCount.getAndUpdate(origin -> TASK_CANCELLED);
            for (int i = 0; i < notStartedTaskCount; i++) {
                taskFinishBarrier.countDown();
            }
            taskFinishBarrier.await();
        } catch (Throwable t) {
            logger.error("Failed to waiting for checker task finish.", t);
        }
        cb.finish(baseEc, this);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "[{0}] Checker job: {1}[{2}][{3}] finish.",
            baseEc.getTraceId(), schemaName, tableName, indexName));

        t.stop();

        // Throw if have exceptions.
        if (exception != null) {
            throw GeneralUtil.nestedException(exception);
        }
    }

    //
    // Row decoder and comparator.
    //

    // Convert data to ParameterContext(for sort and correction(insert)) and raw
    // bytes(for compare).
    private static List<Pair<ParameterContext, byte[]>> row2objects(Row rowSet) {
        final List<ColumnMeta> columns = rowSet.getParentCursorMeta().getColumns();
        final List<Pair<ParameterContext, byte[]>> result = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            result.add(new Pair<>(Transformer.buildColumnParam(rowSet, i), rowSet.getBytes(i)));
        }
        return result;
    }

    private boolean equalPk(List<Pair<ParameterContext, byte[]>> left, List<Pair<ParameterContext, byte[]>> right) {
        return primaryKeysId.stream().mapToInt(idx -> idx)
            .mapToObj(i -> Arrays.equals(left.get(i).getValue(), right.get(i).getValue()))
            .allMatch(res -> res);
    }

    private boolean equalAll(List<Pair<ParameterContext, byte[]>> left, List<Pair<ParameterContext, byte[]>> right) {
        if (left.size() == right.size()) {
            for (int i = 0; i < left.size(); ++i) {
                if (!Arrays.equals(left.get(i).getValue(), right.get(i).getValue())) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    // Compare the result.
    public boolean compareRows(List<List<Pair<ParameterContext, byte[]>>> baseRows,
                               List<List<Pair<ParameterContext, byte[]>>> checkRows,
                               BiFunction<List<Pair<ParameterContext, byte[]>>, List<Pair<ParameterContext, byte[]>>, Boolean> cb) {
        // Compare.
        int primaryPos = 0, indexPos = 0;
        while (primaryPos < baseRows.size() && indexPos < checkRows.size()) {
            if (equalAll(baseRows.get(primaryPos), checkRows.get(indexPos))) {
                // All equal and check next.
                ++primaryPos;
                ++indexPos;
            } else if (equalPk(baseRows.get(primaryPos), checkRows.get(indexPos))) {
                // Same PK but not same others.
                if (!cb.apply(baseRows.get(primaryPos), checkRows.get(indexPos))) {
                    return false;
                }
                ++primaryPos;
                ++indexPos;
            } else {
                final int cmp = rowComparator.compare(baseRows.get(primaryPos), checkRows.get(indexPos));
                if (cmp > 0) {
                    // Orphan index.
                    if (!cb.apply(null, checkRows.get(indexPos))) {
                        return false;
                    }
                    ++indexPos;
                } else if (cmp < 0) {
                    // Missing index.
                    if (!cb.apply(baseRows.get(primaryPos), null)) {
                        return false;
                    }
                    ++primaryPos;
                }
            }
        }

        // Consume extra data.
        while (primaryPos < baseRows.size()) {
            if (!cb.apply(baseRows.get(primaryPos), null)) {
                return false;
            }
            ++primaryPos;
        }
        while (indexPos < checkRows.size()) {
            if (!cb.apply(null, checkRows.get(indexPos))) {
                return false;
            }
            ++indexPos;
        }

        return true;
    }

    protected Map<String, Set<String>> getSourcePhysicalTables() {
        return GsiUtils.getPhyTables(schemaName, tableName);
    }

    protected Map<String, Set<String>> getTargetPhysicalTables() {
        return GsiUtils.getPhyTables(schemaName, indexName);
    }

    public boolean isMoveTable() {
        return false;
    }

    public String getTargetGroup(String baseDbIndex, boolean primaryToGsi) {
        throw new UnsupportedOperationException();
    }

    public Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> getContrastShardRows(String baseDbIndex,
                                                                                                     String basePhyTable,
                                                                                                     List<List<Pair<ParameterContext, byte[]>>> baseRows,
                                                                                                     ExecutionContext baseEc,
                                                                                                     boolean primaryToGsi) {
        Map<String, Map<String, List<List<Pair<ParameterContext, byte[]>>>>> shardedRowGroups;
        if (isMoveTable()) {
            String targetDbIndex = getTargetGroup(baseDbIndex, primaryToGsi);
            shardedRowGroups = new HashMap<>();
            Map<String, List<List<Pair<ParameterContext, byte[]>>>> phyTableValues = new HashMap<>();
            phyTableValues.put(basePhyTable, baseRows);
            shardedRowGroups.put(targetDbIndex, phyTableValues);
        } else {
            shardedRowGroups = shardRows(
                baseEc,
                primaryToGsi,
                baseRows);
        }
        return shardedRowGroups;
    }
}
