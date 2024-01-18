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

package com.alibaba.polardbx.executor.fastchecker;

import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.ddl.workqueue.FastCheckerThreadPool;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.ddl.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static java.lang.Math.max;

public class FastChecker extends PhyOperationBuilderCommon {
    private static final Logger logger = LoggerFactory.getLogger(FastChecker.class);

    private final String srcSchemaName;

    private final String dstSchemaName;
    private final String srcLogicalTableName;
    private final String dstLogicalTableName;
    private final Map<String, String> sourceTargetGroup;
    private Map<String, Set<String>> srcPhyDbAndTables;
    private Map<String, Set<String>> dstPhyDbAndTables;
    private final List<String> srcColumns;
    private final List<String> dstColumns;
    private final List<String> srcPks;
    private final List<String> dstPks;

    private final ITransactionManager tm;

    private final long parallelism;
    private final int lockTimeOut;

    private final PhyTableOperation planSelectHashCheckSrc;
    private final PhyTableOperation planSelectHashCheckWithUpperBoundSrc;
    private final PhyTableOperation planSelectHashCheckWithLowerBoundSrc;
    private final PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc;

    private final PhyTableOperation planSelectHashCheckDst;
    private final PhyTableOperation planSelectHashCheckWithUpperBoundDst;
    private final PhyTableOperation planSelectHashCheckWithLowerBoundDst;
    private final PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst;
    private final PhyTableOperation planIdleSelectSrc;
    private final PhyTableOperation planIdleSelectDst;

    private final PhyTableOperation planSelectSampleSrc;
    private final PhyTableOperation planSelectSampleDst;

    enum ParallelPolicy {
        /**
         * parallel by group, one group only allows single task at the same time.
         */
        PhyGroupParallel,

        /**
         * parallel by tables
         */
        PhyTableParallel
    }

    /**
     * srcColumns and dstColumns must have the same order,
     * otherwise the check result may be wrong.
     */

    /**
     * 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序！！！
     */
    public FastChecker(String srcSchemaName, String dstSchemaName, String srcLogicalTableName,
                       String dstLogicalTableName, Map<String, String> sourceTargetGroup,
                       Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                       List<String> srcColumns, List<String> dstColumns, List<String> srcPks, List<String> dstPks,
                       long parallelism, int lockTimeOut, PhyTableOperation planSelectHashCheckSrc,
                       PhyTableOperation planSelectHashCheckWithUpperBoundSrc,
                       PhyTableOperation planSelectHashCheckWithLowerBoundSrc,
                       PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc,
                       PhyTableOperation planSelectHashCheckDst, PhyTableOperation planSelectHashCheckWithUpperBoundDst,
                       PhyTableOperation planSelectHashCheckWithLowerBoundDst,
                       PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst, PhyTableOperation planIdleSelectSrc,
                       PhyTableOperation planIdleSelectDst, PhyTableOperation planSelectSampleSrc,
                       PhyTableOperation planSelectSampleDst) {
        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.srcLogicalTableName = srcLogicalTableName;
        this.dstLogicalTableName = dstLogicalTableName;
        this.sourceTargetGroup = sourceTargetGroup;
        this.srcPhyDbAndTables = srcPhyDbAndTables;
        this.dstPhyDbAndTables = dstPhyDbAndTables;
        this.srcColumns = srcColumns;
        this.dstColumns = dstColumns;
        this.srcPks = srcPks;
        this.dstPks = dstPks;

        this.planSelectHashCheckSrc = planSelectHashCheckSrc;
        this.planSelectHashCheckWithUpperBoundSrc = planSelectHashCheckWithUpperBoundSrc;
        this.planSelectHashCheckWithLowerBoundSrc = planSelectHashCheckWithLowerBoundSrc;
        this.planSelectHashCheckWithLowerUpperBoundSrc = planSelectHashCheckWithLowerUpperBoundSrc;
        this.planSelectHashCheckDst = planSelectHashCheckDst;
        this.planSelectHashCheckWithUpperBoundDst = planSelectHashCheckWithUpperBoundDst;
        this.planSelectHashCheckWithLowerBoundDst = planSelectHashCheckWithLowerBoundDst;
        this.planSelectHashCheckWithLowerUpperBoundDst = planSelectHashCheckWithLowerUpperBoundDst;

        this.planIdleSelectSrc = planIdleSelectSrc;
        this.planIdleSelectDst = planIdleSelectDst;
        this.planSelectSampleSrc = planSelectSampleSrc;
        this.planSelectSampleDst = planSelectSampleDst;

        this.parallelism = parallelism;
        this.lockTimeOut = lockTimeOut;
        this.tm = ExecutorContext.getContext(srcSchemaName).getTransactionManager();
    }

    public static boolean isSupported(String schema) {
        return ExecutorContext.getContext(schema).getStorageInfoManager().supportFastChecker();
    }

    public static FastChecker create(String schemaName, String tableName, Map<String, String> sourceTargetGroup,
                                     Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables, long parallelism,
                                     ExecutionContext ec) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);

        if (null == tableMeta) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, "Incorrect SCALEOUT relationship.");
        }

        final List<String> allColumns =
            tableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        final List<String> allColumnsDst = new ArrayList<>(allColumns);
        final List<String> srcPks = getorderedPrimaryKeys(tableMeta, ec);
        final List<String> dstPks = new ArrayList<>(srcPks);

        if (parallelism <= 0) {
            parallelism = Math.max(FastCheckerThreadPool.getInstance().getCorePoolSize() / 2, 1);
        }

        final int lockTimeOut = ec.getParamManager().getInt(ConnectionParams.FASTCHECKER_LOCK_TIMEOUT);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new FastChecker(schemaName, schemaName, tableName, tableName, sourceTargetGroup, srcPhyDbAndTables,
            dstPhyDbAndTables, allColumns, allColumnsDst, srcPks, dstPks, parallelism, lockTimeOut,
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, srcPks, false, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, srcPks, false, true),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, srcPks, true, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, srcPks, true, true),

            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, dstPks, false, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, dstPks, false, true),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, dstPks, true, false),
            builder.buildSelectHashCheckForChecker(tableMeta, allColumns, dstPks, true, true),

            builder.buildIdleSelectForChecker(tableMeta, allColumns),
            builder.buildIdleSelectForChecker(tableMeta, allColumnsDst),

            builder.buildSqlSelectForSample(tableMeta, srcPks, srcPks, false, false),
            builder.buildSqlSelectForSample(tableMeta, dstPks, dstPks, false, false));
    }

    private long getTableRowsCount(final String schema, final String dbIndex, final String phyTable) {
        String dbIndexWithoutGroup = GroupInfoUtil.buildPhysicalDbNameFromGroupName(dbIndex);
        List<List<Object>> phyDb = StatsUtils.queryGroupByPhyDb(schema, dbIndexWithoutGroup, "select database();");
        if (GeneralUtil.isEmpty(phyDb) || GeneralUtil.isEmpty(phyDb.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("group %s can not find physical db", dbIndex));
        }

        String phyDbName = String.valueOf(phyDb.get(0).get(0));
        String rowsCountSQL = StatsUtils.genTableRowsCountSQL(phyDbName, phyTable);
        List<List<Object>> result = StatsUtils.queryGroupByPhyDb(schema, dbIndexWithoutGroup, rowsCountSQL);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("db %s can not find table %s", phyDbName, phyTable));
        }

        return Long.parseLong(String.valueOf(result.get(0).get(0)));
    }

    private long getTableAvgRowSize(final String schema, final String dbIndex, final String phyTable) {
        String dbIndexWithoutGroup = GroupInfoUtil.buildPhysicalDbNameFromGroupName(dbIndex);
        List<List<Object>> phyDb = StatsUtils.queryGroupByPhyDb(schema, dbIndexWithoutGroup, "select database();");
        if (GeneralUtil.isEmpty(phyDb) || GeneralUtil.isEmpty(phyDb.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("group %s can not find physical db", dbIndex));
        }

        String phyDbName = String.valueOf(phyDb.get(0).get(0));
        String avgTableRowLengthSQL = StatsUtils.genAvgTableRowLengthSQL(phyDbName, phyTable);
        List<List<Object>> result = StatsUtils.queryGroupByPhyDb(schema, dbIndexWithoutGroup, avgTableRowLengthSQL);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("db %s can not find table %s", phyDbName, phyTable));
        }

        return Long.parseLong(String.valueOf(result.get(0).get(0)));
    }

    private PhyTableOperation buildSamplePlanWithParam(String dbIndex, String phyTable, List<ParameterContext> params,
                                                       float calSamplePercentage, boolean isSrcSchema) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));
        PhyTableOperation phyTableOperation = isSrcSchema ? this.planSelectSampleSrc : this.planSelectSampleDst;
        SqlSelect sqlSelect = (SqlSelect) phyTableOperation.getNativeSqlNode().clone();
        OptimizerHint optimizerHint = new OptimizerHint();
        optimizerHint.addHint("+sample_percentage(" + calSamplePercentage + ")");
        sqlSelect.setOptimizerHint(optimizerHint);

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setBytesSql(RelUtils.toNativeBytesSql(sqlSelect));
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);
        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(phyTableOperation, buildParams);
    }

    /**
     * where 条件已经是DNF析取范式形式
     * 因此相应的填充参数的方法为 将(pk1, pk2, pk3) < (?, ?, ?) 转换为DNF析取范式形式，再填充
     */
    private PhyTableOperation buildHashcheckPlanWithDnfParam(String dbIndex, String phyTable,
                                                             List<ParameterContext> params,
                                                             PhyTableOperation planTemplate,
                                                             boolean withLowerBound,
                                                             boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        PhyTableOpBuildParams dyParams = new PhyTableOpBuildParams();
        dyParams.setGroupName(dbIndex);
        dyParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));

        int beginParamIndex = 2;
        // Parameters for where(DNF)
        final int pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(beginParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {beginParamIndex, params.get(j).getArgs()[1]}));
                    beginParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(beginParamIndex,
                        new ParameterContext(params.get(base + j).getParameterMethod(),
                            new Object[] {beginParamIndex, params.get(base + j).getArgs()[1]}));
                    beginParamIndex++;
                }
            }
        }

        dyParams.setDynamicParams(planParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(planTemplate, dyParams);
    }

    //for large table, we split table into batch
    private List<Map<Integer, ParameterContext>> splitPhyTableIntoBatch(final ExecutionContext baseEc,
                                                                        final String phyDbName, final String phyTable,
                                                                        final long tableRowsCount,
                                                                        final long batchSize,
                                                                        final boolean isSrcSchema) {
        boolean enableInnodbBtreeSampling = OptimizerContext.getContext(srcSchemaName).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);

        List<Map<Integer, ParameterContext>> batchBoundList = new ArrayList<>();

        if (!enableInnodbBtreeSampling) {
            return batchBoundList;
        }

        final long maxSampleSize = baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_MAX_SAMPLE_SIZE);
        final float maxSamplePercentage =
            baseEc.getParamManager().getFloat(ConnectionParams.FASTCHECKER_MAX_SAMPLE_PERCENTAGE);

        if (tableRowsCount <= batchSize) {
            return batchBoundList;
        }

        final long batchNum = tableRowsCount / batchSize;

        float calSamplePercentage = maxSampleSize * 1.0f / tableRowsCount * 100;
        if (calSamplePercentage <= 0 || calSamplePercentage > maxSamplePercentage) {
            calSamplePercentage = maxSamplePercentage;
        }

        PhyTableOperation plan =
            buildSamplePlanWithParam(phyDbName, phyTable, new ArrayList<>(), calSamplePercentage, isSrcSchema);
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "[{0}] FastChecker {1}[{2}][{3}], begin to sample, phy table rows {4}, "
                + "actual sample rate {5}%, phySqlInfo: {6}, param: {7}",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
            tableRowsCount, calSamplePercentage, plan.getBytesSql(), plan.getParam()));

        // execute query
        final List<Map<Integer, ParameterContext>> sampleResult = GsiUtils.wrapWithSingleDbTrx(tm, baseEc, (ec) -> {
            final Cursor cursor = ExecutorHelper.execute(plan, ec);
            try {
                return Transformer.convertUpperBoundWithDefault(cursor, (columnMeta, i) -> {
                    // Generate default parameter context for upper bound of empty source table
                    ParameterMethod defaultMethod = ParameterMethod.setString;
                    Object defaultValue = "0";

                    final DataType columnType = columnMeta.getDataType();
                    if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.DateType, DataTypes.TimestampType,
                        DataTypes.DatetimeType, DataTypes.TimeType, DataTypes.YearType)) {
                        // For time data type, use number zero as upper bound
                        defaultMethod = ParameterMethod.setLong;
                        defaultValue = 0L;
                    }

                    return new ParameterContext(defaultMethod, new Object[] {i, defaultValue});
                });
            } finally {
                cursor.close(new ArrayList<>());
            }
        });

        long step = sampleResult.size() / batchNum;
        if (step <= 0) {
            return batchBoundList;
        }
        for (int i = 1; i < batchNum; i++) {
            long boundIndex = i * step;
            if (boundIndex < sampleResult.size()) {
                batchBoundList.add(sampleResult.get((int) boundIndex));
            }
        }

        return batchBoundList;
    }

    private Long getPhyTableDegistByBatch(String phyDbName, String phyTable, ExecutionContext baseEc,
                                          boolean isSrcTableTask, List<Map<Integer, ParameterContext>> batchBoundList,
                                          final long tableRowsCount) {
        if (batchBoundList.isEmpty()) {
            return null;
        }

        List<Long> hashResults = new ArrayList<>();
        List<PhyTableOperation> hashcheckPlans = new ArrayList<>();

        Map<Integer, ParameterContext> firstBound = null, lastBound = null;
        if (batchBoundList.size() == 1) {
            firstBound = batchBoundList.get(0);
            lastBound = batchBoundList.get(0);
        } else {
            firstBound = batchBoundList.get(0);
            lastBound = batchBoundList.get(batchBoundList.size() - 1);
        }

        // build batch plans
        PhyTableOperation operation =
            isSrcTableTask ? this.planSelectHashCheckWithUpperBoundSrc : this.planSelectHashCheckWithUpperBoundDst;
        List<ParameterContext> firstBoundPc = new ArrayList<>();
        for (int i = 1; i <= firstBound.size(); i++) {
            firstBoundPc.add(firstBound.get(i));
        }
        hashcheckPlans.add(buildHashcheckPlanWithDnfParam(phyDbName, phyTable, firstBoundPc, operation, false, true));

        for (int i = 0; i < batchBoundList.size() - 1; i++) {
            PhyTableOperation operationMidBound = isSrcTableTask ? this.planSelectHashCheckWithLowerUpperBoundSrc :
                this.planSelectHashCheckWithLowerUpperBoundDst;
            Map<Integer, ParameterContext> midBound = batchBoundList.get(i);
            List<ParameterContext> midBoundPc = new ArrayList<>();
            for (int j = 1; j <= midBound.size(); j++) {
                midBoundPc.add(midBound.get(j));
            }
            midBound = batchBoundList.get(i + 1);
            for (int j = 1; j <= midBound.size(); j++) {
                midBoundPc.add(midBound.get(j));
            }
            hashcheckPlans.add(
                buildHashcheckPlanWithDnfParam(phyDbName, phyTable, midBoundPc, operationMidBound, true, true));
        }

        PhyTableOperation operationLastBound =
            isSrcTableTask ? this.planSelectHashCheckWithLowerBoundSrc : this.planSelectHashCheckWithLowerBoundDst;
        List<ParameterContext> lastBoundPc = new ArrayList<>();
        for (int i = 1; i <= lastBound.size(); i++) {
            lastBoundPc.add(lastBound.get(i));
        }
        hashcheckPlans.add(
            buildHashcheckPlanWithDnfParam(phyDbName, phyTable, lastBoundPc, operationLastBound, true, false));

        //log batch sql
        for (int i = 0; i < hashcheckPlans.size(); i++) {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] FastChecker {1}[{2}][{3}], batch {4}, phySqlInfo: {5}, param: {6}",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst", i,
                hashcheckPlans.get(i).getBytesSql(), hashcheckPlans.get(i).getParam()));
        }

        //excute
        for (int i = 0; i < hashcheckPlans.size(); i++) {
            PhyTableOperation phyPlan = hashcheckPlans.get(i);
            Long batchHashResult = executeHashcheckPlan(phyPlan, baseEc);
            if (batchHashResult != null) {
                hashResults.add(batchHashResult);
            }
            if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
                long jobId = baseEc.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        }

        if (hashResults.isEmpty()) {
            return null;
        }

        final HashCaculator caculator = new HashCaculator();
        for (Long elem : hashResults) {
            caculator.caculate(elem);
        }
        return caculator.getHashVal();
    }

    private Long getPhyTableDegistByFullScan(String phyDbName, String phyTable, ExecutionContext baseEc,
                                             boolean isSrcTableTask) {
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));
//        PhyTableOperation plan =
//            new PhyTableOperation(isSrcTableTask ? this.planSelectHashCheckSrc : this.planSelectHashCheckDst);
//        plan.setDbIndex(phyDbName);
//        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
//        plan.setParam(params);

        PhyTableOperation targetPhyOp = isSrcTableTask ? this.planSelectHashCheckSrc : this.planSelectHashCheckDst;
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(phyDbName);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(params);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        //log batch sql
        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "[{0}] FastChecker {1}[{2}][{3}], full scan, phySqlInfo: {4}",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst", plan));

        return executeHashcheckPlan(plan, baseEc);
    }

    private Long executeHashcheckPlan(PhyTableOperation plan, ExecutionContext ec) {
        return GsiUtils.retryOnException(() -> {
            Cursor cursor = null;
            Long result = null;
            try {
                cursor = ExecutorHelper.executeByCursor(plan, ec, false);
                Row row;
                if (cursor != null && (row = cursor.next()) != null) {
                    result = (Long) row.getObject(0);
                    while (cursor.next() != null) {
                        //do nothing
                    }
                }
            } finally {
                if (cursor != null) {
                    cursor.close(new ArrayList<>());
                }
            }

            return result;
        }, (e) -> {
            if (e.getSQLState() != null && e.getSQLState().equals(SQLSTATE_DEADLOCK)
                && ErrorCode.ER_LOCK_DEADLOCK.getCode() == e.getErrorCode()) {
                return true;
            }
            return false;
        }, (e, retryCount) -> {
            if (retryCount < 3) {
                // Only sleep on no retry operation(dead lock).
                try {
                    TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
                } catch (InterruptedException ex) {
                    // Throw it out, because this may caused by user interrupt.
                    throw GeneralUtil.nestedException(ex);
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER,
                    "FastChecker max retry times exceeded: " + e.getMessage());
            }
        });
    }

    // use Long to store uint64_t hash result generated by DN, since java doesn't support unsigned type.
    private Pair<Long, Boolean> hashcheckForSinglePhyTable(String phyDbName, String phyTable, ExecutionContext baseEc,
                                                           boolean isSrcTableTask, long maxBatchRows) {

        String schema = isSrcTableTask ? srcSchemaName : dstSchemaName;
        long tableRowsCount = getTableRowsCount(schema, phyDbName, phyTable);

        //get phy table's avgRowSize
        long tableAvgRowLength = getTableAvgRowSize(schema, phyDbName, phyTable);
        long fastcheckerMaxBatchFileSize =
            baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_FILE_SIZE);

        boolean needBatchCheck = false;
        if (tableRowsCount * tableAvgRowLength > fastcheckerMaxBatchFileSize || tableRowsCount > maxBatchRows) {
            needBatchCheck = true;
        }

        long startTime = System.currentTimeMillis();
        /**
         * if table size exceed batch size, we will calculate digest by batch.
         * otherwise, we will straightly calculate the whole phy table's digest
         * */

        boolean failedToSplitBatch = false;
        Long hashResult = null;

        if (needBatchCheck) {
            long finalBatchRows = maxBatchRows;
            if (tableRowsCount * tableAvgRowLength > fastcheckerMaxBatchFileSize) {
                tableAvgRowLength = max(1, tableAvgRowLength);
                finalBatchRows = fastcheckerMaxBatchFileSize / tableAvgRowLength;
            }
            if (finalBatchRows > maxBatchRows) {
                finalBatchRows = maxBatchRows;
            }

            List<Map<Integer, ParameterContext>> batchBoundList =
                splitPhyTableIntoBatch(baseEc, phyDbName, phyTable, tableRowsCount, finalBatchRows, isSrcTableTask);
            if (!batchBoundList.isEmpty()) {
                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "[{0}] FastChecker start hash phy for {1}[{2}][{3}], and phy table is divided into {4} batches",
                    baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst",
                    batchBoundList.size() + 1));

                hashResult = getPhyTableDegistByBatch(phyDbName, phyTable, baseEc, isSrcTableTask, batchBoundList,
                    tableRowsCount);
            } else {
                failedToSplitBatch = true;
            }
        }

        if (!needBatchCheck || failedToSplitBatch) {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] FastChecker start hash phy for {1}[{2}][{3}], and phy table is hashed by full scan",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst"));

            hashResult = getPhyTableDegistByFullScan(phyDbName, phyTable, baseEc, isSrcTableTask);

        }

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
            "[{0}] FastChecker finish phy hash for {1}[{2}][{3}], time use[{4}], table hash value[{5}]",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst",
            (System.currentTimeMillis() - startTime) / 1000.0, hashResult == null ? "null" : hashResult));

        return Pair.of(hashResult, isSrcTableTask);
    }

    public boolean checkWithChangeSet(ExecutionContext baseEc, boolean stopDoubleWrite, DdlTask task,
                                      List<String> relatedTables) {
        if (!stopDoubleWrite) {
            return check(baseEc);
        }

        final int timeoutMaxRetryTimes =
            baseEc.getParamManager().getInt(ConnectionParams.FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES);

        ExecutionContext tsoEc = baseEc.copy();
        tsoEc.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);

        boolean tsoCheckResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.TSO, tsoEc, (ec) -> {
            /**
             * use idle query (select ... limit 1) for each phyDB so that DN can reserve TSO timestamp,
             * to prevent "TSO snapshot too old" when checking process is time consuming.
             * */
            idleQueryForEachPhyDb(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec);

            // stop double write
            final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;
            final DdlTask currentTask = task;
            DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
                @Override
                protected Integer invoke() {
                    ComplexTaskMetaManager
                        .updateSubTasksStatusByJobIdAndObjName(task.getJobId(),
                            srcSchemaName,
                            srcLogicalTableName,
                            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                            ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER,
                            getConnection());
                    try {
                        for (String name : relatedTables) {
                            TableInfoManager.updateTableVersionWithoutDataId(srcSchemaName, name, getConnection());
                        }
                    } catch (Exception e) {
                        throw GeneralUtil.nestedException(e);
                    }
                    currentTask.setState(DdlTaskState.DIRTY);
                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(currentTask);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            };
            delegate.execute();

            LOGGER.info(
                String.format(
                    "Update table status[ schema:%s, table:%s, before state:%s, after state:%s]",
                    srcSchemaName,
                    srcLogicalTableName,
                    ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG.name(),
                    ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER.name()));

            try {
                SyncManagerHelper.sync(
                    new TablesMetaChangePreemptiveSyncAction(srcSchemaName, relatedTables, 1500L, 1500L,
                        TimeUnit.SECONDS),
                    true);
            } catch (Throwable t) {
                LOGGER.error(String.format(
                    "error occurs while sync table meta, schemaName:%s, tableName:%s", srcSchemaName,
                    srcLogicalTableName));
                throw GeneralUtil.nestedException(t);
            }

            int retryCount = 0;
            boolean timeoutHappened;
            boolean checkRet = false;
            long batchSize = ec.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_SIZE);
            do {
                timeoutHappened = false;
                try {
                    checkRet = parallelCheck(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec, this.parallelism,
                        batchSize, ParallelPolicy.PhyGroupParallel);
                } catch (TddlNestableRuntimeException e) {
                    if (StringUtils.containsIgnoreCase(e.getMessage(), "fetch phy table digest timeout")) {
                        timeoutHappened = true;
                        batchSize = batchSize / 4;
                        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                            "[{0}] FastChecker calculate digest timeout with batch size [{1}], and begin to retry with new batch Size [{2}]",
                            baseEc.getTraceId(), batchSize * 4, batchSize));
                    } else {
                        throw e;
                    }
                } finally {
                    ++retryCount;
                }

            } while (timeoutHappened && retryCount <= timeoutMaxRetryTimes);

            return checkRet;
        });

        if (!tsoCheckResult) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] FastChecker with TsoCheck failed", baseEc.getTraceId()));
        }
        return tsoCheckResult;
    }

    /**
     * batch校验超时重试：
     * 1. 当fastchecker以默认batchSize校验，出现timeout exception后，会调整batchSize到上次的1/4
     * 如此重试timeoutRetryTimes次，当超过timeoutRetryTimes次后，fastchecker抛出timeout exception
     * 2. 通常fastchecker的调用者在fastchecker外部也会自己搞重试机制（主要是为了处理其它异常），请注意当fastchecker抛出timeout exception后，
     * 不要再在外部重试，以防止叠加起来重试的次数过多
     */
    public boolean check(ExecutionContext baseEc) {
        long batchSize = baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_SIZE);
        final int timeoutMaxRetryTimes =
            baseEc.getParamManager().getInt(ConnectionParams.FASTCHECKER_BATCH_TIMEOUT_RETRY_TIMES);
        int retryCount = 0;
        boolean timeoutHappened;
        boolean tsoCheckResult = false;
        do {
            timeoutHappened = false;
            try {
                tsoCheckResult = tsoCheck(baseEc, batchSize);
            } catch (TddlNestableRuntimeException e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), "fetch phy table digest timeout")) {
                    timeoutHappened = true;
                    batchSize = batchSize / 4;
                    SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                        "[{0}] FastChecker calculate digest timeout with batch size [{1}], and begin to retry with new batch Size [{2}]",
                        baseEc.getTraceId(), batchSize * 4, batchSize));
                } else {
                    throw e;
                }
            } finally {
                ++retryCount;
            }
        } while (timeoutHappened && retryCount <= timeoutMaxRetryTimes);

        if (!tsoCheckResult) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] FastChecker with TsoCheck failed", baseEc.getTraceId()));
        }
        //boolean xaCheckResult = xaCheckForIsomorphicTable(baseEc);
        return tsoCheckResult;
    }

    protected boolean tsoCheck(ExecutionContext baseEc, long batchSize) {
        ExecutionContext tsoEc = baseEc.copy();
        tsoEc.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        boolean tsoCheckResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.TSO, tsoEc, (ec) -> {
            /**
             * use idle query (select ... limit 1) for each phyDB so that DN can reserve TSO timestamp,
             * to prevent "TSO snapshot too old" when checking process is time consuming.
             * */
            idleQueryForEachPhyDb(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec);
            return parallelCheck(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec, this.parallelism, batchSize,
                ParallelPolicy.PhyGroupParallel);
        });

        return tsoCheckResult;
    }

    /**
     * since for scaleOut, src tables and dst tables have same structure and name (but they have different phyDb)
     * we can set up readview by each pair(src table, dst table).
     * So we need not lock all the table one time. Instead, we once lock a pair of them.
     * step1. exec "lock tables ... read" for a pair of table(src, dst)
     * step2. select 1 to establish readview
     * step3. release lock
     * step4. do check
     * step5. go to step1 to check another pair of table.
     */
    protected boolean xaCheckForIsomorphicTable(ExecutionContext baseEc, long maxBatchSize) {
        // make sure that src and dst have same tableNum
        if (srcPhyDbAndTables.size() != dstPhyDbAndTables.size()) {
            return false;
        }
        for (Map.Entry<String, String> entry : sourceTargetGroup.entrySet()) {
            String srcPhyDb = entry.getKey();
            String dstPhyDb = entry.getValue();
            /**
             * since for scaleOut, src tables' Name and dst tables' are same
             * we sort them to match each pair
             * */
            List<String> srcPhyTables = srcPhyDbAndTables.get(srcPhyDb).stream().sorted().collect(Collectors.toList());
            List<String> dstPhyTables = dstPhyDbAndTables.get(dstPhyDb).stream().sorted().collect(Collectors.toList());
            if (srcPhyTables.size() != dstPhyTables.size()) {
                return false;
            }

            boolean xaSingleResult;
            for (int i = 0; i < srcPhyTables.size(); i++) {
                Map<String, Set<String>> src = ImmutableMap.of(srcPhyDb, ImmutableSet.of(srcPhyTables.get(i)));
                Map<String, Set<String>> dst = ImmutableMap.of(dstPhyDb, ImmutableSet.of(dstPhyTables.get(i)));
                Map<String, Set<String>> needLockTables =
                    ImmutableMap.of(srcPhyDb, ImmutableSet.of(srcPhyTables.get(i)), dstPhyDb,
                        ImmutableSet.of(dstPhyTables.get(i)));

                TablesLocker locker = new TablesLocker(this.srcSchemaName, needLockTables);
                try {
                    locker.lock(lockTimeOut);
                    xaSingleResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.XA, baseEc, (ec) -> {
                        try {
                            idleQueryForEachPhyDb(src, dst, ec);
                        } finally {
                            locker.unlock();
                        }
                        return parallelCheck(src, dst, ec, this.parallelism, maxBatchSize,
                            ParallelPolicy.PhyTableParallel);
                    });
                } finally {
                    locker.unlock();
                }
                if (xaSingleResult == false) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * For GSI check or other scene, src tables and dst tables are heterogeneous,
     * so we need lock all src tables and all dst tables to do check.
     * step1. exec "lock tables ... read" for all src tables and dst tables.
     * step2. select 1 to establish readview
     * step3. release lock.
     * step4. do check
     */
    protected boolean xaCheckForHeterogeneousTable(ExecutionContext baseEc, long maxBatchSize) {
        Map<String, Set<String>> needLockTables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.srcPhyDbAndTables.forEach((phyDb, phyTables) -> {
            if (needLockTables.containsKey(phyDb)) {
                needLockTables.get(phyDb).addAll(phyTables);
            } else {
                needLockTables.put(phyDb, new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
                needLockTables.get(phyDb).addAll(phyTables);
            }
        });
        //todo: in GSI, we may not lock index table
        this.dstPhyDbAndTables.forEach((phyDb, phyTables) -> {
            if (needLockTables.containsKey(phyDb)) {
                needLockTables.get(phyDb).addAll(phyTables);
            } else {
                needLockTables.put(phyDb, new TreeSet<>(String.CASE_INSENSITIVE_ORDER));
                needLockTables.get(phyDb).addAll(phyTables);
            }
        });

        boolean xaCheckResult = false;
        TablesLocker locker = new TablesLocker(this.srcSchemaName, needLockTables);
        locker.lock(lockTimeOut);

        try {
            xaCheckResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.XA, baseEc, (ec) -> {
                try {
                    idleQueryForEachPhyDb(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec);
                } finally {
                    locker.unlock();
                }
                return parallelCheck(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec, this.parallelism, maxBatchSize,
                    ParallelPolicy.PhyTableParallel);
            });
        } finally {
            locker.unlock();
        }
        return xaCheckResult;
    }

    private void idleQueryForEachPhyDb(Map<String, Set<String>> srcDbAndTb, Map<String, Set<String>> dstDbAndTb,
                                       ExecutionContext baseEc) {
        Map<Pair<String, Boolean>, Set<String>> phyDbAndTableGather =
            new TreeMap<>(new Comparator<Pair<String, Boolean>>() {
                @Override
                public int compare(Pair<String, Boolean> o1, Pair<String, Boolean> o2) {
                    int ret = String.CASE_INSENSITIVE_ORDER.compare(o1.getKey(), o2.getKey());
                    if (ret == 0) {
                        return Boolean.compare(o1.getValue(), o2.getValue());
                    } else {
                        return ret;
                    }
                }
            });
        /**
         * inorder to establish readview,
         * we only need to select * limit 1 on each phyDB.
         * */
        srcDbAndTb.forEach((phyDb, phyTables) -> {
            phyDbAndTableGather.put(Pair.of(phyDb, true), ImmutableSet.of(phyTables.stream().findFirst().get()));
        });

        dstDbAndTb.forEach((phyDb, phyTables) -> {
            phyDbAndTableGather.put(Pair.of(phyDb, false), ImmutableSet.of(phyTables.stream().findFirst().get()));
        });

        phyDbAndTableGather.forEach((phyDb, phyTables) -> phyTables.forEach(phyTable -> {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));
            PhyTableOperation targetPhyOp = phyDb.getValue() ? this.planIdleSelectSrc : this.planIdleSelectDst;

//            PhyTableOperation plan =
//                new PhyTableOperation(phyDb.getValue() ? this.planIdleSelectSrc : this.planIdleSelectDst);
//            plan.setDbIndex(phyDb.getKey());
//            plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
//            plan.setParam(params);

            PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
            buildParams.setGroupName(phyDb.getKey());
            buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
            buildParams.setDynamicParams(params);
            PhyTableOperation plan =
                PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

            GsiUtils.retryOnException(() -> {
                Cursor cursor = null;
                try {
                    cursor = ExecutorHelper.executeByCursor(plan, baseEc, false);
                    while (cursor != null && cursor.next() != null) {
                    }
                } finally {
                    if (cursor != null) {
                        cursor.close(new ArrayList<>());
                    }
                }
                return true;
            }, (e) -> {
                if (e.getSQLState() != null && e.getSQLState().equals(SQLSTATE_DEADLOCK)
                    && ErrorCode.ER_LOCK_DEADLOCK.getCode() == e.getErrorCode()) {
                    return true;
                }
                return false;
            }, (e, retryCount) -> {
                if (retryCount < 3) {
                    //sleep when dead lock.
                    try {
                        TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
                    } catch (InterruptedException ex) {
                        // Throw it out, because this may caused by user interrupt.
                        throw GeneralUtil.nestedException(ex);
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER,
                        "FastChecker(idle select) max retry times exceeded: " + e.getMessage());
                }
            });
        }));
    }

    /**
     * if ParallelPolicy is PhyTableParallel, we put all phyTable task into "runTasks" function,
     * to parallel check(it also subject to parallelism limitation).
     * if ParallelPolicy is PhyGroupParallel, we once select single phyTable task from each group.
     */
    private boolean parallelCheck(Map<String, Set<String>> srcDbAndTb, Map<String, Set<String>> dstDbAndTb,
                                  ExecutionContext baseEc, long parallelism, long maxBatchSize, ParallelPolicy policy) {
        // Force master first and following will copy this EC.
        baseEc.getExtraCmds().put(ConnectionProperties.MASTER, true);

        List<Pair<Long, Boolean>> result = new ArrayList<>();

        int srcTableTaskCount = 0;
        for (Set<String> phyTables : srcDbAndTb.values()) {
            srcTableTaskCount += phyTables.size();
        }
        int dstTableTaskCount = 0;
        for (Set<String> phyTables : dstDbAndTb.values()) {
            dstTableTaskCount += phyTables.size();
        }

        if (policy == ParallelPolicy.PhyTableParallel) {
            final List<FutureTask<Pair<Long, Boolean>>> allFutureTasks =
                new ArrayList<>(srcTableTaskCount + dstTableTaskCount);
            final BlockingQueue<Object> blockingQueue =
                parallelism <= 0 ? null : new ArrayBlockingQueue<>((int) parallelism);
            //gather src tasks
            srcDbAndTb.forEach((phyDb, phyTables) -> phyTables.forEach(
                phyTable -> allFutureTasks.add(new FutureTask<Pair<Long, Boolean>>(() -> {
                    try {
                        return hashcheckForSinglePhyTable(phyDb, phyTable, baseEc, true, maxBatchSize);
                    } finally {
                        // Poll in finally to prevent dead lock on putting blockingQueue.
                        if (blockingQueue != null) {
                            blockingQueue.poll(); // Parallelism control notify.
                        }
                    }
                }))));

            dstDbAndTb.forEach(
                (phyDb, phyTables) -> phyTables.forEach(phyTable -> allFutureTasks.add(new FutureTask<>(() -> {
                    try {
                        return hashcheckForSinglePhyTable(phyDb, phyTable, baseEc, false, maxBatchSize);
                    } finally {
                        // Poll in finally to prevent dead lock on putting blockingQueue.
                        if (blockingQueue != null) {
                            blockingQueue.poll(); // Parallelism control notify.
                        }
                    }
                }))));

            Collections.shuffle(allFutureTasks);

            runTasks(allFutureTasks, blockingQueue, result, parallelism);

        } else if (policy == ParallelPolicy.PhyGroupParallel) {
            // tablesByGroup<phyDb, Set<Pair<phyTable, isSrc>>>
            final Map<String, Set<Pair<String, Boolean>>> tablesByGroup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            srcDbAndTb.forEach((phyDb, phyTables) -> phyTables.forEach(phyTable -> {
                if (tablesByGroup.containsKey(phyDb)) {
                    tablesByGroup.get(phyDb).add(Pair.of(phyTable, true));
                } else {
                    tablesByGroup.put(phyDb, new TreeSet<>(new Comparator<Pair<String, Boolean>>() {
                        @Override
                        public int compare(Pair<String, Boolean> o1, Pair<String, Boolean> o2) {
                            int ret = String.CASE_INSENSITIVE_ORDER.compare(o1.getKey(), o2.getKey());
                            if (ret == 0) {
                                ret = Boolean.compare(o1.getValue(), o2.getValue());
                            }
                            return ret;
                        }
                    }));
                    tablesByGroup.get(phyDb).add(Pair.of(phyTable, true));
                }
            }));
            dstDbAndTb.forEach((phyDb, phyTables) -> phyTables.forEach(phyTable -> {
                if (tablesByGroup.containsKey(phyDb)) {
                    tablesByGroup.get(phyDb).add(Pair.of(phyTable, false));
                } else {
                    tablesByGroup.put(phyDb, new TreeSet<>(new Comparator<Pair<String, Boolean>>() {
                        @Override
                        public int compare(Pair<String, Boolean> o1, Pair<String, Boolean> o2) {
                            int ret = String.CASE_INSENSITIVE_ORDER.compare(o1.getKey(), o2.getKey());
                            if (ret == 0) {
                                ret = Boolean.compare(o1.getValue(), o2.getValue());
                            }
                            return ret;
                        }
                    }));
                    tablesByGroup.get(phyDb).add(Pair.of(phyTable, false));
                }
            }));

            while (!tablesByGroup.isEmpty()) {
                final BlockingQueue<Object> blockingQueue = parallelism <= 0 ? null : new ArrayBlockingQueue<>(
                    (int) parallelism);
                final List<FutureTask<Pair<Long, Boolean>>> futureTasks = new ArrayList<>();
                List<String> finishPhyDb = new ArrayList<>();
                tablesByGroup.forEach((phyDb, phyTables) -> {
                    if (phyTables.isEmpty()) {
                        finishPhyDb.add(phyDb);
                    } else {
                        Pair<String, Boolean> phyTable = phyTables.stream().findFirst().get();
                        futureTasks.add(new FutureTask<>(() -> {
                            try {
                                return hashcheckForSinglePhyTable(phyDb, phyTable.getKey(), baseEc,
                                    phyTable.getValue(), maxBatchSize);
                            } finally {
                                if (blockingQueue != null) {
                                    blockingQueue.poll();
                                }
                            }
                        }));
                        phyTables.remove(phyTable);
                    }
                });
                finishPhyDb.forEach(dbName -> {
                    tablesByGroup.remove(dbName);
                });

                runTasks(futureTasks, blockingQueue, result, parallelism);
            }
        }

        List<Long> srcResult =
            result.stream().filter(item -> item != null && item.getKey() != null && item.getValue()).map(Pair::getKey)
                .collect(Collectors.toList());
        List<Long> dstResult =
            result.stream().filter(item -> item != null && item.getKey() != null && !item.getValue()).map(Pair::getKey)
                .collect(Collectors.toList());

        return srcTableTaskCount == result.stream().filter(Objects::nonNull).filter(Pair::getValue).count()
            && dstTableTaskCount == result.stream().filter(Objects::nonNull).filter(x -> !x.getValue()).count()
            && compare(srcResult, dstResult);
    }

    private boolean compare(List<Long> src, List<Long> dst) {
        final HashCaculator srcCaculator = new HashCaculator();
        final HashCaculator dstCaculator = new HashCaculator();
        src.forEach(elem -> srcCaculator.caculate(elem));
        dst.forEach(elem -> dstCaculator.caculate(elem));
        return srcCaculator.getHashVal().equals(dstCaculator.getHashVal());
    }

    private void runTasks(List<FutureTask<Pair<Long, Boolean>>> futures, BlockingQueue<Object> blockingQueue,
                          List<Pair<Long, Boolean>> result, long parallelism) {
        AtomicReference<Exception> excep = new AtomicReference<>(null);
        if (parallelism <= 0) {
            futures.forEach(task -> FastCheckerThreadPool.getInstance()
                .executeWithContext(task, PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK));
        } else {
            futures.forEach(task -> {
                try {
                    blockingQueue.put(task); // Just put an object to get blocked when full.
                } catch (Exception e) {
                    excep.set(e);
                }
                if (null == excep.get()) {
                    FastCheckerThreadPool.getInstance()
                        .executeWithContext(task, PriorityFIFOTask.TaskPriority.HIGH_PRIORITY_TASK);
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

        for (FutureTask<Pair<Long, Boolean>> future : futures) {
            try {
                result.add(future.get());
            } catch (ExecutionException e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });
                if (null == excep.get()) {
                    excep.set(e);
                }
                if (e.getMessage().toLowerCase().contains("XResult stream fetch result timeout".toLowerCase())) {
                    throw new TddlNestableRuntimeException("fastchecker fetch phy table digest timeout", e);
                }
            } catch (InterruptedException e) {
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
    }

    public void reportCheckOk(ExecutionContext ec) {
        final CheckerManager checkerManager = new CheckerManager(srcSchemaName);
        final String finishDetails = "FastChecker check OK.";
        checkerManager.insertReports(ImmutableList.of(
            new CheckerManager.CheckerReport(-1, ec.getDdlJobId(), srcSchemaName, srcLogicalTableName, dstSchemaName,
                dstLogicalTableName, "", "", "SUMMARY",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                CheckerManager.CheckerReportStatus.FINISH.getValue(), "--", finishDetails, "Reporter.", null)));
    }

    class HashCaculator {
        private final Long p;
        private final Long q;
        private final Long r;

        private Long hashVal;
        private boolean firstCaculate;

        //p q r should be same as p q r in DN
        public HashCaculator() {
            p = 3860031L;
            q = 2779L;
            r = 2L;
            hashVal = 0L;
            firstCaculate = true;
        }

        public Long caculate(Long elem) {
            if (firstCaculate == true) {
                hashVal = elem;
                firstCaculate = false;
            } else {
                hashVal = p + q * (hashVal + elem) + r * hashVal * elem;
            }
            return hashVal;
        }

        public Long getHashVal() {
            return hashVal;
        }
    }

    /**
     * use TableLocker to lock phyTables.
     * we save all the connections to execute unlock.
     */
    class TablesLocker {
        /**
         * set lock table timeout(n seconds) in session level
         */
        private static final String TABLE_LOCK_TIMEOUT = "SET SESSION LOCK_WAIT_TIMEOUT = ";
        private static final String LOCK_TABLES = "LOCK TABLES ";
        private static final String READ_MODE = " READ";
        private static final String UNLOCK_TABLES = "UNLOCK TABLES";

        private final String schemaName;
        private final Map<String, Set<String>> phyDbAndTables;
        private Map<String, Connection> lockConnections;

        public TablesLocker(String schemaName, Map<String, Set<String>> phyDbAndTables) {
            this.schemaName = schemaName;
            this.phyDbAndTables = phyDbAndTables;
            this.lockConnections = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        }

        public void lock(int timeOutSeconds) {
            phyDbAndTables.forEach((phyDb, phyTables) -> {
                if (phyTables.size() == 0) {
                    //this return only break the lambda, it will not finish lock function
                    return;
                }
                TGroupDataSource dataSource = getDataSource(phyDb);
                if (dataSource == null) {
                    this.unlock();
                    throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "FastChecker get connection fail.");
                }

                boolean lockFailed = false;
                long connOrignalLockWaitTimeout = -1;
                Connection conn = null;
                try {
                    conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
                    lockConnections.put(phyDb, conn);

                    String showLockWaitTimeoutStmt = "show variables like 'lock_wait_timeout'";
                    try (PreparedStatement ps = conn.prepareStatement(showLockWaitTimeoutStmt);
                        ResultSet rs = ps.executeQuery()) {
                        boolean hasNext = rs.next();
                        if (hasNext) {
                            connOrignalLockWaitTimeout = Long.valueOf(rs.getString("Value"));
                        }
                    }

                    String setLockWaitTimeOutStmt = TABLE_LOCK_TIMEOUT + timeOutSeconds;
                    try (PreparedStatement ps = conn.prepareStatement(setLockWaitTimeOutStmt)) {
                        ps.execute();
                    }

                    String lockTblStmt = LOCK_TABLES + String.join(READ_MODE + ", ", phyTables) + READ_MODE;
                    try (PreparedStatement ps = conn.prepareStatement(lockTblStmt)) {
                        ps.execute();
                    }

                } catch (SQLException e) {
                    /**
                     * DN timeout will throw SQLException
                     * */
                    lockFailed = true;
                    if (StringUtils.containsIgnoreCase(e.getMessage(), "Lock wait timeout")) {
                        throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "FastChecker acquire lock timeout.",
                            e);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "FastChecker acquire lock failed.",
                            e);
                    }
                } catch (TddlRuntimeException e) {
                    /**
                     * CN(tddl) timeout will throw TddlRuntimeException
                     * */
                    lockFailed = true;
                    if (StringUtils.containsIgnoreCase(e.getMessage(), "Query timeout")) {
                        throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "FastChecker acquire lock timeout.",
                            e);
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "FastChecker acquire lock failed.",
                            e);
                    }
                } finally {

                    try {
                        if (connOrignalLockWaitTimeout > -1 && conn != null) {
                            String recoverConnLockWaitTimeOutStmt = TABLE_LOCK_TIMEOUT + connOrignalLockWaitTimeout;
                            try (PreparedStatement ps = conn.prepareStatement(recoverConnLockWaitTimeOutStmt)) {
                                ps.execute();
                            }
                        }
                    } catch (Throwable ex) {
                        logger.warn("Failed to recover connection lock wait timeout", ex);
                    }

                    if (lockFailed) {
                        this.unlock();
                    }

                }
            });
        }

        public void unlock() {
            lockConnections.forEach((phyDb, conn) -> {
                try {
                    String statement = UNLOCK_TABLES;
                    PreparedStatement ps = conn.prepareStatement(statement);
                    ps.execute();
                } catch (Throwable e) {
                    logger.warn("Failed to exec unlock tables", e);
                }
            });

            lockConnections.forEach((phyDb, conn) -> {
                try {
                    conn.close();
                } catch (Throwable e) {
                    logger.warn("Failed to close locked connections", e);
                }
            });

            lockConnections.clear();
        }

        private TGroupDataSource getDataSource(String phyDb) {
            TopologyHandler topology = ExecutorContext.getContext(schemaName).getTopologyHandler();
            Object dataSource = topology.get(phyDb).getDataSource();
            if (dataSource != null && dataSource instanceof TGroupDataSource) {
                return (TGroupDataSource) dataSource;
            }
            return null;
        }
    }

    public void setDstPhyDbAndTables(Map<String, Set<String>> dstPhyDbAndTables) {
        this.dstPhyDbAndTables = dstPhyDbAndTables;
    }

    public void setSrcPhyDbAndTables(Map<String, Set<String>> srcPhyDbAndTables) {
        this.srcPhyDbAndTables = srcPhyDbAndTables;
    }

    public static List<String> getorderedPrimaryKeys(TableMeta tableMeta, ExecutionContext ec) {
        final SchemaManager sm = ec.getSchemaManager(tableMeta.getSchemaName());
        List<String> primaryKeys = ImmutableList
            .copyOf((tableMeta.isHasPrimaryKey() ? tableMeta.getPrimaryIndex().getKeyColumns() :
                tableMeta.getGsiImplicitPrimaryKey())
                .stream().map(ColumnMeta::getName).collect(Collectors.toList()));
        if (GeneralUtil.isEmpty(primaryKeys) && tableMeta.isGsi()) {
            String primaryTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
            final TableMeta primaryTableMeta = sm.getTable(primaryTable);
            primaryKeys = primaryTableMeta.getPrimaryIndex().getKeyColumns().stream().map(ColumnMeta::getName)
                .collect(Collectors.toList());
        }
        return primaryKeys;
    }
}
