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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.util.ChangeSetUtils;
import com.alibaba.polardbx.executor.ddl.workqueue.FastCheckerThreadPool;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupBatchChecker;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupReporter;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyOperationBuilderCommon;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.IMppTsoTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName;
import static java.lang.Math.max;

public class FastChecker extends PhyOperationBuilderCommon {
    private static final String FAST_CHECKER_REPORT_NAME = "FastCheckerReporter.";
    private static final Logger logger = LoggerFactory.getLogger(FastChecker.class);

    protected final String srcSchemaName;

    protected final String dstSchemaName;
    protected final String srcLogicalTableName;
    protected final String dstLogicalTableName;
    private Map<String, Set<String>> srcPhyDbAndTables;
    private Map<String, Set<String>> dstPhyDbAndTables;
    Map<Pair<String, String>, List<Pair<String, String>>> srcToDstPhyDbAndTablesMap;
    boolean isMirrorCheck;
    private final List<String> srcColumns;
    private final List<String> dstColumns;
    private final List<String> srcPks;
    private final List<String> dstPks;

    private final ITransactionManager tm;

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

    protected volatile AtomicInteger phyTaskSum;
    protected volatile AtomicInteger phyTaskFinished;

    /**
     * srcColumns and dstColumns must have the same order,
     * otherwise the check result may be wrong.
     */

    /**
     * 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序！！！
     */
    public FastChecker(String srcSchemaName, String dstSchemaName,
                       String srcLogicalTableName, String dstLogicalTableName,
                       Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                       List<String> srcColumns, List<String> dstColumns,
                       List<String> srcPks, List<String> dstPks,
                       PhyTableOperation planSelectHashCheckSrc,
                       PhyTableOperation planSelectHashCheckWithUpperBoundSrc,
                       PhyTableOperation planSelectHashCheckWithLowerBoundSrc,
                       PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc,
                       PhyTableOperation planSelectHashCheckDst, PhyTableOperation planSelectHashCheckWithUpperBoundDst,
                       PhyTableOperation planSelectHashCheckWithLowerBoundDst,
                       PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst, PhyTableOperation planIdleSelectSrc,
                       PhyTableOperation planIdleSelectDst, PhyTableOperation planSelectSampleSrc,
                       PhyTableOperation planSelectSampleDst,
                       Map<Pair<String, String>, List<Pair<String, String>>> srcToDstPhyDbAndTablesMap,
                       boolean isMirrorCheck) {
        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.srcLogicalTableName = srcLogicalTableName;
        this.dstLogicalTableName = dstLogicalTableName;
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

        this.phyTaskSum = new AtomicInteger(0);
        this.phyTaskFinished = new AtomicInteger(0);

        this.srcToDstPhyDbAndTablesMap = srcToDstPhyDbAndTablesMap;
        this.isMirrorCheck = isMirrorCheck;
        this.tm = ExecutorContext.getContext(srcSchemaName).getTransactionManager();
    }

    public static boolean isSupported(String schema) {
        return ExecutorContext.getContext(schema).getStorageInfoManager().supportFastChecker();
    }

    public static FastChecker create(String schemaName, String tableName,
                                     Map<String, Set<String>> srcPhyDbAndTables,
                                     Map<String, Set<String>> dstPhyDbAndTables,
                                     Map<Pair<String, String>, List<Pair<String, String>>> srcToDstPhyDbAndTablesMap,
                                     boolean isMirrorCheck,
                                     ExecutionContext ec) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta tableMeta = sm.getTable(tableName);

        if (null == tableMeta) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, "Incorrect SCALEOUT relationship.");
        }

        final List<String> allColumns =
            tableMeta.getAllColumns().stream().map(ColumnMeta::getName).collect(Collectors.toList());
        final List<String> allColumnsDst = new ArrayList<>(allColumns);
        final List<String> srcPks = getOrderedPrimaryKeys(tableMeta);
        final List<String> dstPks = new ArrayList<>(srcPks);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new FastChecker(schemaName, schemaName, tableName, tableName, srcPhyDbAndTables,
            dstPhyDbAndTables, allColumns, allColumnsDst, srcPks, dstPks,
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

            builder.buildSqlSelectForSample(tableMeta, srcPks),
            builder.buildSqlSelectForSample(tableMeta, dstPks),
            srcToDstPhyDbAndTablesMap,
            isMirrorCheck);
    }

    protected long getTableRowsCount(final String schema, final String dbIndex, final String phyTable) {
        String dbIndexWithoutGroup = buildPhysicalDbNameFromGroupName(dbIndex);
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

    protected long getTableAvgRowSize(final String schema, final String dbIndex, final String phyTable) {
        String dbIndexWithoutGroup = buildPhysicalDbNameFromGroupName(dbIndex);
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

    private PhyTableOperation buildSamplePlanWithParam(String dbIndex, String phyTable,
                                                       float calSamplePercentage, boolean isSrcSchema) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));
        PhyTableOperation phyTableOperation = isSrcSchema ? this.planSelectSampleSrc : this.planSelectSampleDst;
        SqlSelect sqlSelect = (SqlSelect) phyTableOperation.getNativeSqlNode().clone();
        OptimizerHint optimizerHint = new OptimizerHint();
        optimizerHint.addHint("+sample_percentage(" + GeneralUtil.formatSampleRate(calSamplePercentage) + ")");
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
    protected List<Map<Integer, ParameterContext>> splitPhyTableIntoBatch(final ExecutionContext baseEc,
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

        PhyTableOperation plan = buildSamplePlanWithParam(phyDbName, phyTable, calSamplePercentage, isSrcSchema);
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker {1}[{2}][{3}], begin to sample, phy table rows {4}, "
                + "actual sample rate {5}%, phySqlInfo: {6}, param: {7}",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
            tableRowsCount, calSamplePercentage, plan.getBytesSql(), plan.getParam()));

        List<List<Object>> sampledUnorderedRowsValue = new ArrayList<>();
        List<Map<Integer, ParameterContext>> sampledUnorderedRowsPc = null;
        List<Map<Integer, ParameterContext>> returnedSampledRowsPc = new ArrayList<>();

        // execute sample query
        Cursor cursor = null;
        List<ColumnMeta> columnMetas = null;
        try {
            cursor = ExecutorHelper.execute(plan, baseEc);
            columnMetas = cursor.getReturnColumns();
            sampledUnorderedRowsPc =
                Transformer.convertUpperBoundWithDefaultForFastChecker(cursor, false, sampledUnorderedRowsValue);
        } finally {
            if (cursor != null) {
                cursor.close(new ArrayList<>());
            }
        }

        if (sampledUnorderedRowsPc.isEmpty() || columnMetas == null || columnMetas.isEmpty()) {
            return ImmutableList.of();
        }

        final List<ColumnMeta> metasForSort = columnMetas;
        Map<List<Object>, Integer> orderedRowWithIdx = new TreeMap<>(
            (r1, r2) -> {
                for (int i = 0; i < metasForSort.size(); i++) {
                    ColumnMeta columnMeta = metasForSort.get(i);
                    int re = columnMeta.getDataType().compare(r1.get(i), r2.get(i));
                    if (re != 0) {
                        return re;
                    }
                }
                return 0;
            }
        );

        //get bound and sort bound
        long step = sampledUnorderedRowsValue.size() / batchNum;
        if (step <= 0) {
            return batchBoundList;
        }
        for (int i = 1; i < batchNum; i++) {
            long boundIndex = i * step;
            if (boundIndex < sampledUnorderedRowsValue.size()) {
                orderedRowWithIdx.put(sampledUnorderedRowsValue.get((int) boundIndex), (int) boundIndex);
            }
        }

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker {1}[{2}][{3}] sampled rows num {4}, batchNum {5}, step {6}",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
                sampledUnorderedRowsValue.size(),
                batchNum,
                step
            )
        );

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker {1}[{2}][{3}] sampled bound after sort is {4} {5}",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
                metasForSort,
                orderedRowWithIdx
            )
        );

        //get ordered pc
        for (Map.Entry<List<Object>, Integer> entry : orderedRowWithIdx.entrySet()) {
            int idx = entry.getValue();
            returnedSampledRowsPc.add(sampledUnorderedRowsPc.get(idx));
        }

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker {1}[{2}][{3}] pc for check after sort is {4} {5}",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcSchema ? "src" : "dst",
                metasForSort,
                returnedSampledRowsPc
                    .stream()
                    .map(pcMap -> pcMap.values()
                        .stream()
                        .map(parameterContext -> {
                            if (parameterContext.getValue() instanceof byte[]) {
                                return Arrays.toString((byte[]) parameterContext.getValue());
                            } else {
                                return parameterContext.getValue().toString();
                            }
                        })
                        .collect(Collectors.joining(", ", "[", "]"))
                    )
                    .collect(Collectors.joining(", "))
            )
        );

        return returnedSampledRowsPc;
    }

    protected PhyTableOperation genHashCheckPlan(String phyDbName, String phyTable, boolean isSrcTableTask,
                                                 Map<Integer, ParameterContext> batchBound1,
                                                 Map<Integer, ParameterContext> batchBound2, Boolean withLowerBound,
                                                 Boolean withUpperBound) {
        PhyTableOperation resultOperation = null;
        List<ParameterContext> boundPc = new ArrayList<>();
        for (int i = 1; i <= batchBound1.size(); i++) {
            boundPc.add(batchBound1.get(i));
        }
        if (withLowerBound && withUpperBound) {
            for (int i = 1; i <= batchBound1.size(); i++) {
                boundPc.add(batchBound2.get(i));
            }
            PhyTableOperation operation =
                isSrcTableTask ? this.planSelectHashCheckWithLowerUpperBoundSrc :
                    this.planSelectHashCheckWithLowerUpperBoundDst;
            resultOperation = buildHashcheckPlanWithDnfParam(phyDbName, phyTable, boundPc, operation, true, true);
        } else if (withLowerBound) {
            PhyTableOperation operation =
                isSrcTableTask ? this.planSelectHashCheckWithLowerBoundSrc : this.planSelectHashCheckWithLowerBoundDst;
            resultOperation = buildHashcheckPlanWithDnfParam(phyDbName, phyTable, boundPc, operation, true, false);
        } else if (withUpperBound) {
            PhyTableOperation operation =
                isSrcTableTask ? this.planSelectHashCheckWithUpperBoundSrc : this.planSelectHashCheckWithUpperBoundDst;
            resultOperation = buildHashcheckPlanWithDnfParam(phyDbName, phyTable, boundPc, operation, false, true);
        }
        return resultOperation;
    }

    protected List<PhyTableOperation> genHashCheckPlans(String phyDbName, String phyTable, boolean isSrcTableTask,
                                                        List<Map<Integer, ParameterContext>> batchBoundList) {
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
        return hashcheckPlans;
    }

    private Long getPhyTableDigestByBatch(String phyDbName, String phyTable, ExecutionContext baseEc,
                                          boolean isSrcTableTask,
                                          List<Map<Integer, ParameterContext>> batchBoundList) {
        if (batchBoundList.isEmpty()) {
            return null;
        }

        List<Long> hashResults = new ArrayList<>();
        List<PhyTableOperation> hashCheckPlans = genHashCheckPlans(phyDbName, phyTable, isSrcTableTask, batchBoundList);
        // execute
        for (PhyTableOperation phyPlan : hashCheckPlans) {
            Long batchHashResult = executeHashcheckPlan(phyPlan, baseEc);
            if (batchHashResult != null) {
                hashResults.add(batchHashResult);
            }
            SQLRecorderLogger.ddlLogger.info(String.format(
                "fetch hashcheck result for [%s.%s], [%s]: [%s]",
                phyDbName, phyTable, StringUtils.join(batchBoundList.stream().map(o -> GsiUtils.rowToString(o)).collect(
                    Collectors.toList()), ";"),
                hashResults
            ));
            if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
                long jobId = baseEc.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
        }

        if (hashResults.isEmpty()) {
            return null;
        }

        final HashCalculator calculator = new HashCalculator();
        for (Long elem : hashResults) {
            calculator.calculate(elem);
        }
        return calculator.getHashVal();
    }

    private Long getPhyTableDigestByBatch(CheckerBatch checkerBatch, ExecutionContext baseEc) {
        if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
            long jobId = baseEc.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }
        Map<Integer, ParameterContext> batchBound2 = null;
        List<Map<Integer, ParameterContext>> batchBoundList = checkerBatch.batchBound;
        if (batchBoundList.isEmpty()) {
            return null;
        } else if (batchBoundList.size() > 1) {
            batchBound2 = batchBoundList.get(1);
        }

        PhyTableOperation hashCheckPlan =
            genHashCheckPlan(checkerBatch.phyDb, checkerBatch.phyTb, checkerBatch.isSourceTable, batchBoundList.get(0),
                batchBound2, checkerBatch.withLowerBound,
                checkerBatch.withUpperBound);
        // execute
        Long batchHashResult = executeHashcheckPlan(hashCheckPlan, baseEc);
        SQLRecorderLogger.ddlLogger.info(String.format(
            "fetch hashcheck result for [%s] : [%s]",
            checkerBatch.getBatchBoundDesc(),
            batchHashResult
        ));
        return batchHashResult;
    }

    protected PhyTableOperation genHashCheckPlan(String phyDbName, String phyTable, ExecutionContext baseEc,
                                                 boolean isSrcTableTask) {
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        PhyTableOperation targetPhyOp = isSrcTableTask ? this.planSelectHashCheckSrc : this.planSelectHashCheckDst;
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(phyDbName);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(params);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        //log batch sql
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker {1}[{2}][{3}], full scan, phySqlInfo: {4}",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst", plan));

        return plan;
    }

    private Long getPhyTableDigestByFullScan(String phyDbName, String phyTable, ExecutionContext baseEc,
                                             boolean isSrcTableTask) {

        if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
            long jobId = baseEc.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        PhyTableOperation plan = genHashCheckPlan(phyDbName, phyTable, baseEc, isSrcTableTask);

        return executeHashcheckPlan(plan, baseEc);
    }

    private Long executeHashcheckPlan(PhyTableOperation plan, ExecutionContext ec) {
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
    }

    /**
     * 1. 含local partition的表将不进行batch check (因为sample结果是乱序的)
     * 2. 逻辑表为unique gsi的表将不进行batch check (因为其物理表只含主键列，但主键列没有主键属性)
     */
    protected boolean whetherCanSplitIntoBatch(ExecutionContext baseEc, boolean isSrc) {
        //won't do sample for local partition table
        TableMeta tableMeta = isSrc ?
            baseEc.getSchemaManager(srcSchemaName).getTable(srcLogicalTableName)
            : baseEc.getSchemaManager(dstSchemaName).getTable(dstLogicalTableName);
        if (tableMeta != null && tableMeta.getLocalPartitionDefinitionInfo() != null) {
            return false;
        }

        //won't sample for missing primary key table
        List<String> pks = isSrc ? this.srcPks : this.dstPks;
        if (pks.isEmpty()) {
            return false;
        }
        return true;
    }

    private List<Pair<String, Runnable>> generateAsyncTask(
        List<Map<Integer, ParameterContext>> batchBoundList,
        LinkedBlockingQueue<FutureTask<Pair<Long, Boolean>>> allTasks,
        Map mdcContext, String storageInst, String phyDbName, String phyTable,
        ExecutionContext baseEc,
        Boolean isSrcTableTask, Map<Runnable, CheckerBatch> taskMap) {
        List<Pair<String, Runnable>> tasks = new ArrayList<>();
        Map<Integer, ParameterContext> firstBatchBound = batchBoundList.get(0);
        int batchIndex = 0;
        CheckerBatch firstBatch =
            new CheckerBatch(phyDbName, phyTable, batchIndex, isSrcTableTask, Lists.newArrayList(firstBatchBound),
                false, true);
        FutureTask<Pair<Long, Boolean>> firstTask = new FutureTask<>(
            () -> {
                MDC.setContextMap(mdcContext);
                Long result = getPhyTableDigestByBatch(firstBatch, baseEc);
                return Pair.of(result, isSrcTableTask);
            }
        );
        allTasks.offer(firstTask);
        taskMap.put(firstTask, firstBatch);
        batchIndex++;

        tasks.add(Pair.of(storageInst, firstTask));
        for (; batchIndex < batchBoundList.size(); batchIndex++) {
            List<Map<Integer, ParameterContext>> middlebatchBoundList =
                batchBoundList.subList(batchIndex - 1, batchIndex + 1);
            CheckerBatch middleBatch =
                new CheckerBatch(phyDbName, phyTable, batchIndex, isSrcTableTask, middlebatchBoundList, true, true);
            FutureTask<Pair<Long, Boolean>> middleTask = new FutureTask<>(
                () -> {
                    MDC.setContextMap(mdcContext);
                    Long result = getPhyTableDigestByBatch(middleBatch, baseEc);
                    return Pair.of(result, isSrcTableTask);
                }
            );
            allTasks.offer(middleTask);
            taskMap.put(middleTask, middleBatch);
            tasks.add(Pair.of(storageInst, middleTask));
        }
        Map<Integer, ParameterContext> lastBatchBound = batchBoundList.get(batchBoundList.size() - 1);
        CheckerBatch lastBatch =
            new CheckerBatch(phyDbName, phyTable, batchIndex, isSrcTableTask, Lists.newArrayList(lastBatchBound), true,
                false);
        FutureTask<Pair<Long, Boolean>> lastTask = new FutureTask<>(
            () -> {
                MDC.setContextMap(mdcContext);
                Long result = getPhyTableDigestByBatch(lastBatch, baseEc);
                return Pair.of(result, isSrcTableTask);
            }
        );
        allTasks.offer(lastTask);
        taskMap.put(lastTask, lastBatch);
        batchIndex++;
        tasks.add(Pair.of(storageInst, lastTask));
        return tasks;
    }

    private List<Map<Integer, ParameterContext>> sampleForBatchBoundList(String phyDbName, String phyTable,
                                                                         ExecutionContext baseEc,
                                                                         Boolean isSrcTableTask,
                                                                         long maxBatchRows) {

        String schema = isSrcTableTask ? srcSchemaName : dstSchemaName;
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker start to divide for {1}[{2}][{3}]",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst"));
        List<Map<Integer, ParameterContext>> batchBoundList = new ArrayList<>();
        long tableRowsCount = getTableRowsCount(schema, phyDbName, phyTable);

        //get phy table's avgRowSize
        long tableAvgRowLength = getTableAvgRowSize(schema, phyDbName, phyTable);
        long fastcheckerMaxBatchFileSize =
            baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_FILE_SIZE);

        boolean needBatchCheck = false;
        if (tableRowsCount * tableAvgRowLength > fastcheckerMaxBatchFileSize || tableRowsCount > maxBatchRows) {
            needBatchCheck = true;
        }

        /**
         * if table size exceed batch size, we will calculate digest by batch.
         * otherwise, we will straightly calculate the whole phy table's digest
         * */

        if (!whetherCanSplitIntoBatch(baseEc, isSrcTableTask)) {
            needBatchCheck = false;
        }

        if (needBatchCheck) {
            long finalBatchRows = maxBatchRows;
            if (tableRowsCount * tableAvgRowLength > fastcheckerMaxBatchFileSize) {
                tableAvgRowLength = max(1, tableAvgRowLength);
                finalBatchRows = fastcheckerMaxBatchFileSize / tableAvgRowLength;
            }
            if (finalBatchRows > maxBatchRows) {
                finalBatchRows = maxBatchRows;
            }

            batchBoundList =
                splitPhyTableIntoBatch(baseEc, phyDbName, phyTable, tableRowsCount, finalBatchRows, isSrcTableTask);
            if (!batchBoundList.isEmpty()) {
                SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                    "[{0}] FastChecker start divide for {1}[{2}][{3}], and phy table is divided into {4} batches",
                    baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst",
                    batchBoundList.size() + 1));
            }
        }
        return batchBoundList;
    }

    private Pair<Long, Boolean> hashCheckForSinglePhyTable(String phyDbName, String phyTable, ExecutionContext baseEc,
                                                           boolean isSrcTableTask, long maxBatchRows,
                                                           FastCheckerThreadPool fastCheckerThreadPool,
                                                           Map mdcContext, String storageInst,
                                                           LinkedBlockingQueue<FutureTask<Pair<Long, Boolean>>> allTasks,
                                                           AtomicInteger totalNum,
                                                           List<Map<Integer, ParameterContext>> batchBoundList,
                                                           Map<Runnable, CheckerBatch> taskMap) {

        Boolean internalParallel = baseEc.getParamManager().getBoolean(ConnectionParams.FASTCHECKER_BATCH_PARALLEL);
        Boolean asyncSumbitTasks = false;

        Long hashResult = null;
        Long startTime = System.currentTimeMillis();
        Boolean failedToSplitBatch = false;
        if (!batchBoundList.isEmpty()) {
            if (internalParallel) {
                List<Pair<String, Runnable>> tasks =
                    generateAsyncTask(batchBoundList, allTasks, mdcContext, storageInst, phyDbName, phyTable,
                        baseEc, isSrcTableTask, taskMap);
                totalNum.getAndAdd(tasks.size());
                fastCheckerThreadPool.submitTasks(tasks);
                asyncSumbitTasks = true;
            } else {
                hashResult =
                    getPhyTableDigestByBatch(phyDbName, phyTable, baseEc, isSrcTableTask, batchBoundList);
            }
        } else {
            failedToSplitBatch = true;
        }

        if (failedToSplitBatch) {
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker start hash phy for {1}[{2}][{3}], and phy table is hashed by full scan",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst"));

            hashResult = getPhyTableDigestByFullScan(phyDbName, phyTable, baseEc, isSrcTableTask);

        }
        if (asyncSumbitTasks) {
            // return immediately, because totalNum has been set.
            return Pair.of(null, isSrcTableTask);
        }

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0}] FastChecker finish phy hash for {1}[{2}][{3}], time use[{4}], table hash value[{5}]",
            baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst",
            (System.currentTimeMillis() - startTime) / 1000.0, hashResult == null ? "null" : hashResult));

        this.phyTaskFinished.incrementAndGet();

        FastCheckerThreadPool.getInstance().increaseCheckTaskInfo(
            baseEc.getDdlJobId(),
            0,
            1
        );

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
        //set trx isolation: RR
        tsoEc.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        //set share readView
        tsoEc.setShareReadView(true);
        //socket timeout unit: ms
        ParamManager.setVal(
            tsoEc.getParamManager().getProps(),
            ConnectionParams.SOCKET_TIMEOUT,
            Integer.toString(1000 * 60 * 60 * 24 * 7),
            true
        );

        boolean tsoCheckResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.TSO, tsoEc, (ec) -> {
            /**
             * use idle query (select ... limit 1) for each phyDB so that DN can reserve TSO timestamp,
             * to prevent "TSO snapshot too old" when checking process is time consuming.
             * */
            idleQueryForEachPhyDb(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec);

            resendSnapshotTimestamp(ec, baseEc.getTraceId());

            // stop double write
            ChangeSetUtils.doChangeSetSchemaChange(
                srcSchemaName, srcLogicalTableName,
                relatedTables, task,
                ComplexTaskMetaManager.ComplexTaskStatus.WRITE_REORG,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY
            );

            ChangeSetUtils.doChangeSetSchemaChange(
                srcSchemaName, srcLogicalTableName,
                relatedTables, task,
                ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
                ComplexTaskMetaManager.ComplexTaskStatus.DOING_CHECKER
            );

            int retryCount = 0;
            boolean timeoutHappened;
            boolean checkRet = false;
            long batchSize = ec.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_SIZE);
            do {
                timeoutHappened = false;
                try {
                    checkRet = parallelCheck(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec, batchSize);
                } catch (Throwable e) {
                    //rollback task info
                    FastCheckerThreadPool.getInstance().rollbackCheckTaskInfo(
                        baseEc.getDdlJobId(),
                        this.phyTaskSum.get(),
                        this.phyTaskFinished.get()
                    );

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
            } catch (Throwable e) {
                //rollback task info
                FastCheckerThreadPool.getInstance()
                    .rollbackCheckTaskInfo(baseEc.getDdlJobId(), this.phyTaskSum.get(), this.phyTaskFinished.get());

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
        return tsoCheckResult;
    }

    protected boolean tsoCheck(ExecutionContext baseEc, long batchSize) {
        ExecutionContext tsoEc = baseEc.copy();
        //set trx isolation: RR
        tsoEc.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        //set share readView
        tsoEc.setShareReadView(true);
        //socket timeout unit: ms
        ParamManager.setVal(
            tsoEc.getParamManager().getProps(),
            ConnectionParams.SOCKET_TIMEOUT,
            Integer.toString(1000 * 60 * 60 * 24 * 7),
            true
        );
        boolean tsoCheckResult = GsiUtils.wrapWithTransaction(tm, ITransactionPolicy.TSO, tsoEc, (ec) -> {
            /**
             * use idle query (select ... limit 1) for each phyDB so that DN can reserve TSO timestamp,
             * to prevent "TSO snapshot too old" when checking process is time consuming.
             * */
            idleQueryForEachPhyDb(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec);
            resendSnapshotTimestamp(ec, baseEc.getTraceId());
            return parallelCheck(this.srcPhyDbAndTables, this.dstPhyDbAndTables, ec, batchSize);
        });

        return tsoCheckResult;
    }

    private void resendSnapshotTimestamp(ExecutionContext ec, String traceId) {
        FailPoint.injectSuspendFromHint(FailPointKey.FP_FASTCHECKER_IDLE_QUERY_SLEEP, ec);
        if (ec.getParamManager().getBoolean(ConnectionParams.FASTCHECKER_RESEND_SNAPSHOT)
            && InstanceVersion.isMYSQL80()) {
            try {
                FailPoint.injectExceptionFromHint(FailPointKey.FP_FASTCHECKER_RESEND_SNAPSHOT_EXCEPTION, ec);
                ITransaction trx = ec.getTransaction();
                if (trx instanceof IMppTsoTransaction) {
                    ((IMppTsoTransaction) trx).resendSnapshotTimestamp();
                }
            } catch (Throwable e) {
                // Ignore error when resending tso, and hope checker will be finished within 30 minutes.
                SQLRecorderLogger.ddlLogger.warn(
                    MessageFormat.format("[{0}] FastChecker resend tso failed, caused by {1}",
                        traceId, e.getMessage()), e);
            }
        }
    }

    private void idleQueryForEachPhyDb(Map<String, Set<String>> srcDbAndTb,
                                       Map<String, Set<String>> dstDbAndTb,
                                       ExecutionContext baseEc) {
        Map<Pair<String, Boolean>, String> phyDbAndOneTable =
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
         * inorder to establish readView,
         * we only need to select * limit 1 on each phyDB.
         * */
        srcDbAndTb.forEach((phyDb, phyTables) -> {
            phyDbAndOneTable.put(Pair.of(phyDb, true), phyTables.stream().findFirst().get());
        });
        dstDbAndTb.forEach((phyDb, phyTables) -> {
            phyDbAndOneTable.put(Pair.of(phyDb, false), phyTables.stream().findFirst().get());
        });

        for (Map.Entry<Pair<String, Boolean>, String> entry : phyDbAndOneTable.entrySet()) {
            Pair<String, Boolean> phyDbInfoPair = entry.getKey();
            String phyTb = entry.getValue();
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            params.put(1, PlannerUtils.buildParameterContextForTableName(phyTb, 1));
            PhyTableOperation targetPhyOp =
                phyDbInfoPair.getValue() ? this.planIdleSelectSrc : this.planIdleSelectDst;

            PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
            buildParams.setGroupName(phyDbInfoPair.getKey());
            buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTb)));
            buildParams.setDynamicParams(params);

            PhyTableOperation plan =
                PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

            {
                Cursor cursor = null;
                try {
                    cursor = ExecutorHelper.executeByCursor(plan, baseEc, false);
                    while (cursor != null && cursor.next() != null) {
                    }
                } catch (Exception e) {
                    throw new TddlNestableRuntimeException(
                        String.format("FastChecker establish read view failed on group[%s]",
                            phyDbInfoPair.getKey()), e);
                } finally {
                    if (cursor != null) {
                        cursor.close(new ArrayList<>());
                    }
                }
            }
        }
    }

    protected Map<Pair<String, String>, List<Pair<String, String>>> constructDstToSrcPhyDbAndTablesMap(
        Map<Pair<String, String>, List<Pair<String, String>>> srcToDstPhyDbAndTablesMap) {
        Map<Pair<String, String>, List<Pair<String, String>>> targetToSourceTopologyMap = new TreeMap<>();
        if (srcToDstPhyDbAndTablesMap != null) {
            for (Pair<String, String> srcPhyDbPhyTb : srcToDstPhyDbAndTablesMap.keySet()) {
                for (Pair<String, String> targetPhyDbPhyTb : srcToDstPhyDbAndTablesMap.get(srcPhyDbPhyTb)) {
                    targetToSourceTopologyMap.put(targetPhyDbPhyTb, Lists.newArrayList(srcPhyDbPhyTb));
                }
            }
            return targetToSourceTopologyMap;
        } else {
            return null;
        }
//        Map<Pair<String, String>, List<Pair<String, String>>> resultMap = new TreeMap<>();
//        if (srcToDstPhyDbAndTablesMap != null) {
//            resultMap = srcToDstPhyDbAndTablesMap;
//            return resultMap;
//        }
//        Map<String, Pair<String, String>> srcTableToSrcPhyDbAndTableMap = new HashMap<>();
//        for (String srcPhyDb : srcPhyDbAndTables.keySet()) {
//            for (String srcPhyTable : srcPhyDbAndTables.get(srcPhyDb)) {
//                srcTableToSrcPhyDbAndTableMap.put(srcPhyTable, Pair.of(srcPhyDb, srcPhyTable));
//            }
//        }
//        for (String dstPhyDb : dstPhyDbAndTables.keySet()) {
//            for (String dstPhyTable : dstPhyDbAndTables.get(dstPhyDb)) {
//                resultMap.put(Pair.of(dstPhyDb, dstPhyTable),
//                    Arrays.asList(srcTableToSrcPhyDbAndTableMap.get(dstPhyTable)));
//            }
//        }
//        return resultMap;
    }

    protected boolean parallelCheck(Map<String, Set<String>> srcDbAndTb,
                                    Map<String, Set<String>> dstDbAndTb,
                                    ExecutionContext baseEc, long batchSize) {
        Boolean batchCheckerReport;
        Boolean internalCheck = baseEc.getParamManager().getBoolean(ConnectionParams.FASTCHECKER_BATCH_PARALLEL);
        Set<String> allGroups = new TreeSet<>(String::compareToIgnoreCase);
        allGroups.addAll(srcDbAndTb.keySet());
        allGroups.addAll(dstDbAndTb.keySet());

        //<GroupName, StorageInstId>
        // firstly initialize executors for insts
        FastCheckerThreadPool threadPool = FastCheckerThreadPool.getInstance();
        Map<String, String> mapping = queryStorageInstIdByPhyGroup(allGroups);
        List<String> storageInstNames = allGroups.stream().map(o -> mapping.get(o)).collect(Collectors.toList());
        AtomicInteger fastCheckSubTaskNum = new AtomicInteger(0);
        threadPool.initializeExecutorsForInsts(storageInstNames);

        Map<String, List<FutureTask<Pair<Long, Boolean>>>> allFutureTasksByGroup =
            new TreeMap<>(String::compareToIgnoreCase);

        Map<Runnable, CheckerBatch> taskToSrcDbTbBatchBoundListMap = new ConcurrentHashMap<>();
        Map<Runnable, CheckerBatch> taskToDstDbTbBatchBoundListMap = new ConcurrentHashMap<>();
        int srcTableTaskCount = 0, dstTableTaskCount = 0;
        final Map mdcContext = MDC.getCopyOfContextMap();
        LinkedBlockingQueue<FutureTask<Pair<Long, Boolean>>> allFutureTasks = new LinkedBlockingQueue<>();
        Map<Pair<String, String>, List<Map<Integer, ParameterContext>>> batchBoundListMap = new HashMap<>();
        Map<Pair<String, String>, List<Pair<String, String>>> topologyMap =
            constructDstToSrcPhyDbAndTablesMap(srcToDstPhyDbAndTablesMap);
        SQLRecorderLogger.ddlLogger.warn("the topology map used by checker is " + JSON.toJSONString(topologyMap));
        batchCheckerReport = GeneralUtil.isNotEmpty(topologyMap);
//        if (!GeneralUtil.isEmpty(topologyMap)){
//            batchCheckerReport = true;
////            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
////                " expected mirror check, but get no mirror check list");
//        } else {
//            batchCheckerReport = false;
//        }

        // sample from src table
        for (Map.Entry<String, Set<String>> entry : srcDbAndTb.entrySet()) {
            String srcDb = entry.getKey();
            for (String srcTb : entry.getValue()) {
                List<Map<Integer, ParameterContext>> batchBoundList = sampleForBatchBoundList(srcDb, srcTb,
                    baseEc, true, batchSize);
                batchBoundListMap.put(Pair.of(srcDb, srcTb), batchBoundList);
            }
        }
        // src table
        for (Map.Entry<String, Set<String>> entry : srcDbAndTb.entrySet()) {
            String srcDb = entry.getKey();
            for (String srcTb : entry.getValue()) {
                // for which we need to firstly put all into checker task.
                FutureTask<Pair<Long, Boolean>> task = new FutureTask<>(
                    () -> {
                        MDC.setContextMap(mdcContext);
                        List<Map<Integer, ParameterContext>> batchBoundList =
                            batchBoundListMap.get(Pair.of(srcDb, srcTb));
                        String instName = mapping.get(srcDb);
                        return hashCheckForSinglePhyTable(srcDb, srcTb, baseEc, true, batchSize, threadPool, mdcContext,
                            instName, allFutureTasks, fastCheckSubTaskNum,
                            batchBoundList, taskToSrcDbTbBatchBoundListMap);
                    }
                );
                taskToDstDbTbBatchBoundListMap.put(task, new CheckerBatch(srcDb, srcTb, -1, true, null, false, false));
                allFutureTasksByGroup.computeIfAbsent(srcDb, k -> new ArrayList<>()).add(task);
                srcTableTaskCount++;
            }
        }

        if (!batchCheckerReport) {
            // sample from dst table
            for (Map.Entry<String, Set<String>> entry : dstDbAndTb.entrySet()) {
                String dstDb = entry.getKey();
                for (String dstTb : entry.getValue()) {
                    List<Map<Integer, ParameterContext>> batchBoundList = sampleForBatchBoundList(dstDb, dstTb,
                        baseEc, false, batchSize);
                    batchBoundListMap.put(Pair.of(dstDb, dstTb), batchBoundList);
                }
            }

        }
        for (Map.Entry<String, Set<String>> entry : dstDbAndTb.entrySet()) {
            String dstDb = entry.getKey();
            for (String dstTb : entry.getValue()) {
                FutureTask<Pair<Long, Boolean>> task = new FutureTask<>(
                    () -> {
                        MDC.setContextMap(mdcContext);
                        List<Map<Integer, ParameterContext>> batchBoundList = batchCheckerReport ?
                            batchBoundListMap.get(topologyMap.get(Pair.of(dstDb, dstTb)).get(0)) :
                            batchBoundListMap.get(Pair.of(dstDb, dstTb));
                        String instName = mapping.get(dstDb);
                        return hashCheckForSinglePhyTable(dstDb, dstTb, baseEc, false, batchSize, threadPool,
                            mdcContext, instName, allFutureTasks, fastCheckSubTaskNum,
                            batchBoundList, taskToDstDbTbBatchBoundListMap);
                    }
                );
                taskToDstDbTbBatchBoundListMap.put(task, new CheckerBatch(dstDb, dstTb, -1, false, null, false, false));
                allFutureTasksByGroup.computeIfAbsent(dstDb, k -> new ArrayList<>()).add(task);
                dstTableTaskCount++;
            }
        }

        SQLRecorderLogger.ddlLogger.info(
            MessageFormat.format(
                "[{0}] FastChecker try to submit {1} tasks to fastChecker threadPool",
                baseEc.getTraceId(),
                srcTableTaskCount + dstTableTaskCount
            )
        );

        //update task info
        this.phyTaskSum.set(srcTableTaskCount + dstTableTaskCount);

        FastCheckerThreadPool.getInstance().increaseCheckTaskInfo(
            baseEc.getDdlJobId(),
            this.phyTaskSum.get(),
            0
        );

        List<Pair<String, Runnable>> allTasksByStorageInstId = new ArrayList<>();
        for (Map.Entry<String, List<FutureTask<Pair<Long, Boolean>>>> entry : allFutureTasksByGroup.entrySet()) {
            String groupName = entry.getKey();
            if (!mapping.containsKey(groupName)) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_FAST_CHECKER,
                    String.format("FastChecker failed to get group-storageInstId mapping, group [%s]", groupName)
                );
            }
            String storageInstId = mapping.get(groupName);
            for (FutureTask<Pair<Long, Boolean>> task : entry.getValue()) {
                allTasksByStorageInstId.add(Pair.of(storageInstId, task));
            }
        }

        fastCheckSubTaskNum.getAndAdd(allTasksByStorageInstId.size());
        threadPool.submitTasks(allTasksByStorageInstId);

        Map<CheckerBatch, Pair<Long, Boolean>> result = new HashMap<>();
        allFutureTasks.addAll(allTasksByStorageInstId
            .stream()
            .map(Pair::getValue)
            .map(task -> (FutureTask<Pair<Long, Boolean>>) task)
            .collect(Collectors.toList()));

        while (fastCheckSubTaskNum.get() > 0) {
            while (!allFutureTasks.isEmpty()) {
                FutureTask<Pair<Long, Boolean>> futureTask = allFutureTasks.poll();
                try {
                    if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
                        long jobId = baseEc.getDdlJobId();
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "The job '" + jobId + "' has been cancelled");
                    }
                    Pair<Long, Boolean> checkResult = futureTask.get();
                    if (taskToSrcDbTbBatchBoundListMap.containsKey(futureTask)) {
                        result.put(taskToSrcDbTbBatchBoundListMap.get(futureTask), checkResult);
                    } else if (taskToDstDbTbBatchBoundListMap.containsKey(futureTask)) {
                        result.put(taskToDstDbTbBatchBoundListMap.get(futureTask), checkResult);
                    }
                    fastCheckSubTaskNum.decrementAndGet();
//                    allFutureTasks.r(futureTask);
                } catch (Exception e) {
                    for (FutureTask<Pair<Long, Boolean>> taskToBeCancel : allFutureTasks) {
                        try {
                            taskToBeCancel.cancel(true);
                        } catch (Exception ignore) {
                        }
                    }
                    if (e.getMessage().toLowerCase().contains("XResult stream fetch result timeout".toLowerCase())) {
                        throw new TddlNestableRuntimeException("FastChecker fetch phy table digest timeout", e);
                    } else {
                        throw new TddlNestableRuntimeException(e);
                    }
                }
            }
        }

        if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
            long jobId = baseEc.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        if (internalCheck && batchCheckerReport) {
            return compareBatchResult(topologyMap, result, baseEc);
        } else {
            List<Pair<Long, Boolean>> resultValue = result.values().stream().collect(Collectors.toList());
            List<Long> srcResult =
                resultValue.stream().filter(p -> p != null && p.getKey() != null && p.getValue()).map(Pair::getKey)
                    .collect(Collectors.toList());
            List<Long> dstResult =
                resultValue.stream().filter(p -> p != null && p.getKey() != null && !p.getValue()).map(Pair::getKey)
                    .collect(Collectors.toList());
            if (internalCheck) {
                return compare(srcResult, dstResult);
            } else {
                return srcTableTaskCount == resultValue.stream().filter(Objects::nonNull).filter(Pair::getValue).count()
                    && dstTableTaskCount == resultValue.stream().filter(Objects::nonNull).filter(x -> !x.getValue())
                    .count() && compare(srcResult, dstResult);
            }
        }
    }

    private boolean checkBatch(CheckerBatch sourceBatch, CheckerBatch targetBatch, ExecutionContext baseEc) {
        Cursor extractCursor = null;
        // Transform
        final long batchSize =
            baseEc.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_BATCH_SIZE);
        final long speedLimit =
            baseEc.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_LIMITATION);
        final long speedMin =
            baseEc.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_SPEED_MIN);
        final long parallelism =
            baseEc.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_CHECK_PARALLELISM);
        final long earlyFailNumber =
            baseEc.getParamManager().getLong(ConnectionParams.TABLEGROUP_REORG_EARLY_FAIL_NUMBER);
        final boolean useBinary = baseEc.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);
        AlterTableGroupBatchChecker checker = AlterTableGroupBatchChecker.create(this.srcSchemaName,
            this.srcLogicalTableName,
            this.srcLogicalTableName,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            SqlSelect.LockMode.UNDEF,
            SqlSelect.LockMode.UNDEF,
            baseEc,
            sourceBatch.getPhyTables(),
            targetBatch.getPhyTables(),
            sourceBatch,
            targetBatch
        );
        checker.setJobId(baseEc.getDdlJobId());
        final AlterTableGroupReporter reporter = new AlterTableGroupReporter(earlyFailNumber);
        reporter.setReportName(FAST_CHECKER_REPORT_NAME);
        reporter.setForceReportIntoMetaDb(true);
        checker.check(baseEc, reporter);
        return true;
    }

    private boolean compareBatchResult(Map<Pair<String, String>, List<Pair<String, String>>> topologyMap,
                                       Map<CheckerBatch, Pair<Long, Boolean>> result, ExecutionContext baseEc) {
        Boolean compareResult = true;
        Boolean detailedErrorReport = baseEc.getParamManager().getBoolean(ConnectionParams.FASTCHECKER_ERROR_REPORT);
        int maxBatchRechecked = DynamicConfig.getInstance().getFastCheckerMaxRecheckBatch();
        String traceId = baseEc.getTraceId();
        List<Pair<Pair<String, String>, String>> batchErrorReport = new ArrayList<>();
        Set<Pair<String, String>> unvisitedTopology = new TreeSet<>(topologyMap.keySet());
        List<Pair<CheckerBatch, CheckerBatch>> batchPairList = new ArrayList<>();
        for (CheckerBatch targetBatch : result.keySet()) {
            if (!targetBatch.isSourceTable) {
                Pair<Long, Boolean> targetBatchResult = result.get(targetBatch);
                // two cases are:
                // null indicate no data.
                // null indicate no check(physical table level)
                // since this is mirror batch check, for both two case, we can use Objects.equals to compare.
                if (!targetBatchResult.getValue()) {
                    CheckerBatch sourceBatch =
                        CheckerBatch.buildSourceBatchFromTargetBatch(targetBatch, topologyMap);
                    unvisitedTopology.remove(Pair.of(targetBatch.phyDb, targetBatch.phyTb));
                    Pair<Long, Boolean> sourceBatchResult = result.get(sourceBatch);
                    if (sourceBatchResult == null || !Objects.equals(sourceBatchResult.getKey()
                        , targetBatchResult.getKey())) {
                        compareResult = false;
                        Pair<Pair<String, String>, String> batchReport =
                            CheckerBatch.buildBatchReport(sourceBatch, targetBatch, sourceBatchResult,
                                targetBatchResult);
                        batchPairList.add(Pair.of(sourceBatch, targetBatch));
                        batchErrorReport.add(batchReport);
                    }
                }
            }
        }
        if (unvisitedTopology.size() > 0) {
            compareResult = false;
            for (Pair<String, String> topology : unvisitedTopology) {
                String errMsg =
                    MessageFormat.format(
                        "[{0}] FastChecker failed to found any checker result for [%s.%s]",
                        traceId, topology.getKey(), topology.getValue()
                    );
                batchErrorReport.add(Pair.of(topology, errMsg));
                SQLRecorderLogger.ddlLogger.error(errMsg);
            }
        }
        if (!batchErrorReport.isEmpty()) {
            reportCheckError(baseEc, batchErrorReport);
        }
        if (detailedErrorReport && maxBatchRechecked > 0) {
            for (int i = 0; i < batchPairList.size() && i < maxBatchRechecked; i++) {
                CheckerBatch srcBatch = batchPairList.get(i).getKey();
                CheckerBatch dstBatch = batchPairList.get(i).getValue();
                checkBatch(srcBatch, dstBatch, baseEc);
            }
        }
        return compareResult;
    }

    private boolean compare(List<Long> src, List<Long> dst) {
        final HashCalculator srcCalculator = new HashCalculator();
        final HashCalculator dstCalculator = new HashCalculator();
        src.forEach(srcCalculator::calculate);
        dst.forEach(dstCalculator::calculate);
        return srcCalculator.getHashVal().equals(dstCalculator.getHashVal());
    }

    public void reportCheckOk(ExecutionContext ec) {
        final CheckerManager checkerManager = new CheckerManager(srcSchemaName);
        final String finishDetails = "FastChecker check OK.";
        checkerManager.insertReports(ImmutableList.of(
            new CheckerManager.CheckerReport(-1, ec.getDdlJobId(), srcSchemaName, srcLogicalTableName,
                dstSchemaName,
                dstLogicalTableName, "", "", "SUMMARY",
                new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                CheckerManager.CheckerReportStatus.FINISH.getValue(), "--", finishDetails, "Reporter.", null)));
    }

    public void reportCheckError(ExecutionContext ec, List<Pair<Pair<String, String>, String>> batchErrorReport) {
        final CheckerManager checkerManager = new CheckerManager(srcSchemaName);
        List<CheckerManager.CheckerReport> reportLists = new ArrayList<>();
        for (int i = 0; i < batchErrorReport.size(); i++) {
            Pair<String, String> sourcePhyDbAndTable = batchErrorReport.get(i).getKey();
            String phyDb = sourcePhyDbAndTable.getKey();
            CheckerManager.CheckerReport report =
                new CheckerManager.CheckerReport(-1, ec.getDdlJobId(), srcSchemaName, srcLogicalTableName,
                    dstSchemaName,
                    dstLogicalTableName, phyDb, sourcePhyDbAndTable.getValue(), "CONFLICT",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
                    CheckerManager.CheckerReportStatus.FINISH.getValue(), "--",
                    batchErrorReport.get(i).getValue(), FAST_CHECKER_REPORT_NAME, null);
            reportLists.add(report);
        }
        checkerManager.insertReports(reportLists);
    }

    public class HashCalculator {
        private final Long p;
        private final Long q;
        private final Long r;

        private Long hashVal;
        private boolean firstCaculate;

        //p q r should be same as p q r in DN
        public HashCalculator() {
            p = 3860031L;
            q = 2779L;
            r = 2L;
            hashVal = 0L;
            firstCaculate = true;
        }

        public Long calculate(Long elem) {
            if (elem == null) {
                return hashVal;
            }
            if (firstCaculate) {
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

    public void setDstPhyDbAndTables(Map<String, Set<String>> dstPhyDbAndTables) {
        this.dstPhyDbAndTables = dstPhyDbAndTables;
    }

    public void setSrcPhyDbAndTables(Map<String, Set<String>> srcPhyDbAndTables) {
        this.srcPhyDbAndTables = srcPhyDbAndTables;
    }

    public static List<String> getOrderedPrimaryKeys(TableMeta tableMeta) {
        return ImmutableList
            .copyOf(
                (tableMeta.isHasPrimaryKey() ? tableMeta.getPrimaryIndex().getKeyColumns() :
                    new ArrayList<ColumnMeta>())
                    .stream().map(ColumnMeta::getName).collect(Collectors.toList())
            );
    }

    /**
     * map < groupName, storageInstId >
     */
    protected Map<String, String> queryStorageInstIdByPhyGroup(Set<String> groupName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            return groupDetailInfoAccessor.getStorageInstMappingByOnlyGroupName(
                ServerInstIdManager.getInstance().getMasterInstId(),
                groupName
            );
        } catch (Exception e) {
            throw new TddlNestableRuntimeException("FastChecker query group-storageInstId info failed", e);
        }
    }
}
