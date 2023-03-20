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

package com.alibaba.polardbx.executor.backfill;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.workqueue.PriorityFIFOTask;
import com.alibaba.polardbx.executor.workqueue.PriorityWorkQueue;
import com.alibaba.polardbx.gms.partition.BackfillExtraFieldJSON;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
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
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.OptimizerHint;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.alibaba.polardbx.ErrorCode.ER_LOCK_DEADLOCK;
import static com.alibaba.polardbx.ErrorCode.ER_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.gsi.GsiBackfillManager.BackfillStatus.UNFINISHED;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_COUNT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_LOCK_TIMEOUT;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_RANDOM_BACKFILL_EXCEPTION;
import static com.alibaba.polardbx.gms.partition.BackfillExtraFieldJSON.isNotEmpty;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class Extractor extends PhyOperationBuilderCommon {

    protected final String schemaName;
    protected final String sourceTableName;
    private final String targetTableName;
    protected final long batchSize;
    protected volatile RateLimiter rateLimiter;
    protected volatile long nowSpeedLimit;
    protected final long parallelism;
    protected final GsiBackfillManager backfillManager;

    /* Templates, built once, used every time. */
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    protected final PhyTableOperation planSelectWithMax;
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    protected final PhyTableOperation planSelectWithMin;
    /**
     * <pre>
     * SELECT {all_columns_exists_in_index_table}
     * FROM {physical_primary_table}
     * WHERE (pk0, ... , pkn) > (?, ... , ?) => DNF
     *   AND (pk0, ... , pkn) <= (?, ... , ?) => DNF
     * ORDER BY pk0, ... , pkn
     * LIMIT ?
     * LOCK IN SHARE MODE
     * </pre>
     */
    protected final PhyTableOperation planSelectWithMinAndMax;
    /**
     * <pre>
     * SELECT IFNULL(MAX(pk0), 0), ... , IFNULL(MAX(pkn), 0)
     * FROM (
     *  SELECT pk0, ... , pkn
     *  FROM {physical_table}
     *  ORDER BY pk0 DESC, ... , pkn DESC LIMIT 1
     * ) T1
     * </pre>
     */
    protected final PhyTableOperation planSelectMaxPk;

    private final PhyTableOperation planSelectSample;

    private final PhyTableOperation planSelectMinAndMaxSample;

    private boolean needBuildSubBoundList = true;

    private Map<String, List<Map<Integer, ParameterContext>>> backfillSubBoundList = new HashMap<>();

    protected final List<Integer> primaryKeysId;
    /**
     * map the column id to primary key order
     * E.G. the 2-nd column is the 1-st key in primary key list, thus there is an entry {2}->{1}
     */
    protected final Map<Long, Long> primaryKeysIdMap;

    protected final ITransactionManager tm;

    protected final Reporter reporter;
    protected com.alibaba.polardbx.executor.backfill.Throttle throttle;

    static private final Integer maxRandomInterval = 10000;

    protected Extractor(String schemaName, String sourceTableName, String targetTableName, long batchSize,
                        long speedMin,
                        long speedLimit,
                        long parallelism,
                        PhyTableOperation planSelectWithMax, PhyTableOperation planSelectWithMin,
                        PhyTableOperation planSelectWithMinAndMax,
                        PhyTableOperation planSelectMaxPk,
                        PhyTableOperation planSelectSample,
                        PhyTableOperation planSelectMinAndMaxSample,
                        List<Integer> primaryKeysId) {
        this.schemaName = schemaName;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.batchSize = batchSize;
        // Use RateLimiter in spark_project, because RateLimiter have some unknown bug in Guava.
        this.rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        this.nowSpeedLimit = speedLimit;
        this.parallelism = parallelism;
        this.planSelectWithMax = planSelectWithMax;
        this.planSelectWithMin = planSelectWithMin;
        this.planSelectWithMinAndMax = planSelectWithMinAndMax;
        this.planSelectMaxPk = planSelectMaxPk;
        this.planSelectSample = planSelectSample;
        this.planSelectMinAndMaxSample = planSelectMinAndMaxSample;
        //this.primaryKeys = primaryKeys;
        this.primaryKeysId = primaryKeysId;
        this.primaryKeysIdMap = new HashMap<>();
        for (int i = 0; i < primaryKeysId.size(); i++) {
            primaryKeysIdMap.put((long) primaryKeysId.get(i), (long) i);
        }
        this.tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        this.backfillManager = new GsiBackfillManager(schemaName);
        this.reporter = new Reporter(backfillManager);
        this.throttle = new com.alibaba.polardbx.executor.backfill.Throttle(speedMin, speedLimit, schemaName);
    }

    /**
     * Load latest position mark
     *
     * @param ec Id of parent DDL job
     * @return this
     */
    public Extractor loadBackfillMeta(ExecutionContext ec) {
        Long backfillId = ec.getBackfillId();
        throttle.setBackFillId(backfillId);

        final String positionMarkHint = ec.getParamManager().getString(ConnectionParams.GSI_BACKFILL_POSITION_MARK);

        if (TStringUtil.isEmpty(positionMarkHint)) {
            // Init position mark with upper bound
            final List<GsiBackfillManager.BackfillObjectRecord> initBfoList = initAllUpperBound(ec, backfillId);

            // Insert ignore
            backfillManager.initBackfillMeta(ec, initBfoList);
        } else {
            // Load position mark from HINT
            final List<GsiBackfillManager.BackfillObjectRecord> backfillObjects = JSON
                .parseArray(StringEscapeUtils.unescapeJava(positionMarkHint),
                    GsiBackfillManager.BackfillObjectRecord.class);

            // Insert ignore
            backfillManager
                .initBackfillMeta(ec, backfillId, schemaName, sourceTableName, targetTableName, backfillObjects);
        }

        // Load from system table
        this.reporter.loadBackfillMeta(backfillId);
        SQLRecorderLogger.ddlLogger.info(
            String.format("loadBackfillMeta for backfillId %d: %s", backfillId, this.reporter.getBackfillBean()));
        return this;
    }

    /**
     * Init upper bound for all physical primary table
     *
     * @param ec Execution context
     * @param ddlJobId Ddl job id
     * @return BackfillObjectRecord with upper bound initialized, one for each physical table and primary key
     */
    protected List<GsiBackfillManager.BackfillObjectRecord> initAllUpperBound(ExecutionContext ec, long ddlJobId) {

        return getSourcePhyTables().entrySet()
            .stream()
            .flatMap(e -> e.getValue()
                .stream()
                .flatMap(
                    phyTable -> splitAndInitUpperBound(ec, ddlJobId, e.getKey(), phyTable, primaryKeysId).stream()))
            .collect(Collectors.toList());
    }

    protected List<Map<Integer, ParameterContext>> executePhysicalPlan(final ExecutionContext baseEc,
                                                                       RelNode plan) {

        return GsiUtils.wrapWithSingleDbTrx(tm,
            baseEc,
            (ec) -> {
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
    }

    private long getTableRowsCount(final String dbIndex, final String phyTable) {
        String dbIndexWithoutGroup = GroupInfoUtil.buildPhysicalDbNameFromGroupName(dbIndex);
        List<List<Object>> phyDb = StatsUtils.queryGroupByPhyDb(schemaName, dbIndexWithoutGroup, "select database();");
        if (GeneralUtil.isEmpty(phyDb) || GeneralUtil.isEmpty(phyDb.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("group %s can not find physical db", dbIndex));
        }

        String phyDbName = String.valueOf(phyDb.get(0).get(0));
        String rowsCountSQL = StatsUtils.genTableRowsCountSQL(phyDbName, phyTable);
        List<List<Object>> result = StatsUtils.queryGroupByPhyDb(schemaName, dbIndexWithoutGroup, rowsCountSQL);
        if (GeneralUtil.isEmpty(result) || GeneralUtil.isEmpty(result.get(0))) {
            throw new TddlRuntimeException(ErrorCode.ERR_BACKFILL_GET_TABLE_ROWS,
                String.format("db %s can not find table %s", phyDbName, phyTable));
        }

        return Long.parseLong(String.valueOf(result.get(0).get(0)));
    }

    protected List<GsiBackfillManager.BackfillObjectRecord> splitAndInitUpperBound(final ExecutionContext baseEc,
                                                                                   final long ddlJobId,
                                                                                   final String dbIndex,
                                                                                   final String phyTable,
                                                                                   final List<Integer> primaryKeysId) {
        if (!baseEc.getParamManager().getBoolean(ConnectionParams.ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL)) {
            return initUpperBound(baseEc, ddlJobId, dbIndex, phyTable, primaryKeysId);
        }

        boolean enableInnodbBtreeSampling = OptimizerContext.getContext(schemaName).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);
        if (!enableInnodbBtreeSampling) {
            return initUpperBound(baseEc, ddlJobId, dbIndex, phyTable, primaryKeysId);
        }

        int splitCount =
            baseEc.getParamManager().getInt(ConnectionParams.PHYSICAL_TABLE_BACKFILL_PARALLELISM);
        long maxPhyTableRowCount =
            baseEc.getParamManager().getLong(ConnectionParams.PHYSICAL_TABLE_START_SPLIT_SIZE);
        long maxSampleSize =
            baseEc.getParamManager().getLong(ConnectionParams.BACKFILL_MAX_SAMPLE_SIZE);
        float samplePercentage =
            baseEc.getParamManager().getFloat(ConnectionParams.BACKFILL_MAX_SAMPLE_PERCENTAGE);

        long rowCount = getTableRowsCount(dbIndex, phyTable);
        // judge need split
        if (rowCount < maxPhyTableRowCount || splitCount <= 1) {
            return initUpperBound(baseEc, ddlJobId, dbIndex, phyTable, primaryKeysId);
        }

        float calSamplePercentage = maxSampleSize * 1.0f / rowCount * 100;

        if (calSamplePercentage <= 0 || calSamplePercentage > samplePercentage) {
            calSamplePercentage = samplePercentage;
        }

        PhyTableOperation plan =
            buildSamplePlanWithParam(dbIndex, phyTable, new ArrayList<>(), calSamplePercentage, false, false);

        // Execute query
        final List<Map<Integer, ParameterContext>> resultList = executePhysicalPlan(baseEc, plan);

        List<Map<Integer, ParameterContext>> upperBoundList = new ArrayList<>();
        List<List<Map<Integer, ParameterContext>>> subUpperBoundList = new ArrayList<>();
        final List<Map<Integer, ParameterContext>> upperBound = getUpperBound(baseEc, dbIndex, phyTable);

        // step must not less than zero
        int step = resultList.size() / splitCount;
        if (step <= 0) {
            return null;
        }

        int subStep = step / splitCount;
        if (subStep > 0 && needBuildSubBoundList) {
            for (int i = 0; i < splitCount; ++i) {
                int lastIndex = i * step;
                int index = Math.min(resultList.size() - 1, (i + 1) * step);
                List<Map<Integer, ParameterContext>> subList = resultList.subList(lastIndex, index + 1);

                List<Map<Integer, ParameterContext>> subBound = new ArrayList<>();
                IntStream.range(1, splitCount)
                    .mapToObj(j -> subList.get(Math.min(subList.size() - 1, (j + 1) * subStep)))
                    .forEach(subBound::add);

                // 替换最大边界
                subBound.remove(subBound.size() - 1);
                if (i == splitCount - 1) {
                    subBound.addAll(upperBound);
                } else {
                    subBound.add(subList.get(subList.size() - 1));
                }

                subUpperBoundList.add(subBound);
            }
        }

        IntStream.range(0, splitCount)
            .mapToObj(i -> resultList.get(Math.min(resultList.size() - 1, (i + 1) * step)))
            .forEach(upperBoundList::add);
        upperBoundList.remove(upperBoundList.size() - 1);
        upperBoundList.addAll(upperBound);

        return genBackfillObjectRecordByUpperBound(ddlJobId, dbIndex, phyTable, null, upperBoundList,
            subUpperBoundList, rowCount / splitCount, 1);
    }

    private Map<Integer, ParameterContext> convertBoundParamMap(List<ParameterContext> param) {
        Map<Integer, ParameterContext> ret = new HashMap<>();
        if (param == null || param.isEmpty()) {
            return ret;
        }

        for (int i = 0; i < param.size(); ++i) {
            ret.put(i + 1, param.get(i));
        }
        return ret;
    }

    private List<Map<Integer, ParameterContext>> calculateSubUpperBound(List<Map<Integer, ParameterContext>> subBound,
                                                                        List<ParameterContext> lowerBoundParam) {
        if (subBound == null || subBound.isEmpty()) {
            return null;
        }

        int j = 0;
        try {
            for (Map<Integer, ParameterContext> bound : subBound) {
                List<ParameterContext> tmpParam = new ArrayList<>(bound.values());

                boolean lessOrEqual = true;
                for (int i = 0; i < tmpParam.size(); ++i) {
                    final Object arg1 = tmpParam.get(i).getArgs()[1];
                    final Object arg2 = lowerBoundParam.get(i).getArgs()[1];

                    final DataType type = DataTypeUtil.getTypeOfObject(arg1);

                    if (DataTypeUtil.isNumberSqlType(type) || DataTypeUtil
                        .anyMatchSemantically((DataType) tmpParam.get(i).getArgs()[2], DataTypes.ULongType)) {
                        final BigDecimal current = DataTypes.DecimalType.convertFrom(arg1).toBigDecimal();
                        final BigDecimal lastValue = DataTypes.DecimalType.convertFrom(arg2).toBigDecimal();
                        if (lastValue.compareTo(current) == 0) {
                            continue;
                        }

                        if (lastValue.compareTo(current) < 0) {
                            break;
                        }

                        if (lastValue.compareTo(current) > 0) {
                            lessOrEqual = false;
                            break;
                        }
                    } else {
                        return null;
                    }
                }

                if (lessOrEqual) {
                    break;
                }
                j++;
            }
        } catch (Exception err) {
            return null;
        }

        return subBound.subList(j, subBound.size());
    }

    protected List<GsiBackfillManager.BackfillObjectRecord> splitPhysicalBatch(final ExecutionContext ec, Long rows,
                                                                               String dbIndex, String phyTable,
                                                                               List<GsiBackfillManager.BackfillObjectBean> backfillObjects) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);
        // get lowerBound and upperBound
        List<ParameterContext> upperBoundParam =
            buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        List<ParameterContext> lowerBoundParam = initSelectParam(backfillObjects, primaryKeysIdMap);

        // calculate split point
        int splitCount =
            ec.getParamManager().getInt(ConnectionParams.SLIDE_WINDOW_SPLIT_SIZE);
        long maxSampleSize =
            ec.getParamManager().getLong(ConnectionParams.BACKFILL_MAX_SAMPLE_SIZE);
        float samplePercentage =
            ec.getParamManager().getFloat(ConnectionParams.BACKFILL_MAX_SAMPLE_PERCENTAGE);

        float calSamplePercentage = maxSampleSize * 1.0f / rows * 100;

        if (calSamplePercentage <= 0 || calSamplePercentage > samplePercentage) {
            calSamplePercentage = samplePercentage;
        }

        List<Map<Integer, ParameterContext>> upperBoundList = new ArrayList<>();
        List<List<Map<Integer, ParameterContext>>> subUpperBoundList = new ArrayList<>();
        boolean notSplit = backfillObjects.get(0).extra.getSplitLevel() == null;
        if (notSplit) {
            PhyTableOperation plan = buildSamplePlanWithParam(dbIndex, physicalTableName,
                new ArrayList<>(), calSamplePercentage, false, false
            );

            // Execute query
            final List<Map<Integer, ParameterContext>> sampleResult = executePhysicalPlan(ec, plan);

            // step must not less than zero
            int step = sampleResult.size() / splitCount;
            if (step <= 0) {
                return null;
            }

            int subStep = step / splitCount;
            if (subStep > 0) {
                for (int i = 0; i < splitCount; ++i) {
                    int lastIndex = i * step;
                    int index = Math.min(sampleResult.size() - 1, (i + 1) * step);
                    List<Map<Integer, ParameterContext>> subList = sampleResult.subList(lastIndex, index + 1);

                    List<Map<Integer, ParameterContext>> subBound = new ArrayList<>();
                    IntStream.range(1, splitCount)
                        .mapToObj(j -> subList.get(Math.min(subList.size() - 1, (j + 1) * subStep)))
                        .forEach(subBound::add);

                    // 替换最大边界
                    subBound.remove(subBound.size() - 1);
                    if (i == splitCount - 1) {
                        subBound.add(convertBoundParamMap(upperBoundParam));
                    } else {
                        subBound.add(subList.get(subList.size() - 1));
                    }

                    subUpperBoundList.add(subBound);
                }
            }

            IntStream.range(0, splitCount)
                .mapToObj(i -> sampleResult.get(Math.min(sampleResult.size() - 1, (i + 1) * step)))
                .forEach(upperBoundList::add);
            upperBoundList.remove(upperBoundList.size() - 1);
            upperBoundList.add(convertBoundParamMap(upperBoundParam));
        } else {
            if (backfillSubBoundList.isEmpty()) {
                return null;
            }

            List<Map<Integer, ParameterContext>> subBound = backfillSubBoundList.get(phyTable);
            // adjust subUpperBound
            upperBoundList = calculateSubUpperBound(subBound, lowerBoundParam);
            // clean
            backfillSubBoundList.remove(phyTable);

            if (upperBoundList == null || upperBoundList.isEmpty() || upperBoundList.size() == 1) {
                return null;
            }
            splitCount = upperBoundList.size();
        }

        long ddlJobId = backfillObjects.get(0).jobId;
        // return new backfill object record

        int splitLevel = 1;
        if (backfillObjects.get(0).extra.getSplitLevel() != null) {
            splitLevel += Integer.parseInt(backfillObjects.get(0).extra.getSplitLevel());
        }
        return genBackfillObjectRecordByUpperBound(ddlJobId, dbIndex, phyTable, convertBoundParamMap(lowerBoundParam),
            upperBoundList, subUpperBoundList, rows / splitCount, splitLevel);
    }

    private List<GsiBackfillManager.BackfillObjectRecord> genBackfillObjectRecordByUpperBound(final long ddlJobId,
                                                                                              final String dbIndex,
                                                                                              final String physicalTableName,
                                                                                              Map<Integer, ParameterContext> lowerBound,
                                                                                              List<Map<Integer, ParameterContext>> upperBoundList,
                                                                                              List<List<Map<Integer, ParameterContext>>> subUpperBoundList,
                                                                                              long approximateRowCount,
                                                                                              int splitLevel) {
        List<GsiBackfillManager.BackfillObjectRecord> upperBoundRecords = new ArrayList<>();

        // Convert to BackfillObjectRecord
        Map<Integer, ParameterContext> lastItem = lowerBound;
        int i = 0;
        for (Map<Integer, ParameterContext> item : upperBoundList) {
            final AtomicInteger srcIndex = new AtomicInteger(0);
            final String suffix = "_%" + String.format("%02x", i++);
            final String name = physicalTableName + suffix;

            if (subUpperBoundList != null && !subUpperBoundList.isEmpty() && subUpperBoundList.size() > i - 1) {
                this.backfillSubBoundList.put(name, subUpperBoundList.get(i - 1));
            }

            Map<Integer, ParameterContext> finalLastItem = lastItem;
            upperBoundRecords.addAll(
                primaryKeysId.stream().mapToInt(columnIndex -> columnIndex).mapToObj(columnIndex -> {
                    final ParameterContext pc = item.get(srcIndex.getAndIncrement() + 1);
                    ParameterContext lastPc = null;
                    if (finalLastItem != null) {
                        lastPc = finalLastItem.get(srcIndex.get());
                    }
                    BackfillExtraFieldJSON extra = new BackfillExtraFieldJSON();
                    extra.setApproximateRowCount(String.valueOf(approximateRowCount));
                    extra.setSplitLevel(String.valueOf(splitLevel));

                    return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                        schemaName,
                        sourceTableName,
                        targetTableName,
                        dbIndex,
                        name,
                        columnIndex,
                        pc.getParameterMethod().name(),
                        lastPc == null ? null :
                            com.alibaba.polardbx.executor.gsi.utils.Transformer.serializeParam(lastPc),
                        com.alibaba.polardbx.executor.gsi.utils.Transformer.serializeParam(pc),
                        BackfillExtraFieldJSON.toJson(extra));
                }).collect(Collectors.toList()));

            lastItem = item;
        }

        return upperBoundRecords;
    }

    private List<Map<Integer, ParameterContext>> getUpperBound(final ExecutionContext baseEc,
                                                               final String dbIndex, final String phyTable) {
        // Build parameter
        final Map<Integer, ParameterContext> params = new HashMap<>(1);
        params.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(params);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(this.planSelectMaxPk, buildParams);

        // Execute query
        return executePhysicalPlan(baseEc, plan);
    }

    /**
     * Get max primary key value from physical table
     *
     * @param baseEc Execution context
     * @param ddlJobId Ddl job id
     * @param dbIndex Group key
     * @param phyTable Physical table name
     * @param primaryKeysId Index of primary keys for ResultSet of data extracted from source
     * @return BackfillObjectRecord with upper bound initialized, one for each primary key
     */
    private List<GsiBackfillManager.BackfillObjectRecord> initUpperBound(final ExecutionContext baseEc,
                                                                         final long ddlJobId,
                                                                         final String dbIndex, final String phyTable,
                                                                         final List<Integer> primaryKeysId) {
        final List<Map<Integer, ParameterContext>> upperBound = getUpperBound(baseEc, dbIndex, phyTable);

        long tableRows = getTableRowsCount(dbIndex, phyTable);
        BackfillExtraFieldJSON extra = new BackfillExtraFieldJSON();
        extra.setApproximateRowCount(String.valueOf(tableRows));

        return getBackfillObjectRecords(baseEc, ddlJobId, dbIndex, phyTable, primaryKeysId, upperBound,
            BackfillExtraFieldJSON.toJson(extra));
    }

    @NotNull
    protected List<GsiBackfillManager.BackfillObjectRecord> getBackfillObjectRecords(ExecutionContext baseEc,
                                                                                     long ddlJobId, String dbIndex,
                                                                                     String phyTable,
                                                                                     List<Integer> primaryKeysId,
                                                                                     final List<Map<Integer, ParameterContext>> upperBound,
                                                                                     String extra) {

        SQLRecorderLogger.ddlLogger.warn(MessageFormat
            .format("[{0}] Backfill upper bound [{1}, {2}] {3}",
                baseEc.getTraceId(),
                dbIndex,
                phyTable,
                GsiUtils.rowToString(upperBound.get(0))));

        // Convert to BackfillObjectRecord
        final AtomicInteger srcIndex = new AtomicInteger(0);
        return primaryKeysId.stream().mapToInt(columnIndex -> columnIndex).mapToObj(columnIndex -> {
            if (upperBound.isEmpty()) {
                // Table is empty, no upper bound needed
                return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                    schemaName,
                    sourceTableName,
                    targetTableName,
                    dbIndex,
                    phyTable,
                    columnIndex,
                    extra);
            } else {
                final ParameterContext pc = upperBound.get(0).get(srcIndex.getAndIncrement() + 1);
                return GsiUtils.buildBackfillObjectRecord(ddlJobId,
                    schemaName,
                    sourceTableName,
                    targetTableName,
                    dbIndex,
                    phyTable,
                    columnIndex,
                    pc.getParameterMethod().name(),
                    null,
                    Transformer.serializeParam(pc),
                    extra);
            }
        }).collect(Collectors.toList());
    }

    /**
     * Lock and read batch from logical table, feed them to consumer
     *
     * @param ec base execution context
     * @param consumer next step
     */
    public void foreachBatch(ExecutionContext ec,
                             BatchConsumer consumer) {
        // For each physical table there is a backfill object which represents a row in
        // system table

        List<Future> futures = new ArrayList<>(16);

        // Re-balance by physicalDb.
        List<List<GsiBackfillManager.BackfillObjectBean>> tasks = reporter.getBackfillBean().backfillObjects.values()
            .stream()
            .filter(v -> v.get(0).status.is(UNFINISHED))
            .collect(Collectors.toList());
        Collections.shuffle(tasks);

        if (!tasks.isEmpty()) {
            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] Backfill job: {1} start with {2} task(s) parallelism {3}.",
                    ec.getTraceId(), tasks.get(0).get(0).jobId, tasks.size(), parallelism));
        }

        AtomicReference<Exception> excep = new AtomicReference<>(null);
        if (parallelism <= 0 || parallelism >= PriorityWorkQueue.getInstance().getCorePoolSize()) {
            // Full queued.
            tasks.forEach(v -> {
                FutureTask<Void> task = new FutureTask<>(
                    () -> foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer), null);
                futures.add(task);
                PriorityWorkQueue.getInstance()
                    .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
            });
        } else {

            // Use a bounded blocking queue to control the parallelism.
            BlockingQueue<Object> blockingQueue = new ArrayBlockingQueue<>((int) parallelism);
            tasks.forEach(v -> {
                try {
                    blockingQueue.put(new Object());
                } catch (Exception e) {
                    excep.set(e);
                }
                if (null == excep.get()) {
                    FutureTask<Void> task = new FutureTask<>(() -> {
                        try {
                            foreachPhyTableBatch(v.get(0).physicalDb, v.get(0).physicalTable, v, ec, consumer);
                        } finally {
                            // Poll in finally to prevent dead lock on putting blockingQueue.
                            blockingQueue.poll();
                        }
                        return null;
                    });
                    futures.add(task);
                    PriorityWorkQueue.getInstance()
                        .executeWithContext(task, PriorityFIFOTask.TaskPriority.GSI_BACKFILL_TASK);
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

        for (Future future : futures) {
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

        if (excep.get() != null) {
            if (!excep.get().getMessage().contains("need to be split into smaller batches")) {
                throttle.stop();
            } else {
                SQLRecorderLogger.ddlLogger.warn("all backfill feature task was interrupted by split");
            }
            throw GeneralUtil.nestedException(excep.get());
        }

        throttle.stop();

        // After all physical table finished
        reporter.updateBackfillStatus(ec, GsiBackfillManager.BackfillStatus.SUCCESS);
    }

    /**
     * Lock and read batch from physical table, feed them to consumer
     *
     * @param dbIndex physical db key
     * @param phyTable physical table name
     * @param ec base execution context
     * @param loader next step
     */
    protected void foreachPhyTableBatch(String dbIndex, String phyTable,
                                        List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                        ExecutionContext ec,
                                        BatchConsumer loader) {
        String physicalTableName = TddlSqlToRelConverter.unwrapPhysicalTableName(phyTable);
        // Load upper bound
        List<ParameterContext> upperBoundParam =
            buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
        final boolean withUpperBound = GeneralUtil.isNotEmpty(upperBoundParam);

        // Init historical position mark
        long successRowCount = backfillObjects.get(0).successRowCount;
        List<ParameterContext> lastPk = initSelectParam(backfillObjects, primaryKeysIdMap);

        long rangeBackfillStartTime = System.currentTimeMillis();

        List<Map<Integer, ParameterContext>> lastBatch = null;
        boolean finished = false;
        long actualBatchSize = batchSize;
        do {
            try {
                if (rateLimiter != null) {
                    rateLimiter.acquire((int) actualBatchSize);
                }
                long start = System.currentTimeMillis();

                // Dynamic adjust lower bound of rate.
                final long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
                if (dynamicRate > 0) {
                    throttle.resetMaxRate(dynamicRate);
                }

                // For next batch, build select plan and parameters
                final PhyTableOperation selectPlan = buildSelectPlanWithParam(dbIndex,
                    physicalTableName,
                    actualBatchSize,
                    Stream.concat(lastPk.stream(), upperBoundParam.stream()).collect(Collectors.toList()),
                    GeneralUtil.isNotEmpty(lastPk),
                    withUpperBound);

                List<ParameterContext> finalLastPk = lastPk;
                lastBatch = GsiUtils.retryOnException(
                    // 1. Lock rows within trx1 (single db transaction)
                    // 2. Fill into index table within trx2 (XA transaction)
                    // 3. Trx1 commit, if (success) {trx2 commit} else {trx2 rollback}
                    () -> GsiUtils.wrapWithSingleDbTrx(tm, ec,
                        (selectEc) -> extract(dbIndex, physicalTableName, selectPlan, selectEc, loader, finalLastPk,
                            upperBoundParam)),
                    e -> (GsiUtils.vendorErrorIs(e, SQLSTATE_DEADLOCK, ER_LOCK_DEADLOCK)
                        || GsiUtils.vendorErrorIs(e, SQLSTATE_LOCK_TIMEOUT, ER_LOCK_WAIT_TIMEOUT))
                        || e.getMessage().contains("Loader check error."),
                    (e, retryCount) -> deadlockErrConsumer(selectPlan, ec, e, retryCount));

                // For status recording
                List<ParameterContext> beforeLastPk = lastPk;

                // Build parameter for next batch
                lastPk = buildSelectParam(lastBatch, primaryKeysId);

                finished = lastBatch.size() != actualBatchSize;

                // Update position mark
                successRowCount += lastBatch.size();
                reporter.updatePositionMark(ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished,
                    primaryKeysIdMap);
                ec.getStats().backfillRows.addAndGet(lastBatch.size());
                DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(lastBatch.size());

                if (!finished) {
                    throttle.feedback(new com.alibaba.polardbx.executor.backfill.Throttle.FeedbackStats(
                        System.currentTimeMillis() - start, start, lastBatch.size()));
                }
                DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set((long) throttle.getActualRateLastCycle());

                if (rateLimiter != null) {
                    // Limit rate.
                    rateLimiter.setRate(throttle.getNewRate());
                }

                // Check DDL is ongoing.
                if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
                    long jobId = ec.getDdlJobId();
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "The job '" + jobId + "' has been cancelled");
                }
                if (actualBatchSize < batchSize) {
                    actualBatchSize = Math.min(actualBatchSize * 2, batchSize);
                }
            } catch (TddlRuntimeException e) {
                boolean retry = (e.getErrorCode() == ErrorCode.ERR_X_PROTOCOL_BAD_PACKET.getCode()) ||
                    (e.getSQLState() != null && e.getSQLState().equalsIgnoreCase("S1000") && e.getMessage()
                        .toLowerCase().contains("max_allowed_packet")) && actualBatchSize > 1;
                if (retry) {
                    actualBatchSize = Math.max(actualBatchSize / 8, 1);
                } else {
                    throw e;
                }
            }

            // for sliding window of split
            checkAndSplitBackfillObject(dbIndex, phyTable, successRowCount, ec, rangeBackfillStartTime, lastBatch,
                backfillObjects);
        } while (!finished);

        DdlEngineStats.METRIC_BACKFILL_ROWS_SPEED.set(0);
        reporter.addBackfillCount(successRowCount);

        SQLRecorderLogger.ddlLogger.warn(MessageFormat.format("[{0}] Last backfill row for {1}[{2}][{3}]: {4}",
            ec.getTraceId(),
            dbIndex,
            phyTable,
            successRowCount,
            GsiUtils.rowToString(lastBatch.isEmpty() ? null : lastBatch.get(lastBatch.size() - 1))));
    }

    /**
     * for split backfill range
     */
    private void checkAndSplitBackfillObject(String dbIndex, String phyTable, long successRowCount,
                                             ExecutionContext ec, long rangeBackfillStartTime,
                                             List<Map<Integer, ParameterContext>> lastBatch,
                                             List<GsiBackfillManager.BackfillObjectBean> backfillObjects) {
        ParamManager pm = OptimizerContext.getContext(schemaName).getParamManager();
        ParamManager ecPm = ec.getParamManager();
        boolean enableSlideWindowBackfill = pm.getBoolean(ConnectionParams.ENABLE_SLIDE_WINDOW_BACKFILL);
        boolean enableSample = pm.getBoolean(ConnectionParams.ENABLE_INNODB_BTREE_SAMPLING);

        boolean enablePhyTblParallelBackfill =
            ecPm.getBoolean(ConnectionParams.ENABLE_PHYSICAL_TABLE_PARALLEL_BACKFILL);
        long slideWindowInterval = ecPm.getLong(ConnectionParams.SLIDE_WINDOW_TIME_INTERVAL);
        long randomInterval = RandomUtils.nextInt(maxRandomInterval) + slideWindowInterval;
        int splitCount = ecPm.getInt(ConnectionParams.SLIDE_WINDOW_SPLIT_SIZE);

        if (enableSample && enablePhyTblParallelBackfill && enableSlideWindowBackfill && splitCount > 1
            && System.currentTimeMillis() - rangeBackfillStartTime > randomInterval
            && PriorityWorkQueue.getInstance().getActiveCount()
            < PriorityWorkQueue.getInstance().getCorePoolSize() * 0.75
            && isNotEmpty(backfillObjects.get(0).extra)) {
            // 估算一下剩余的行数，行数大于多少时，才能分裂
            long rowCount = Long.parseLong(backfillObjects.get(0).extra.getApproximateRowCount());
            long remainingRows = rowCount - successRowCount;
            if (remainingRows > ec.getParamManager().getLong(ConnectionParams.PHYSICAL_TABLE_START_SPLIT_SIZE)) {
                // gen backfill objects
                List<ParameterContext> upperBoundParam =
                    buildUpperBoundParam(backfillObjects.size(), backfillObjects, primaryKeysIdMap);
                List<ParameterContext> lowerBoundParam = initSelectParam(backfillObjects, primaryKeysIdMap);
                if (GeneralUtil.isEmpty(upperBoundParam) || GeneralUtil.isEmpty(lowerBoundParam)) {
                    return;
                }

                List<GsiBackfillManager.BackfillObjectRecord> newBackfillObjects =
                    splitPhysicalBatch(ec, remainingRows, dbIndex, phyTable, backfillObjects);

                if (newBackfillObjects == null) {
                    return;
                }

                // update backfill object meta
                reporter.splitBackfillRange(ec, backfillObjects, newBackfillObjects);

                SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                    "[{0}] the physical batch need to be split into smaller batches, last backfill row for {1}[{2}][{3}]: {4}",
                    ec.getTraceId(),
                    dbIndex,
                    phyTable,
                    successRowCount,
                    GsiUtils.rowToString(lastBatch.isEmpty() ? null : lastBatch.get(lastBatch.size() - 1))));

                this.needBuildSubBoundList = false;

                // throw error, redo task
                long backfillId = ec.getBackfillId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "The physical batch '%s' of backfillTask '%s' need to be split into smaller batches",
                    phyTable, backfillId));
            }
        }
    }

    /**
     * Print log for deadlock found
     *
     * @param plan Select plan
     * @param ec ExecutionContext
     * @param e Exception object
     * @param retryCount Times of retried
     */
    protected static void deadlockErrConsumer(PhyTableOperation plan, ExecutionContext ec,
                                              TddlNestableRuntimeException e, int retryCount) {
        final String dbIndex = plan.getDbIndex();
        final String phyTable = plan.getTableNames().get(0).get(0);
        final String row = GsiUtils.rowToString(plan.getParam());

        if (retryCount < RETRY_COUNT) {

            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] retry count[{4}]: {5}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                retryCount,
                e.getMessage()));

            try {
                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT[retryCount]);
            } catch (InterruptedException ex) {
                // ignore
            }
        } else {
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "[{0}] Deadlock found while extracting backfill data from {1}[{2}][{3}] throw: {4}",
                ec.getTraceId(),
                dbIndex,
                phyTable,
                row,
                e.getMessage()));

            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Extract data and apply to next pipeline
     *
     * @param extractPlan select plan
     * @param extractEc execution context for select plan
     * @param next next pipeline
     * @return converted data of current batch
     */
    protected List<Map<Integer, ParameterContext>> extract(String dbIndex, String phyTableName,
                                                           PhyTableOperation extractPlan, ExecutionContext extractEc,
                                                           BatchConsumer next,
                                                           List<ParameterContext> lowerBound,
                                                           List<ParameterContext> upperBound) {

        Cursor extractCursor = null;
        // Transform
        final List<Map<Integer, ParameterContext>> result;
        try {
            // Extract
            extractCursor = ExecutorHelper.execute(extractPlan, extractEc);
            result = com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam(extractCursor);
        } finally {
            if (extractCursor != null) {
                extractCursor.close(new ArrayList<>());
            }
        }

        FailPoint.injectRandomExceptionFromHint(FP_RANDOM_BACKFILL_EXCEPTION, extractEc);

        // Load
        next.consume(result, Pair.of(extractEc, Pair.of(extractPlan.getDbIndex(), phyTableName)));

        return result;
    }

    protected static List<ParameterContext> buildUpperBoundParam(int offset,
                                                                 List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                                                 Map<Long, Long> primaryKeysIdMap) {
        // Value of Primary Key can never be null
        return backfillObjects.stream().sorted(Comparator.comparingLong(o -> primaryKeysIdMap.get(o.columnIndex)))
            .filter(bfo -> Objects.nonNull(bfo.maxValue)).map(
                bfo -> com.alibaba.polardbx.executor.gsi.utils.Transformer
                    .buildParamByType(offset + bfo.columnIndex, bfo.parameterMethod, bfo.maxValue)).collect(
                Collectors.toList());
    }

    /**
     * Build params for physical select
     *
     * @param batchResult Result of last batch
     * @param primaryKeysId Pk index in parameter list
     * @return built plan
     */
    protected static List<ParameterContext> buildSelectParam(List<Map<Integer, ParameterContext>> batchResult,
                                                             List<Integer> primaryKeysId) {
        List<ParameterContext> lastPk = null;
        if (GeneralUtil.isNotEmpty(batchResult)) {
            Map<Integer, ParameterContext> lastRow = batchResult.get(batchResult.size() - 1);
            lastPk =
                primaryKeysId.stream().mapToInt(i -> i).mapToObj(i -> lastRow.get(i + 1)).collect(Collectors.toList());
        }

        return lastPk;
    }

    /**
     * Build params for physical select from loaded backfill objects
     *
     * @param backfillObjects Backfill objects loaded from meta table
     */
    protected static List<ParameterContext> initSelectParam(List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                                            Map<Long, Long> primaryKeysIdMap) {
        return backfillObjects.stream()
            // Primary key is null only when it is initializing
            .filter(bfo -> Objects.nonNull(bfo.lastValue))
            .sorted(Comparator.comparingLong(o -> primaryKeysIdMap.get(o.columnIndex)))
            .map(bfo -> Transformer.buildParamByType(bfo.columnIndex, bfo.parameterMethod, bfo.lastValue))
            .collect(Collectors.toList());
    }

    /**
     * Build plan for physical select.
     *
     * @param params pk column value of last batch
     * @return built plan
     */
    protected PhyTableOperation buildSelectPlanWithParam(String dbIndex, String phyTable, long batchSize,
                                                         List<ParameterContext> params, boolean withLowerBound,
                                                         boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        final int pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        if (withLowerBound) {
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }
        if (withUpperBound) {
            final int base = withLowerBound ? pkNumber : 0;
            for (int i = 0; i < pkNumber; ++i) {
                for (int j = 0; j <= i; ++j) {
                    planParams.put(nextParamIndex,
                        new ParameterContext(params.get(base + j).getParameterMethod(),
                            new Object[] {nextParamIndex, params.get(base + j).getArgs()[1]}));
                    nextParamIndex++;
                }
            }
        }

        // Parameters for limit
        if (batchSize > 0) {
            planParams.put(nextParamIndex,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));
        }
        // Get ExecutionPlan
//        PhyTableOperation plan;
//        if (!withLowerBound) {
//            plan = new PhyTableOperation(planSelectWithMax);
//        } else {
//            plan = new PhyTableOperation(withUpperBound ? planSelectWithMinAndMax : planSelectWithMin);
//        }
//        plan.setDbIndex(dbIndex);
//        plan.setTableNames(ImmutableList.of(ImmutableList.of(phyTable)));
//        plan.setParam(planParams);

        PhyTableOperation targetPhyOp =
            !withLowerBound ? planSelectWithMax : (withUpperBound ? planSelectWithMinAndMax : planSelectWithMin);
        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setGroupName(dbIndex);
        buildParams.setPhyTables(ImmutableList.of(ImmutableList.of(phyTable)));
        buildParams.setDynamicParams(planParams);
        PhyTableOperation plan =
            PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);

        return PhyTableOperationFactory.getInstance().buildPhyTableOperationByPhyOp(targetPhyOp, buildParams);
    }

    /**
     * Build plan for physical sample select.
     *
     * @param params pk column value of last batch
     * @return built plan
     */
    protected PhyTableOperation buildSamplePlanWithParam(String dbIndex, String phyTable,
                                                         List<ParameterContext> params, float calSamplePercentage,
                                                         boolean withLowerBound, boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();
        // Physical table is 1st parameter
        planParams.put(1, PlannerUtils.buildParameterContextForTableName(phyTable, 1));

        int nextParamIndex = 2;

        // Parameters for where(DNF)
        if (withLowerBound && withUpperBound) {
            for (ParameterContext param : params) {
                planParams.put(nextParamIndex,
                    new ParameterContext(param.getParameterMethod(),
                        new Object[] {nextParamIndex, param.getArgs()[1]}));
                nextParamIndex++;
            }
        }

        PhyTableOperation phyTableOperation = withLowerBound ? planSelectMinAndMaxSample : planSelectSample;
        SqlSelect sqlSelect = (SqlSelect) phyTableOperation.getNativeSqlNode();
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

    public Map<String, Set<String>> getSourcePhyTables() {
        return Maps.newHashMap();
    }

    public List<Integer> getPrimaryKeysId() {
        return primaryKeysId;
    }

    public static List<String> getPrimaryKeys(TableMeta tableMeta, ExecutionContext ec) {
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

    public static class ExtractorInfo {
        TableMeta sourceTableMeta;
        List<String> targetTableColumns;
        List<String> primaryKeys;

        /**
         * record the serial number of primary key in the table
         * e.g.
         * create table gg(
         * a int(11),
         * b int(11),
         * c int(11),
         * primary key(c , b)
         * );
         * primaryKeysId should be {2, 1}
         * primaryKeysId[0]=2 means the 1-st column in primary key is the 3-rd column in the table
         */
        List<Integer> primaryKeysId;

        public ExtractorInfo(TableMeta sourceTableMeta, List<String> targetTableColumns, List<String> primaryKeys,
                             List<Integer> appearedKeysId) {
            this.sourceTableMeta = sourceTableMeta;
            this.targetTableColumns = targetTableColumns;
            this.primaryKeys = primaryKeys;
            this.primaryKeysId = appearedKeysId;
        }

        public TableMeta getSourceTableMeta() {
            return sourceTableMeta;
        }

        public List<String> getTargetTableColumns() {
            return targetTableColumns;
        }

        public List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public List<Integer> getPrimaryKeysId() {
            return primaryKeysId;
        }
    }

    public static ExtractorInfo buildExtractorInfo(ExecutionContext ec,
                                                   String schemaName,
                                                   String sourceTableName,
                                                   String targetTableName) {
        final SchemaManager sm = OptimizerContext.getContext(schemaName).getLatestSchemaManager();
        final TableMeta sourceTableMeta = sm.getTable(sourceTableName);
        final TableMeta targetTableMeta = sm.getTable(targetTableName);
        final List<String> targetTableColumns = targetTableMeta.getWriteColumns()
            .stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        Map<String, Integer> targetColumnMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < targetTableColumns.size(); i++) {
            targetColumnMap.put(targetTableColumns.get(i), i);
        }
        // order reserved primary keys
        List<String> primaryKeys = getPrimaryKeys(sourceTableMeta, ec);
        // primary keys appeared in select list with reserved order
        List<String> appearedKeys = new ArrayList<>();
        List<Integer> appearedKeysId = new ArrayList<>();
        for (String primaryKey : primaryKeys) {
            if (targetColumnMap.containsKey(primaryKey)) {
                appearedKeys.add(primaryKey);
                appearedKeysId.add(targetColumnMap.get(primaryKey));
            }
        }

        return new ExtractorInfo(sourceTableMeta, targetTableColumns, appearedKeys, appearedKeysId);
    }
}
