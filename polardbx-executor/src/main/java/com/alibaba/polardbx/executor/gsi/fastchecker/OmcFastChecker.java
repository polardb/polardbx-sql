package com.alibaba.polardbx.executor.gsi.fastchecker;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.workqueue.FastCheckerThreadPool;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Data;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections.MapUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static java.lang.Math.max;

/**
 * @author wumu
 */
public class OmcFastChecker extends FastChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(FastChecker.class);

    public OmcFastChecker(String schemaName, String srcLogicalTableName, String dstLogicalTableName,
                          Map<String, Set<String>> srcPhyDbAndTables, Map<String, Set<String>> dstPhyDbAndTables,
                          List<String> srcColumns, List<String> dstColumns, List<String> srcPks, List<String> dstPks,
                          PhyTableOperation planSelectHashCheckSrc,
                          PhyTableOperation planSelectHashCheckWithUpperBoundSrc,
                          PhyTableOperation planSelectHashCheckWithLowerBoundSrc,
                          PhyTableOperation planSelectHashCheckWithLowerUpperBoundSrc,
                          PhyTableOperation planSelectHashCheckDst,
                          PhyTableOperation planSelectHashCheckWithUpperBoundDst,
                          PhyTableOperation planSelectHashCheckWithLowerBoundDst,
                          PhyTableOperation planSelectHashCheckWithLowerUpperBoundDst,
                          PhyTableOperation planIdleSelectSrc, PhyTableOperation planIdleSelectDst,
                          PhyTableOperation planSelectSampleSrc, PhyTableOperation planSelectSampleDst) {
        super(schemaName, schemaName, srcLogicalTableName, dstLogicalTableName, srcPhyDbAndTables,
            dstPhyDbAndTables, srcColumns, dstColumns, srcPks, dstPks, planSelectHashCheckSrc,
            planSelectHashCheckWithUpperBoundSrc, planSelectHashCheckWithLowerBoundSrc,
            planSelectHashCheckWithLowerUpperBoundSrc, planSelectHashCheckDst, planSelectHashCheckWithUpperBoundDst,
            planSelectHashCheckWithLowerBoundDst, planSelectHashCheckWithLowerUpperBoundDst, planIdleSelectSrc,
            planIdleSelectDst, planSelectSampleSrc, planSelectSampleDst);
    }

    public static FastChecker create(String schemaName, String tableName, String indexName,
                                     Map<String, String> srcColumnMap, Map<String, String> dstColumnMap,
                                     ExecutionContext ec) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
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

        // for rebuild table
        final List<String> indexColumns = new ArrayList<>();
        final List<String> baseTableColumns = new ArrayList<>();

        final List<String> commonColumns = new ArrayList<>();
        final List<String> baseTableOriginColumns = new ArrayList<>();
        final List<String> baseTableCheckColumns = new ArrayList<>();
        final List<String> indexOriginColumns = new ArrayList<>();
        final List<String> indexCheckColumns = new ArrayList<>();

        for (ColumnMeta columnMeta : indexTableMeta.getAllColumns()) {
            String baseTableColumnName = null;
            if (columnMeta.getMappingName() != null) {
                if (!columnMeta.getMappingName().isEmpty()) {
                    baseTableColumnName = columnMeta.getMappingName().toLowerCase();
                }
            } else {
                baseTableColumnName = columnMeta.getName().toLowerCase();
            }

            if (baseTableColumnName != null) {
                String indexColumnName = columnMeta.getName().toLowerCase();
                baseTableColumns.add(baseTableColumnName);
                indexColumns.add(indexColumnName);

                if (MapUtils.isNotEmpty(srcColumnMap) && srcColumnMap.containsKey(baseTableColumnName)) {
                    baseTableOriginColumns.add(baseTableColumnName);
                    baseTableCheckColumns.add(srcColumnMap.get(baseTableColumnName));

                    assert (MapUtils.isNotEmpty(dstColumnMap) && dstColumnMap.containsKey(indexColumnName));
                    indexOriginColumns.add(indexColumnName);
                    indexCheckColumns.add(dstColumnMap.get(indexColumnName));
                } else {
                    commonColumns.add(baseTableColumnName);
                }
            }
        }

        // 重要：构造planSelectSampleSrc 和 planSelectSampleDst时，传入的主键必须按原本的主键顺序!
        final List<String> baseTablePks = FastChecker.getOrderedPrimaryKeys(baseTableMeta);
        final List<String> indexTablePks = FastChecker.getOrderedPrimaryKeys(indexTableMeta);

        final Map<String, Set<String>> srcPhyDbAndTables = GsiUtils.getPhyTables(schemaName, tableName);
        final Map<String, Set<String>> dstPhyDbAndTables = GsiUtils.getPhyTables(schemaName, indexName);

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, ec);

        return new OmcFastChecker(schemaName, tableName, indexName, srcPhyDbAndTables, dstPhyDbAndTables,
            baseTableColumns, indexColumns, baseTablePks, indexTablePks,
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, commonColumns, baseTableOriginColumns,
                baseTableCheckColumns, baseTablePks, false, false),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, commonColumns, baseTableOriginColumns,
                baseTableCheckColumns, baseTablePks, false, true),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, commonColumns, baseTableOriginColumns,
                baseTableCheckColumns, baseTablePks, true, false),
            builder.buildSelectHashCheckForGSIChecker(baseTableMeta, commonColumns, baseTableOriginColumns,
                baseTableCheckColumns, baseTablePks, true, true),

            builder.buildSelectHashCheckForGSIChecker(indexTableMeta, commonColumns, indexOriginColumns,
                indexCheckColumns, indexTablePks, false, false),
            builder.buildSelectHashCheckForGSIChecker(indexTableMeta, commonColumns, indexOriginColumns,
                indexCheckColumns, indexTablePks, false, true),
            builder.buildSelectHashCheckForGSIChecker(indexTableMeta, commonColumns, indexOriginColumns,
                indexCheckColumns, indexTablePks, true, false),
            builder.buildSelectHashCheckForGSIChecker(indexTableMeta, commonColumns, indexOriginColumns,
                indexCheckColumns, indexTablePks, true, true),

            builder.buildIdleSelectForChecker(baseTableMeta, baseTableColumns),
            builder.buildIdleSelectForChecker(indexTableMeta, indexColumns),

            builder.buildSqlSelectForSample(baseTableMeta, baseTablePks),
            builder.buildSqlSelectForSample(indexTableMeta, indexTablePks));
    }

    @Data
    static class HashCheckResult {
        public Long commonHash;
        public Long originColumnHash;
        public Long checkColumnHash;
    }

    @Override
    protected boolean parallelCheck(Map<String, Set<String>> srcDbAndTb,
                                    Map<String, Set<String>> dstDbAndTb,
                                    ExecutionContext baseEc, long batchSize) {
        Set<String> allGroups = new TreeSet<>(String::compareToIgnoreCase);
        allGroups.addAll(srcDbAndTb.keySet());
        allGroups.addAll(dstDbAndTb.keySet());

        //<GroupName, StorageInstId>
        Map<String, String> mapping = queryStorageInstIdByPhyGroup(allGroups);

        Map<String, List<FutureTask<Pair<HashCheckResult, Boolean>>>> allFutureTasksByGroup =
            new TreeMap<>(String::compareToIgnoreCase);

        int srcTableTaskCount = 0, dstTableTaskCount = 0;
        final Map mdcContext = MDC.getCopyOfContextMap();

        for (Map.Entry<String, Set<String>> entry : srcDbAndTb.entrySet()) {
            String srcDb = entry.getKey();
            for (String srcTb : entry.getValue()) {
                FutureTask<Pair<HashCheckResult, Boolean>> task = new FutureTask<>(
                    () -> {
                        MDC.setContextMap(mdcContext);
                        return hashCheckForSinglePhyTable(srcDb, srcTb, baseEc, true, batchSize);
                    }
                );

                allFutureTasksByGroup.putIfAbsent(srcDb, new ArrayList<>());
                allFutureTasksByGroup.get(srcDb).add(task);
                srcTableTaskCount++;
            }
        }

        for (Map.Entry<String, Set<String>> entry : dstDbAndTb.entrySet()) {
            String dstDb = entry.getKey();
            for (String dstTb : entry.getValue()) {
                FutureTask<Pair<HashCheckResult, Boolean>> task = new FutureTask<>(
                    () -> {
                        MDC.setContextMap(mdcContext);
                        return hashCheckForSinglePhyTable(dstDb, dstTb, baseEc, false, batchSize);
                    }
                );

                allFutureTasksByGroup.putIfAbsent(dstDb, new ArrayList<>());
                allFutureTasksByGroup.get(dstDb).add(task);
                dstTableTaskCount++;
            }
        }

        LOGGER.info(
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

        //submit tasks to fastChecker threadPool
        FastCheckerThreadPool threadPool = FastCheckerThreadPool.getInstance();
        List<Pair<String, Runnable>> allTasksByStorageInstId = new ArrayList<>();
        for (Map.Entry<String, List<FutureTask<Pair<HashCheckResult, Boolean>>>> entry : allFutureTasksByGroup.entrySet()) {
            String groupName = entry.getKey();
            if (!mapping.containsKey(groupName)) {
                throw new TddlRuntimeException(
                    ErrorCode.ERR_FAST_CHECKER,
                    String.format("FastChecker failed to get group-storageInstId mapping, group [%s]", groupName)
                );
            }
            String storageInstId = mapping.get(groupName);
            for (FutureTask<Pair<HashCheckResult, Boolean>> task : entry.getValue()) {
                allTasksByStorageInstId.add(Pair.of(storageInstId, task));
            }
        }

        threadPool.submitTasks(allTasksByStorageInstId);

        List<Pair<HashCheckResult, Boolean>> result = new ArrayList<>();
        List<FutureTask<Pair<HashCheckResult, Boolean>>> allFutureTasks = allTasksByStorageInstId
            .stream()
            .map(Pair::getValue)
            .map(task -> (FutureTask<Pair<HashCheckResult, Boolean>>) task)
            .collect(Collectors.toList());

        for (FutureTask<Pair<HashCheckResult, Boolean>> futureTask : allFutureTasks) {
            try {
                result.add(futureTask.get());
            } catch (Exception e) {
                for (FutureTask<Pair<HashCheckResult, Boolean>> taskToBeCancel : allFutureTasks) {
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

        List<HashCheckResult> srcResult =
            result.stream().filter(p -> p != null && p.getKey() != null && p.getValue()).map(Pair::getKey)
                .collect(Collectors.toList());
        List<HashCheckResult> dstResult =
            result.stream().filter(p -> p != null && p.getKey() != null && !p.getValue()).map(Pair::getKey)
                .collect(Collectors.toList());

        return srcTableTaskCount == result.stream().filter(Objects::nonNull).filter(Pair::getValue).count()
            && dstTableTaskCount == result.stream().filter(Objects::nonNull).filter(x -> !x.getValue()).count()
            && compare(srcResult, dstResult);
    }

    private Pair<HashCheckResult, Boolean> hashCheckForSinglePhyTable(String phyDbName, String phyTable,
                                                                      ExecutionContext baseEc,
                                                                      boolean isSrcTableTask, long maxBatchRows) {
        String schema = isSrcTableTask ? srcSchemaName : dstSchemaName;
        long tableRowsCount = getTableRowsCount(schema, phyDbName, phyTable);

        //get phy table's avgRowSize
        long tableAvgRowLength = getTableAvgRowSize(schema, phyDbName, phyTable);
        long fastCheckerMaxBatchFileSize =
            baseEc.getParamManager().getLong(ConnectionParams.FASTCHECKER_BATCH_FILE_SIZE);

        boolean needBatchCheck = false;
        if (tableRowsCount * tableAvgRowLength > fastCheckerMaxBatchFileSize || tableRowsCount > maxBatchRows) {
            needBatchCheck = true;
        }

        long startTime = System.currentTimeMillis();
        /**
         * if table size exceed batch size, we will calculate digest by batch.
         * otherwise, we will straightly calculate the whole phy table's digest
         * */

        boolean failedToSplitBatch = false;
        HashCheckResult hashResult = null;

        if (!whetherCanSplitIntoBatch(baseEc, isSrcTableTask)) {
            needBatchCheck = false;
        }

        if (needBatchCheck) {
            long finalBatchRows = maxBatchRows;
            if (tableRowsCount * tableAvgRowLength > fastCheckerMaxBatchFileSize) {
                tableAvgRowLength = max(1, tableAvgRowLength);
                finalBatchRows = fastCheckerMaxBatchFileSize / tableAvgRowLength;
            }
            if (finalBatchRows > maxBatchRows) {
                finalBatchRows = maxBatchRows;
            }

            List<Map<Integer, ParameterContext>> batchBoundList =
                splitPhyTableIntoBatch(baseEc, phyDbName, phyTable, tableRowsCount, finalBatchRows, isSrcTableTask);
            if (!batchBoundList.isEmpty()) {
                SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                    "[{0}] FastChecker start hash phy for {1}[{2}][{3}], and phy table is divided into {4} batches",
                    baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst",
                    batchBoundList.size() + 1));

                hashResult =
                    getPhyTableHashCheckResByBatch(phyDbName, phyTable, baseEc, isSrcTableTask, batchBoundList);
            } else {
                failedToSplitBatch = true;
            }
        }

        if (!needBatchCheck || failedToSplitBatch) {
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0}] FastChecker start hash phy for {1}[{2}][{3}], and phy table is hashed by full scan",
                baseEc.getTraceId(), phyDbName, phyTable, isSrcTableTask ? "src" : "dst"));

            hashResult = getPhyTableHashCheckResByFullScan(phyDbName, phyTable, baseEc, isSrcTableTask);
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

    private HashCheckResult getPhyTableHashCheckResByBatch(String phyDbName, String phyTable, ExecutionContext baseEc,
                                                           boolean isSrcTableTask,
                                                           List<Map<Integer, ParameterContext>> batchBoundList) {
        if (batchBoundList.isEmpty()) {
            return null;
        }

        List<HashCheckResult> hashResults = new ArrayList<>();
        List<PhyTableOperation> hashCheckPlans = genHashCheckPlans(phyDbName, phyTable, isSrcTableTask, batchBoundList);

        // execute
        for (PhyTableOperation phyPlan : hashCheckPlans) {
            HashCheckResult batchHashResult = executeHashCheckPlan(phyPlan, baseEc);
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

        if (hashResults.size() == 1) {
            return hashResults.get(0);
        }

        final HashCalculator commonCalculator = new HashCalculator();
        final HashCalculator originColumnCalculator = new HashCalculator();
        final HashCalculator checkColumnCalculator = new HashCalculator();
        for (HashCheckResult hashResult : hashResults) {
            commonCalculator.calculate(hashResult.commonHash);
            originColumnCalculator.calculate(hashResult.originColumnHash);
            checkColumnCalculator.calculate(hashResult.checkColumnHash);
        }

        HashCheckResult ret = new HashCheckResult();
        ret.commonHash = commonCalculator.getHashVal();
        ret.originColumnHash = originColumnCalculator.getHashVal();
        ret.checkColumnHash = checkColumnCalculator.getHashVal();
        return ret;
    }

    private HashCheckResult getPhyTableHashCheckResByFullScan(String phyDbName, String phyTable,
                                                              ExecutionContext baseEc,
                                                              boolean isSrcTableTask) {
        if (CrossEngineValidator.isJobInterrupted(baseEc) || Thread.currentThread().isInterrupted()) {
            long jobId = baseEc.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }

        PhyTableOperation plan = genHashCheckPlan(phyDbName, phyTable, baseEc, isSrcTableTask);

        return executeHashCheckPlan(plan, baseEc);
    }

    private HashCheckResult executeHashCheckPlan(PhyTableOperation plan, ExecutionContext ec) {
        Cursor cursor = null;
        HashCheckResult result = null;
        try {
            cursor = ExecutorHelper.executeByCursor(plan, ec, false);
            Row row;
            if (cursor != null && (row = cursor.next()) != null) {
                result = new HashCheckResult();
                if (row.getColNum() == 1) {
                    result.commonHash = (Long) row.getObject(0);
                } else if (row.getColNum() == 2) {
                    result.originColumnHash = (Long) row.getObject(0);
                    result.checkColumnHash = (Long) row.getObject(1);
                } else if (row.getColNum() == 3) {
                    result.originColumnHash = (Long) row.getObject(0);
                    result.checkColumnHash = (Long) row.getObject(1);
                    result.commonHash = (Long) row.getObject(2);
                }

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

    private boolean compare(List<HashCheckResult> src, List<HashCheckResult> dst) {
        final HashCalculator srcCommonCalculator = new HashCalculator();
        final HashCalculator srcOriginColumnCalculator = new HashCalculator();
        final HashCalculator srcCheckColumnCalculator = new HashCalculator();
        final HashCalculator dstCommonCalculator = new HashCalculator();
        final HashCalculator dstOriginColumnCalculator = new HashCalculator();
        final HashCalculator dstCheckColumnCalculator = new HashCalculator();

        // 1. check common column hash value
        src.forEach(elem -> srcCommonCalculator.calculate(elem.commonHash));
        dst.forEach(elem -> dstCommonCalculator.calculate(elem.commonHash));

        if (!srcCommonCalculator.getHashVal().equals(dstCommonCalculator.getHashVal())) {
            LOGGER.info(
                MessageFormat.format(
                    "FastChecker check common column failed, schemaName: {0}, tableName: {1}, indexName: {2}",
                    srcSchemaName,
                    srcLogicalTableName,
                    dstLogicalTableName
                )
            );
            return false;
        }

        src.forEach(elem -> srcCheckColumnCalculator.calculate(elem.checkColumnHash));
        dst.forEach(elem -> dstOriginColumnCalculator.calculate(elem.originColumnHash));

        if (srcCheckColumnCalculator.getHashVal().equals(dstOriginColumnCalculator.getHashVal())) {
            return true;
        } else {
            LOGGER.info(
                MessageFormat.format(
                    "FastChecker check src virtual column failed, schemaName: {0}, tableName: {1}, indexName: {2}",
                    srcSchemaName,
                    srcLogicalTableName,
                    dstLogicalTableName
                )
            );
        }

        src.forEach(elem -> srcOriginColumnCalculator.calculate(elem.originColumnHash));
        dst.forEach(elem -> dstCheckColumnCalculator.calculate(elem.checkColumnHash));

        if (srcOriginColumnCalculator.getHashVal().equals(dstCheckColumnCalculator.getHashVal())) {
            return true;
        } else {
            LOGGER.info(
                MessageFormat.format(
                    "FastChecker check dst virtual column failed, schemaName: {0}, tableName: {1}, indexName: {2}",
                    srcSchemaName,
                    srcLogicalTableName,
                    dstLogicalTableName
                )
            );
            return false;
        }
    }
}
