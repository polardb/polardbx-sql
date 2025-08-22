package com.alibaba.polardbx.executor.partitionmanagement.corrector;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.backfill.Extractor;
import com.alibaba.polardbx.executor.backfill.GsiPkRangeExtractor;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.fastchecker.CheckerBatch;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.executor.gsi.GsiUtils.RETRY_WAIT;
import static com.alibaba.polardbx.executor.gsi.GsiUtils.SQLSTATE_DEADLOCK;

/**
 * Created by taokun.
 *
 * @author taokun
 */
public class AlterTableGroupBatchChecker extends AlterTableGroupChecker {

    CheckerBatch sourceBatch;
    CheckerBatch targetBatch;

    PhyTableOperation planSelectWithMinPrimary;
    PhyTableOperation planSelectWithMinGsi;
    PhyTableOperation planSelectWithoutMinAndMaxPrimary;
    PhyTableOperation planSelectWithoutMinAndMaxGsi;
    List<String> primaryKeyColumns;

    @Override
    public boolean isMoveTable() {
        return true;
    }

    public AlterTableGroupBatchChecker(String schemaName, String tableName, String indexName,
                                       TableMeta primaryTableMeta,
                                       TableMeta gsiTableMeta, long batchSize,
                                       long speedMin,
                                       long speedLimit,
                                       long parallelism,
                                       boolean useBinary,
                                       SqlSelect.LockMode primaryLock,
                                       SqlSelect.LockMode gsiLock,
                                       PhyTableOperation planSelectWithMaxPrimary,
                                       PhyTableOperation planSelectWithMaxGsi,
                                       PhyTableOperation planSelectWithMinAndMaxPrimary,
                                       PhyTableOperation planSelectWithMinAndMaxGsi,
                                       PhyTableOperation planSelectWithMinPrimary,
                                       PhyTableOperation planSelectWithMinGsi,
                                       PhyTableOperation planSelectWithoutMinAndMaxPrimary,
                                       PhyTableOperation planSelectWithoutMinAndMaxGsi,
                                       SqlSelect planSelectWithInTemplate,
                                       PhyTableOperation planSelectWithIn,
                                       PhyTableOperation planSelectMaxPk,
                                       List<String> indexColumns, List<Integer> primaryKeysId,
                                       Comparator<List<Pair<ParameterContext, byte[]>>> rowComparator,
                                       Map<String, Set<String>> sourceTargetTables,
                                       Map<String, Set<String>> targetTargetTables,
                                       CheckerBatch sourceBatch,
                                       CheckerBatch targetBatch,
                                       List<String> primaryKeyColumns) {
        super(schemaName, tableName, indexName, primaryTableMeta, gsiTableMeta, batchSize, speedMin, speedLimit,
            parallelism, useBinary,
            primaryLock, gsiLock, planSelectWithMaxPrimary, planSelectWithMaxGsi, planSelectWithMinAndMaxPrimary,
            planSelectWithMinAndMaxGsi, planSelectWithInTemplate, planSelectWithIn, planSelectMaxPk, indexColumns,
            primaryKeysId, rowComparator, sourceTargetTables, targetTargetTables);
        this.isGetShardResultForReplicationTable = true;
        this.sourceBatch = sourceBatch;
        this.targetBatch = targetBatch;
        this.planSelectWithMinPrimary = planSelectWithMinPrimary;
        this.planSelectWithMinGsi = planSelectWithMinGsi;
        this.planSelectWithoutMinAndMaxPrimary = planSelectWithoutMinAndMaxPrimary;
        this.planSelectWithoutMinAndMaxGsi = planSelectWithoutMinAndMaxGsi;
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public static AlterTableGroupBatchChecker create(String schemaName, String tableName, String indexName,
                                                     long batchSize, long speedMin,
                                                     long speedLimit, long parallelism, boolean useBinary,
                                                     SqlSelect.LockMode primaryLock, SqlSelect.LockMode gsiLock,
                                                     ExecutionContext ec,
                                                     Map<String, Set<String>> sourceTargetTables,
                                                     Map<String, Set<String>> targetTargetTables,
                                                     CheckerBatch sourceBatch,
                                                     CheckerBatch targetBatch) {
        // Build select plan
        final SchemaManager sm = ec.getSchemaManager(schemaName);
        final TableMeta indexTableMeta = sm.getTable(indexName);

        Extractor.ExtractorInfo info = Extractor.buildExtractorInfo(ec, schemaName, tableName, indexName, false);
        List<String> notConvertedColumns = info.getPrimaryKeys();

        final PhysicalPlanBuilder builder = new PhysicalPlanBuilder(schemaName, useBinary, notConvertedColumns, ec);

        final Pair<SqlSelect, PhyTableOperation> selectWithIn = builder
            .buildSelectWithInForChecker(info.getSourceTableMeta(), info.getTargetTableColumns(), info.getPrimaryKeys(),
                indexTableMeta.hasGsiImplicitPrimaryKey() ? null : "PRIMARY");

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
                ++idx;
            }
            return 0;
        };

        return new AlterTableGroupBatchChecker(schemaName,
            tableName,
            indexName,
            info.getSourceTableMeta(),
            indexTableMeta,
            batchSize,
            speedMin,
            speedLimit,
            parallelism,
            useBinary,
            primaryLock,
            gsiLock,
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, true, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, true, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                true, false, gsiLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, false, primaryLock),
            builder.buildSelectForBackfill(info.getSourceTableMeta(), info.getTargetTableColumns(),
                info.getPrimaryKeys(),
                false, false, gsiLock),
            selectWithIn.getKey(),
            selectWithIn.getValue(),
            builder.buildSelectMaxPkForBackfill(info.getSourceTableMeta(), info.getPrimaryKeys()),
            info.getTargetTableColumns(),
            info.getPrimaryKeysId(),
            rowComparator,
            sourceTargetTables,
            targetTargetTables,
            sourceBatch,
            targetBatch,
            notConvertedColumns);
    }

    public void check(ExecutionContext baseEc, Reporter cb) {
        String dbIndex = sourceBatch.getPhyDb();
        String phyTable = sourceBatch.getPhyTb();
        String logicalTableName = getTableName();
        List<ParameterContext> lowerBound = buildPrimaryKey(sourceBatch.getBound().getKey());
        List<ParameterContext> upperBound = buildPrimaryKey(sourceBatch.getBound().getValue());
        String checkType = "P<->G";
        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] FastChecker batch rechecker start phy for {1} [{2}]",
                baseEc.getTraceId(),
                sourceBatch.getBatchBoundDesc(),
                checkType));

        long startTs = System.currentTimeMillis();

        List<ParameterContext> params = new ArrayList<>();
        AtomicInteger actualBatchSize = new AtomicInteger();
        Set<String> notConvertedColumns = new HashSet<>(primaryKeyColumns);
        do {
            params = Stream.concat(lowerBound.stream(), upperBound.stream()).collect(Collectors.toList());
            PhyTableOperation selectBaseTable = GsiPkRangeExtractor.buildSelectWithParam(
                sourceBatch.getPhyDb(),
                sourceBatch.getPhyTb(),
                false,
                batchSize,
                params,
                !GeneralUtil.isEmpty(lowerBound),
                !GeneralUtil.isEmpty(upperBound),
                planSelectWithoutMinAndMaxPrimary,
                planSelectWithMaxPrimary,
                planSelectWithMinPrimary,
                planSelectWithMinAndMaxPrimary);

            List<ParameterContext> finalLowerBound = lowerBound;
            // Read base rows.
            final Cursor sourceCursor = ExecutorHelper.executeByCursor(selectBaseTable, baseEc, false);
            final List<List<Pair<ParameterContext, byte[]>>> baseRows = new ArrayList<>();
            try {
                Row row;
                while ((row = sourceCursor.next()) != null) {
                    baseRows.add(row2objects(row, useBinary, notConvertedColumns));
                }
            } finally {
                sourceCursor.close(new ArrayList<>());
            }

            actualBatchSize.set(baseRows.size());

            // Read check rows.
            List<ParameterContext> gsiUpperBound = upperBound;
            if (actualBatchSize.get() > 0 && actualBatchSize.get() == batchSize) {
                gsiUpperBound = buildPrimaryKey(baseRows.get(actualBatchSize.get() - 1));
            }
            List<ParameterContext> gsiParams =
                Stream.concat(finalLowerBound.stream(), gsiUpperBound.stream()).collect(Collectors.toList());
            PhyTableOperation selectTargetPlan = GsiPkRangeExtractor.buildSelectWithParam(
                targetBatch.getPhyDb(),
                targetBatch.getPhyTb(),
                false,
                batchSize + cb.earlyFailNumber,
                gsiParams,
                !GeneralUtil.isEmpty(finalLowerBound),
                !GeneralUtil.isEmpty(gsiUpperBound),
                planSelectWithoutMinAndMaxGsi,
                planSelectWithMaxGsi,
                planSelectWithMinGsi,
                planSelectWithMinAndMaxGsi);
            final List<List<Pair<ParameterContext, byte[]>>> checkRows = new ArrayList<>();
            final Cursor targetCursor = ExecutorHelper.executeByCursor(selectTargetPlan, baseEc, false);
            try {
                Row checkRow;
                while ((checkRow = targetCursor.next()) != null) {
                    checkRows.add(row2objects(checkRow, useBinary, notConvertedColumns));
                }
            } finally {
                targetCursor.close(new ArrayList<>());
            }

            baseRows.sort(getRowComparator());
            checkRows.sort(getRowComparator());

            SQLRecorderLogger.ddlLogger.warn(
                MessageFormat.format("[{0}] FastChecker batch checker start batch check for {1}", baseEc.getTraceId(), Reporter.detailString(baseRows, checkRows)));


            if (!cb.batch(logicalTableName, dbIndex, phyTable, baseEc, this, true, baseRows,
                checkRows)) {
                // Callback cancel the current batch of checking.
                throw GeneralUtil.nestedException("Checker retry batch");
            }
            if (CrossEngineValidator.isJobInterrupted(baseEc)) {
                long jobId = baseEc.getDdlJobId();
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + jobId + "' has been cancelled");
            }
            lowerBound = gsiUpperBound;
        } while (actualBatchSize.get() >= batchSize);

        long endTs = System.currentTimeMillis();
        SQLRecorderLogger.ddlLogger.warn(
            MessageFormat.format("[{0}] FastChecker batch rechecker finish phy for {1}[{2}] cost {3} ms",
                baseEc.getTraceId(),
                sourceBatch.getBatchBoundDesc(),
                checkType,
                endTs - startTs));
    }
}
