package com.alibaba.polardbx.executor.backfill;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.gsi.PhysicalPlanBuilder;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableList;
import groovy.lang.IntRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectWithPartition;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableGsiPkRangeBackfillTask.formatPkRows;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class BackfillStats {

    private long maxSampleRows = 200_000L;

    private float sampleRate = 0.05f;

    public String schemaName;
    public String logicalTableName;

    public TableMeta tableMeta;
    public PartitionInfo partitionInfo;

    public List<ColumnMeta> primaryKeyColumns;

    public List<Row> sampleRows;
    public int batchRows;
    public long batchSize;

    public long rangeRows;
    public long rangeSize;

    Map<Integer, ParameterContext> leftRow;
    Map<Integer, ParameterContext> rightRow;

    Boolean totalTable;

    public Boolean checkAndSplitLogicalTable(String schemaName, String logicalTableName, TableMeta tableMeta,
                                             List<SplitBound> splitPoints, Boolean enableSample,
                                             long maxPkRangeSize,
                                             long maxSampleRows) {
        if (enableSample) {
            if (tableMeta.getLocalPartitionDefinitionInfo() != null) {
                return false;
            }
            //tables with primary key absent not support (e.g. ugsi)
            if (!tableMeta.isHasPrimaryKey()) {
                return false;
            }
            this.setMaxSampleRows(maxSampleRows);
            this.prepareSampleRate();
            if (this.needSampleAndSplit(maxPkRangeSize)) {
                this.sampleLogicalTableAndSort();
                splitPoints.addAll(this.generateSplitBound(maxPkRangeSize));
                if (splitPoints.isEmpty()) {
                    long leastMaxSampleRows = (long) Math.ceil(maxPkRangeSize * 2.0 / this.rangeSize * this.rangeRows);
                    String errMsg = String.format(
                        "The sample rows is %d, sample rate is %f," +
                            "The range size is too large: %d, with the range rows: %d while the sample rows is restricted: %d, "
                            + "you can raise the BACKFILL_MAX_SAMPLE_ROWS value to %d", this.sampleRows.size(),
                        this.sampleRate,
                        this.rangeSize, this.rangeRows, maxSampleRows, leastMaxSampleRows);
                    SQLRecorderLogger.ddlLogger.info(errMsg);
                    return true;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    public static BackfillStats createForLogicalBackfill(String schemaName, String logicalTableName,
                                                         TableMeta tableMeta, long maxSampleRows) {
        BackfillStats backfillStats = new BackfillStats();
        backfillStats.schemaName = schemaName;
        backfillStats.logicalTableName = logicalTableName;
        backfillStats.tableMeta = tableMeta;
        backfillStats.maxSampleRows = maxSampleRows;
        backfillStats.primaryKeyColumns =
            new ArrayList<>((tableMeta.isHasPrimaryKey() ? tableMeta.getPrimaryIndex().getKeyColumns() :
                tableMeta.getGsiImplicitPrimaryKey()));
        backfillStats.sampleRows = new ArrayList<>();
        backfillStats.totalTable = true;
        backfillStats.leftRow = Transformer.buildColumnsParam(null);
        backfillStats.rightRow = Transformer.buildColumnsParam(null);
        return backfillStats;
    }

    public static BackfillStats createForLogicalBackfillPkRange(String schemaName, String logicalTableName,
                                                                TableMeta tableMeta, long maxSampleRows,
                                                                Map<Integer, ParameterContext> leftRow,
                                                                Map<Integer, ParameterContext> rightRow,
                                                                long rangeRows,
                                                                long rangeSize
    ) {
        BackfillStats backfillStats = createForLogicalBackfill(schemaName, logicalTableName, tableMeta, maxSampleRows);
        backfillStats.leftRow = leftRow;
        backfillStats.rightRow = rightRow;
        backfillStats.rangeRows = rangeRows;
        backfillStats.rangeSize = rangeSize;
        backfillStats.totalTable = false;
        return backfillStats;
    }

    public void setMaxSampleRows(long maxSamples) {
        this.maxSampleRows = maxSamples;
    }

    public void prepareSampleRate() {
        //SINCE NO LOGICAL TABLE NAME WITH quota IS ALLOWED, we march on in this way.
        if (totalTable) {
            final String COUNT_SQL =
                "SELECT `TABLE_ROWS`, `DATA_LENGTH` from `INFORMATION_SCHEMA`.`TABLES` WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'";
            String sql = String.format(COUNT_SQL, schemaName, logicalTableName);
            List<Map<String, Object>> results =
                DdlHelper.getServerConfigManager().executeQuerySql(sql, schemaName, null);
            if (results.size() > 0) {
                rangeRows = Long.parseLong(results.get(0).get("TABLE_ROWS").toString());
                rangeSize = Long.parseLong(results.get(0).get("DATA_LENGTH").toString());
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "could not find table rows for table " + logicalTableName);
            }
        }
        float maxSampleRate;
        if (rangeRows < maxSampleRows * 2) {
            maxSampleRate = 0.5f;
        } else {
            maxSampleRate = min((float) ((maxSampleRows + 1.0) / rangeRows), 0.5f);
        }
        this.sampleRate = maxSampleRate;
    }

    public Pair<Double, List<Row>> sampleLogicalTableAndSort() {
        // scan sampling
        double sampleRateUp;
        Boolean withLowerBound = GeneralUtil.isNotEmpty(leftRow);
        Boolean withUpperBound = GeneralUtil.isNotEmpty(rightRow);
        if (totalTable && !withLowerBound && !withUpperBound) {
            sampleRateUp = StatisticUtils.scanAnalyze(schemaName, logicalTableName, primaryKeyColumns, sampleRate,
                (int) maxSampleRows,
                sampleRows, false);
        } else {
            PhyTableOperation phyTableOperation =
                buildPhyTableOperationForSample(schemaName, logicalTableName, primaryKeyColumns,
                    tableMeta, withLowerBound, withUpperBound);
            List<ParameterContext> params = buildPkParameters(leftRow, rightRow, primaryKeyColumns.size());
            Map<Integer, ParameterContext> parameterContextMap = buildLogicalSelectWithParam(maxSampleRows,
                params, sampleRate,
                withLowerBound, withUpperBound);

            sampleRateUp =
                StatisticUtils.scanAnalyzeOnPkRange(schemaName, logicalTableName, primaryKeyColumns, sampleRate,
                    (int) maxSampleRows,
                    sampleRows, phyTableOperation, parameterContextMap);
            // scanAnalyzeOnPkRange is much accurate then sampleScan, so we should split based on the returned rows.
            if (sampleRows.size() <= 10000) {
                String msg = String.format(
                    "The sample rows is %d, which is too less within the range from (%s) to (%s) of logical table %s"
                        + " the sample rate is %f, max sample rows is %d", sampleRows.size(),
                    formatPkRows(leftRow, null), formatPkRows(rightRow, null), logicalTableName,
                    sampleRate, maxSampleRows);
                SQLRecorderLogger.ddlLogger.warn(msg);
            }
        }
        List<Integer> indexes = new IntRange(0, primaryKeyColumns.size() - 1).stream().collect(Collectors.toList());
        List<DataType> dataTypes = primaryKeyColumns.stream().map(ColumnMeta::getDataType).collect(Collectors.toList());
        Comparator comparator = ExecUtils.getComparatorForColumns(indexes, dataTypes, true);
        sampleRows.sort(comparator);
        return Pair.of(sampleRateUp, sampleRows);
    }

    public boolean needSampleAndSplit(long maxPkRangeSize) {
        return rangeSize >= maxPkRangeSize;
    }

    public static class SplitBound {
        public Map<Integer, ParameterContext> left;
        public Map<Integer, ParameterContext> right;
        public long rows;
        public long size;

        public SplitBound(Map<Integer, ParameterContext> left, Map<Integer, ParameterContext> right) {
            this.left = left;
            this.right = right;
        }

        public void setRightRow(Map<Integer, ParameterContext> right) {
            this.right = right;
        }

        public void setRowsAndSize(long rows, float sampleRate, long rowLength) {
            this.rows = (long) (rows / sampleRate);
            this.size = rowLength * this.rows;
        }
    }

    public List<SplitBound> generateSplitBound(long maxRangeSize) {
        float estimatedRangSize = sampleRows.size() / sampleRate * rangeSize / rangeRows;
        float ratio = estimatedRangSize / rangeSize;
        long splitCount = Math.max((long) Math.ceil((estimatedRangSize + 1.0) / maxRangeSize), 1L);
        if (splitCount >= sampleRows.size()) {
            String errMsg = String.format(
                "The sample rows is %d, sample rate is %f, and the estimated ratio is %f, " +
                    "The range size is too large: %d, with the range rows: %d while the sample rows is restricted: %d, "
                , this.sampleRows.size(), this.sampleRate, ratio,
                this.rangeSize, this.rangeRows, maxSampleRows);
            SQLRecorderLogger.ddlLogger.warn(errMsg);
            return Collections.emptyList();
        }
        int splitSize = (int) max(1, -Math.floorDiv(sampleRows.size(), -splitCount));
        List<SplitBound> splitBounds = new ArrayList<>();
        int lastRow = 0;
        int row = lastRow + splitSize;
        Map<Integer, ParameterContext> nullParamsRow = Transformer.buildColumnsParam(null);
        splitBounds.add(new SplitBound(leftRow, nullParamsRow));
        long rowLength = rangeSize / rangeRows;
        while (row < sampleRows.size() && lastRow < sampleRows.size()) {
            Map<Integer, ParameterContext> paramsRow = Transformer.buildColumnsParam(sampleRows.get(row));
            SplitBound splitBound = splitBounds.get(splitBounds.size() - 1);
            splitBound.setRightRow(paramsRow);
            splitBound.setRowsAndSize(row - lastRow, sampleRate, rowLength);
            splitBounds.get(splitBounds.size() - 1).setRightRow(paramsRow);
            splitBounds.add(new SplitBound(paramsRow, nullParamsRow));
            lastRow = row;
            row = lastRow + splitSize;
        }
        SplitBound splitBound = splitBounds.get(splitBounds.size() - 1);
        splitBound.setRightRow(rightRow);
        splitBound.setRowsAndSize(sampleRows.size() - lastRow, sampleRate, rowLength);
        batchRows = (int) Math.floor(splitSize / sampleRate);
        batchSize = (int) Math.floor((double) rangeSize / rangeRows * batchRows);
        return splitBounds;
    }

    public static PhyTableOperation buildPhyTableOperationForSample(String schemaName, String logicalTableName,
                                                                    List<ColumnMeta> primaryKeyColumns,
                                                                    TableMeta tableMeta,
                                                                    Boolean withLowerBound,
                                                                    Boolean withUpperBound) {
        final PhysicalPlanBuilder builder =
            new PhysicalPlanBuilder(schemaName, new ExecutionContext(schemaName));
        List<String> pkColumns = primaryKeyColumns.stream().map(o -> o.getName()).collect(Collectors.toList());

        PhyTableOperation phyTableOperation =
            builder.buildSelectPlanForSample(tableMeta, pkColumns, pkColumns, true, withLowerBound, withUpperBound);
        return phyTableOperation;
    }

    public static Map<Integer, ParameterContext> buildLogicalSelectWithParam(long batchSize,
                                                                             List<ParameterContext> params,
                                                                             double sampleRate, boolean withLowerBound,
                                                                             boolean withUpperBound) {
        Map<Integer, ParameterContext> planParams = new HashMap<>();

        int nextParamIndex = 1;

        // Parameters for where(DNF)
        int pkNumber = 0;
        if (withLowerBound || withUpperBound) {
            pkNumber = params.size() / ((withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0));
        }
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

        // Parameter for rand sample
        planParams.put(nextParamIndex,
            new ParameterContext(ParameterMethod.setDouble, new Object[] {nextParamIndex, sampleRate}));
        nextParamIndex++;

        // Parameters for limit
        planParams.put(nextParamIndex,
            new ParameterContext(ParameterMethod.setObject1, new Object[] {nextParamIndex, batchSize}));

        return planParams;
    }

    public static List<ParameterContext> buildPkParameters(Map<Integer, ParameterContext> leftRow,
                                                           Map<Integer, ParameterContext> rightRow, int pkNum) {
        List<ParameterContext> pkParams = new ArrayList<>();
        if (!leftRow.isEmpty()) {
            for (int i = 0; i < pkNum; i++) {
                pkParams.add(leftRow.get(i));
            }
        }
        if (!rightRow.isEmpty()) {
            for (int i = 0; i < pkNum; i++) {
                pkParams.add(rightRow.get(i));
            }
        }
        return pkParams;
    }
}
