package com.alibaba.polardbx.executor.backfill;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.backfill.LogicalTableGsiPkRangeBackfillTask;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.fastjson.JSON.toJSONString;

public class BackfillSampleManager {
    Long jobId;
    Long taskId;
    String schemaName;
    String logicalTableName;

    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    List<String> columnNames;
    DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
    Map<Integer, ParameterContext> leftRow;
    Map<Integer, ParameterContext> rightRow;

    ConcurrentHashMap<Long, List<Object>> positionMarksMap;

    public static ConcurrentHashMap<Long, BackfillSampleManager> backfillSampleManagerMap = new ConcurrentHashMap<>();

    public static BackfillSampleManager loadBackfillSampleManager(String schemaName, String logicalTableName,
                                                                  Long jobId, Long taskId,
                                                                  Map<Integer, ParameterContext> leftRow,
                                                                  Map<Integer, ParameterContext> rightRow) {
        synchronized (backfillSampleManagerMap) {
            return backfillSampleManagerMap.computeIfAbsent(taskId,
                k -> new BackfillSampleManager(schemaName, logicalTableName, jobId, taskId, leftRow, rightRow));
        }
    }

    public static BackfillSampleManager loadBackfillSampleManager(Long taskId) {
        synchronized (backfillSampleManagerMap) {
            return backfillSampleManagerMap.get(taskId);
        }
    }

    public static Boolean removeBackfillSampleManager(Long taskId) {
        synchronized (backfillSampleManagerMap) {
            if (backfillSampleManagerMap.contains(taskId)) {
                backfillSampleManagerMap.remove(taskId);
                return true;
            }
            return false;
        }
    }

    public BackfillSampleManager(String schemaName, String logicalTableName,
                                 Long jobId, Long taskId, Map<Integer, ParameterContext> leftRow,
                                 Map<Integer, ParameterContext> rightRow
    ) {
        this.taskId = taskId;
        this.jobId = jobId;
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
//        this.columnNames = columnNames;
        this.leftRow = leftRow;
        this.rightRow = rightRow;
    }

    public Pair<List<Map<Integer, ParameterContext>>, List<Long>> loadSampleRows(long maxPkRangeSize,
                                                                                 long maxSampleRows, long batchRows,
                                                                                 long batchSize) {
        Pair<List<Map<Integer, ParameterContext>>, List<Long>> sampleRowsAndBackfillIds = querySampleRows();
        if (sampleRowsAndBackfillIds.getKey().isEmpty()) {
            List<Map<Integer, ParameterContext>> sampleRows =
                samplePkRangeFromLogicalTable(schemaName, logicalTableName,
                    columnNames, leftRow, rightRow, maxPkRangeSize, maxSampleRows, batchRows, batchSize);

//            SQLRecorderLogger.ddlLogger.warn(
//                MessageFormat.format(
//                    "[{0}] sample rows from pk range [{1}], result is [{2}]",
//                    taskId,
//                    com.alibaba.polardbx.executor.gsi.GsiUtils.rowsToString(Lists.newArrayList(leftRow, rightRow)),
//                    com.alibaba.polardbx.executor.gsi.GsiUtils.rowsToString(sampleRows)));
            List<Long> backfillIds = LogicalTableGsiPkRangeBackfillTask.generateBackfillIds(sampleRows.size() - 1);
            int affected = insertIgnoreSampleRow(sampleRows, backfillIds);
            if (affected <= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                    "insert GSI backfill sample rows failed! because there are concurrent task running on the same pk range!");
            }
            sampleRowsAndBackfillIds = Pair.of(sampleRows, backfillIds);
        }
        return sampleRowsAndBackfillIds;
    }

    public void updatePositionMark(Long backfillId, Reporter reporter, ExecutionContext ec,
                                   List<GsiBackfillManager.BackfillObjectBean> backfillObjects,
                                   long successRowCount, List<ParameterContext> lastPk,
                                   List<ParameterContext> beforeLastPk, boolean finished,
                                   Map<Long, Long> primaryKeysIdMap) {
        positionMarksMap.put(backfillId,
            Lists.newArrayList(reporter, ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished,
                primaryKeysIdMap));
    }

    public void flushPositionMark() {
//        for(Long backfillId : positionMarksMap.keySet()){
//            List<Object> positionMarkOperator = positionMarksMap.get(backfillId);
//            Reporter reporter = (Reporter) positionMarkOperator.get(0);
//            ExecutionContext ec = (ExecutionContext) positionMarkOperator.get(1);
//            List<GsiBackfillManager.BackfillObjectBean> backfillObjects = (List<GsiBackfillManager.BackfillObjectBean>) positionMarkOperator.get(2);
//            long successRowCount = (long) positionMarkOperator.get(3);
//            List<ParameterContext> lastPk = (List<ParameterContext>) positionMarkOperator.get(4);
//            List<ParameterContext> beforeLastPk = (List<ParameterContext>) positionMarkOperator.get(5);
//            boolean finished = (boolean) positionMarkOperator.get(6);
//            Map<Long, Long> primaryKeysIdMap = (Map<Long, Long>) positionMarkOperator.get(7);
//            reporter.addPositionMarkBatch(ec, backfillObjects, successRowCount, lastPk, beforeLastPk, finished, primaryKeysIdMap);
//        }
        // TODO: flush partition mark.
    }

    public void storeExecutionTime(Map<Long, String> executionTime) {
        String compressedExecutionTime = DdlHelper.compress(JSON.toJSONString(executionTime));
        String lastCompressedExecutionTime = queryStoredExecutionTime();
        if (StringUtils.isEmpty(lastCompressedExecutionTime)) {
            updateExecutionTime(compressedExecutionTime);
        } else {
            updateExecutionTime(lastCompressedExecutionTime + "\n" + compressedExecutionTime);
        }
    }

    public List<Map<Integer, ParameterContext>> samplePkRangeFromLogicalTable(String schemaName,
                                                                              String logicalTableName,
                                                                              List<String> columnNames,
                                                                              Map<Integer, ParameterContext> leftRow,
                                                                              Map<Integer, ParameterContext> rightRow,
                                                                              long maxPkRangeSize,
                                                                              long maxSampleRows,
                                                                              long rangeRows,
                                                                              long rangeSize) {
        TableMeta tableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        BackfillStats backfillStats =
            BackfillStats.createForLogicalBackfillPkRange(schemaName, logicalTableName, tableMeta,
                maxSampleRows, leftRow, rightRow, rangeRows, rangeSize);
        setColumnNames(backfillStats.primaryKeyColumns.stream().map(o -> o.getName()).collect(Collectors.toList()));
        List<BackfillStats.SplitBound> splitPoints = new ArrayList<>();
        backfillStats.setMaxSampleRows(maxSampleRows);
        backfillStats.checkAndSplitLogicalTable(schemaName, logicalTableName, tableMeta,
            splitPoints,
            true, maxPkRangeSize, maxSampleRows);
        List<Map<Integer, ParameterContext>> sampleRows = new ArrayList<>();
        for (BackfillStats.SplitBound splitBound : splitPoints) {
            sampleRows.add(splitBound.left);
        }
        if (splitPoints.size() >= 1) {
            sampleRows.add(splitPoints.get(splitPoints.size() - 1).right);
        } else {
//            Map<Integer, ParameterContext> nullParamsRow = Transformer.buildColumnsParam(null);
            sampleRows.add(leftRow);
            sampleRows.add(rightRow);
        }
        return sampleRows;
    }

    public interface Orm<T> {
        T convert(ResultSet resultSet) throws SQLException;

        Map<Integer, ParameterContext> params();
    }

    public static final class BackfillSampleRecord implements Orm<BackfillSampleRecord> {
        public static BackfillSampleRecord ORM = new BackfillSampleRecord();

        String schemaName;
        String logicalTableName;
        Long jobId;
        Long taskId;
        String columns;
        String sampleRows;
        String extra;
        String backfillIds;

        BackfillSampleRecord() {
        }

        BackfillSampleRecord(String schemaName, String logicalTableName, Long jobId, Long taskId, String columns,
                             String sampleRows,
                             String extra,
                             String backfillIds) {
            this.schemaName = schemaName;
            this.logicalTableName = logicalTableName;
            this.jobId = jobId;
            this.taskId = taskId;
            this.columns = columns;
            this.sampleRows = sampleRows;
            this.extra = extra;
            this.backfillIds = backfillIds;
        }

        @Override
        public BackfillSampleRecord convert(ResultSet resultSet) throws SQLException {
            final long jobId = resultSet.getLong("JOB_ID");
            final long taskId = resultSet.getLong("TASK_ID");
            final String tableSchema = resultSet.getString("SCHEMA_NAME");
            final String tableName = resultSet.getString("TABLE_NAME");
            final String columns = resultSet.getString("COLUMNS");
            final String sampleRows = resultSet.getString("SAMPLE_ROWS");
            final String extra = resultSet.getString("EXTRA");
            final String backfillIds = resultSet.getString("BACKFILL_IDS");

            return new BackfillSampleRecord(tableSchema,
                tableName,
                jobId,
                taskId,
                columns,
                sampleRows,
                extra,
                backfillIds);
        }

        @Override
        public Map<Integer, ParameterContext> params() {
            return Collections.emptyMap();
        }
    }

    private static final String SAMPLE_ROWS = "sampleRows";
    private static final String ROWS_NUMS = "rowsNums";
    private static final String FULL_COLUMNS =
        "`schema_name`, `table_name`, `job_id`, `task_id`, `columns`, `sample_rows`, `gmt_created`, `gmt_modified`, `extra`, `backfill_ids`";

    private static final String COLUMNS =
        "`schema_name`, `table_name`, `job_id`, `task_id`, `columns`, `sample_rows`, `extra`, `backfill_ids`";

    private static final String EXTRA_COLUMN =
        "extra";

    private static final String SQL_INSERT_IGNORE_BACKFILL_SAMPLE_ROWS = "INSERT IGNORE INTO "
        + GmsSystemTables.BACKFILL_SAMPLE_ROWS + "( " + COLUMNS + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String WHERE_PK = " WHERE `schema_name` = ? AND `table_name` = ? AND `task_id` = ?";

    private static final String UPDATE_BACKFILL_SAMPLE_ROWS = "UPDATE  "
        + GmsSystemTables.BACKFILL_SAMPLE_ROWS + " set " + EXTRA_COLUMN + " = ? " + WHERE_PK;

    private static final String SQL_QUERY_BACKFILL_SAMPLE_ROWS = "SELECT " + COLUMNS + " FROM "
        + GmsSystemTables.BACKFILL_SAMPLE_ROWS + WHERE_PK;

    private static final String SQL_QUERY_EXECUTION_TIME = "SELECT " + EXTRA_COLUMN + " FROM "
        + GmsSystemTables.BACKFILL_SAMPLE_ROWS + WHERE_PK;

    public static String toJsonString(List<Map<Integer, ParameterContext>> sampleRows) {
        List<Map<Integer, List<String>>> valueContent = new ArrayList<>();
        for (Map<Integer, ParameterContext> row : sampleRows) {
            Map<Integer, List<String>> value = toValue(row);
            valueContent.add(value);
        }
        Map<String, Object> storedValueContent = new HashMap<>();
        storedValueContent.put(SAMPLE_ROWS, valueContent);
        storedValueContent.put(ROWS_NUMS, valueContent.size());
        return JSON.toJSONString(storedValueContent, SerializerFeature.DisableCircularReferenceDetect);
    }

    public static Map<Integer, List<String>> toValue(Map<Integer, ParameterContext> parameterContextMap) {
        Map<Integer, List<String>> value = new HashMap<>();
        for (Map.Entry<Integer, ParameterContext> entry : parameterContextMap.entrySet()) {
            ParameterContext context = entry.getValue();
            value.put(entry.getKey(), Lists.newArrayList(context.getParameterMethod().name(),
                Transformer.serializeParam(context)));
        }
        return value;
    }

    public static Map<Integer, ParameterContext> fromValue(Map<Integer, List<String>> value) {
        Map<Integer, ParameterContext> row = new HashMap<>();
        for (Map.Entry<Integer, List<String>> entry : value.entrySet()) {
            int index = Integer.parseInt(String.valueOf(entry.getKey()));
            ParameterContext context = Transformer.buildParamByType(index, entry.getValue().get(0),
                entry.getValue().get(1));
            row.put(index, context);
        }
        return row;
    }

    public static List<Map<Integer, ParameterContext>> fromJsonString(String jsonString) {
        List<Map<Integer, List<String>>> valueContent = new ArrayList<>();
        Map<String, Object> storedValueContent = new HashMap<>();
        storedValueContent = JSON.parseObject(jsonString, storedValueContent.getClass());
        valueContent = (List<Map<Integer, List<String>>>) storedValueContent.get(SAMPLE_ROWS);
        List<Map<Integer, ParameterContext>> sampleRows = new ArrayList<>();
        for (Map<Integer, List<String>> value : valueContent) {
            Map<Integer, ParameterContext> row = fromValue(value);
            sampleRows.add(row);
        }
        return sampleRows;
    }

    public BackfillSampleRecord toBackfillSampleRowRecord(
        List<Map<Integer, ParameterContext>> backfillSampleRows, String schemaName, String logicalTable,
        Collection<String> columns, Long jobId,
        Long taskId, List<Long> backfillIds) {
        String valueContent = toJsonString(backfillSampleRows);
        String compressedSampleRows = DdlHelper.compress(valueContent);
        String columnString = StringUtils.join(columns, ",");
        String backfillIdString = StringUtils.join(backfillIds, ",");
        String extra = "";
        BackfillSampleRecord backfillSampleRecord = new BackfillSampleRecord(
            schemaName,
            logicalTable,
            jobId,
            taskId,
            columnString,
            compressedSampleRows,
            extra,
            backfillIdString
        );
        return backfillSampleRecord;
    }

    public Pair<List<Map<Integer, ParameterContext>>, List<Long>> fromBackfillSampleRowRecord(
        BackfillSampleRecord record) {

        List<Map<Integer, ParameterContext>> backfillSampleRows = new ArrayList<>();
        String valueContent = DdlHelper.decompress(record.sampleRows);
        backfillSampleRows = fromJsonString(valueContent);
        List<Long> backfillIds =
            Arrays.stream(record.backfillIds.split(",")).map(Long::valueOf).collect(Collectors.toList());
        return Pair.of(backfillSampleRows, backfillIds);
    }

    Pair<List<Map<Integer, ParameterContext>>, List<Long>> querySampleRows() {
        Pair<List<Map<Integer, ParameterContext>>, List<Long>> sampleRows =
            Pair.of(new ArrayList<>(), new ArrayList<>());
        final List<BackfillSampleRecord> backfillSampleRecords = new ArrayList<>();

        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, schemaName}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, logicalTableName}));
        params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, taskId}));

        GsiUtils.wrapWithTransaction(dataSource, (conn) -> {
                try {
                    backfillSampleRecords.addAll(
                        query(SQL_QUERY_BACKFILL_SAMPLE_ROWS, params, conn, BackfillSampleRecord.ORM));
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        "insert GSI backfill sample rows failed!");
                }
            }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "insert GSI backfill sample rows failed!")
        );
        if (!backfillSampleRecords.isEmpty()) {
            sampleRows = fromBackfillSampleRowRecord(backfillSampleRecords.get(0));
        }
        return sampleRows;
    }

    String queryStoredExecutionTime() {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, schemaName}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, logicalTableName}));
        params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, taskId}));
        List<String> executionTimes = new ArrayList<>();

        GsiUtils.wrapWithTransaction(dataSource, (conn) -> {
                try {
                    executionTimes.addAll(
                        queryExecutionTime(SQL_QUERY_EXECUTION_TIME, params, conn));
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        " query GSI execution Time failed!");
                }
            }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "query GSI execution Time failed!")
        );
        if (!executionTimes.isEmpty()) {
            return executionTimes.get(0);
        } else {
            return "";
        }
    }

    public int insertIgnoreSampleRow(List<Map<Integer, ParameterContext>> backfillSampleRows, List<Long> backfillIds) {

        Map<Integer, ParameterContext> params = new HashMap<>();
        BackfillSampleRecord record =
            toBackfillSampleRowRecord(backfillSampleRows, schemaName, logicalTableName, columnNames, jobId,
                taskId, backfillIds);
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, record.schemaName}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, record.logicalTableName}));
        params.put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {3, record.jobId}));
        params.put(4, new ParameterContext(ParameterMethod.setLong, new Object[] {4, record.taskId}));
        params.put(5, new ParameterContext(ParameterMethod.setString, new Object[] {5, record.columns}));
        params.put(6, new ParameterContext(ParameterMethod.setString, new Object[] {6, record.sampleRows}));
        params.put(7, new ParameterContext(ParameterMethod.setString, new Object[] {7, record.extra}));
        params.put(8, new ParameterContext(ParameterMethod.setString, new Object[] {8, record.backfillIds}));

        final int[] affected = {0};
        GsiUtils.wrapWithTransaction(dataSource, (conn) -> {
                try {
                    affected[0] = update(SQL_INSERT_IGNORE_BACKFILL_SAMPLE_ROWS, Lists.newArrayList(params), conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        "insert GSI backfill sample rows failed!");
                }
            }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "insert GSI backfill sample rows failed!")
        );
        return affected[0];
    }

    protected int update(String sql, List<Map<Integer, ParameterContext>> params, Connection connection)
        throws SQLException {
        final int batchSize = 512;
        final int[] totalAffected = {0};
        for (int i = 0; i < params.size(); i += batchSize) {
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                for (int j = 0; j < batchSize && i + j < params.size(); j++) {
                    Map<Integer, ParameterContext> batch = params.get(i + j);
                    ParameterMethod.setParameters(ps, batch);
                    ps.addBatch();
                }
                int[] affected = ps.executeBatch();
                Arrays.stream(affected).forEach(o -> totalAffected[0] += o);
            }
        }
        return totalAffected[0];
    }

    private <T> List<T> query(String sql, Map<Integer, ParameterContext> params, Connection connection,
                              BackfillSampleManager.Orm<T> orm)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ParameterMethod.setParameters(ps, params);

            final ResultSet rs = ps.executeQuery();

            final List<T> result = new ArrayList<>();
            while (rs.next()) {
                result.add(orm.convert(rs));
            }
            return result;
        }
    }

    private List<String> queryExecutionTime(String sql, Map<Integer, ParameterContext> params, Connection connection)
        throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ParameterMethod.setParameters(ps, params);

            final ResultSet rs = ps.executeQuery();

            final List<String> result = new ArrayList<>();
            while (rs.next()) {
                result.add(rs.getString(EXTRA_COLUMN));
            }
            return result;
        }
    }

    public int updateExecutionTime(String executionTime) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        params.put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, executionTime}));
        params.put(2, new ParameterContext(ParameterMethod.setString, new Object[] {2, schemaName}));
        params.put(3, new ParameterContext(ParameterMethod.setString, new Object[] {3, logicalTableName}));
        params.put(4, new ParameterContext(ParameterMethod.setLong, new Object[] {4, taskId}));

        final int[] affected = {0};
        GsiUtils.wrapWithTransaction(dataSource, (conn) -> {
                try {
                    affected[0] = update(UPDATE_BACKFILL_SAMPLE_ROWS, Lists.newArrayList(params), conn);
                } catch (SQLException e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE,
                        e,
                        "insert execution time  failed!");
                }
            }, (e) -> new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_EXECUTE, e,
                "insert execution time failed!")
        );
        return affected[0];
    }
}
