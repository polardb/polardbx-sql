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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.MemoryNotEnoughException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.SystemPropertiesHelper;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.operator.spill.AsyncFileSingleBufferSpiller;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.view.DrdsSystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.type.SqlTypeName;
import org.eclipse.jetty.util.StringUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * usage:
 * {sql} into outfile '{fileName}' statistics.
 * <p>
 * 1. {sql} must start with 'select', and can't be 'select XX union select XX'.
 * 2.The {fileName} is a relative path under spill/temp directory.
 * 3.If {fileName} ends with '.sql', it will output a file can use 'source {fileName}' to mock environment.
 * Otherwise, it will dump a single file which can be used in test-framework
 */
public class OutFileStatisticsCursor extends OutFileCursor {

    private static Set<String> ignoreSessionVariables = ImmutableSet.<String>builder()
        .add(ConnectionProperties.SERVER_ID.toLowerCase())
        .add(ConnectionProperties.CONN_POOL_PROPERTIES.toLowerCase())
        .build();
    /**
     * whether to print catalog only
     */
    private boolean catalogOnly;

    /**
     * real sql part, will be used in explain cost_trace and explain simple
     */
    private String targetSql;

    ColumnMeta iColumnMeta;
    List<Row> bufferRows;

    boolean closed;

    private static final String SQL_PATTERN = ".*[sS][qQ][lL]";

    private static final String USE_DB = "use %s;";

    private static final String SHOW_DB = "show create database %s;";

    private static final String CREATE_FILESTORE =
        "create filestorage if not exists local_disk with ('file_uri' ='file:///tmp/orc/');";

    private static final String SHOW_TB = "show create table %s.%s;";

    private static final String SET_GLOBAL = "set global %s = '%s'";

    private static final String SET_SESSION = "set session %s = '%s'";

    private static final String CREATE_TABLEGROUP = "CREATE TABLEGROUP IF NOT EXISTS %s";

    private static final String DROP_VIEW_IF_EXISTS = "DROP VIEW IF EXISTS `%s`";
    private static final String CREATE_VIEW = "CREATE VIEW `%s` AS %s";
    private static final String SELECT_TBL =
        "SELECT SCHEMA_NAME, TABLE_NAME, ROW_COUNT, UNIX_TIMESTAMP(GMT_MODIFIED) AS UNIX_TIME"
            + " FROM " + GmsSystemTables.TABLE_STATISTICS + " where `SCHEMA_NAME` = '%s' AND `TABLE_NAME` in (%s)";

    private static final String DELETE_TBL =
        "DELETE FROM metadb." + GmsSystemTables.TABLE_STATISTICS
            + " WHERE `SCHEMA_NAME` = '%s' AND `TABLE_NAME` in (%s)";

    private static final String INSERT_TBL =
        "INSERT INTO metadb." + GmsSystemTables.TABLE_STATISTICS + " (`SCHEMA_NAME`, `TABLE_NAME`, "
            + "`ROW_COUNT`) VALUES %s";

    private static final String SELECT_COL =
        "SELECT SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, CARDINALITY, CMSKETCH, HISTOGRAM, TOPN, NULL_COUNT, SAMPLE_RATE, UNIX_TIMESTAMP(GMT_MODIFIED) AS UNIX_TIME"
            + " FROM " + GmsSystemTables.COLUMN_STATISTICS
            + " where `SCHEMA_NAME` = '%s' and `TABLE_NAME` in (%s)";

    private static final String DELETE_COL =
        "DELETE FROM metadb." + GmsSystemTables.COLUMN_STATISTICS
            + " WHERE `SCHEMA_NAME` = '%s' AND `TABLE_NAME` in (%s)";

    private static final String INSERT_COL =
        "INSERT INTO metadb." + GmsSystemTables.COLUMN_STATISTICS + " (`SCHEMA_NAME`, `TABLE_NAME`, "
            + "`COLUMN_NAME`, `CARDINALITY`, `CMSKETCH`, `HISTOGRAM`, `TOPN`, `NULL_COUNT`, `SAMPLE_RATE`) VALUES %s";

    private static final String SELECT_NDV =
        "SELECT `SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, `SHARD_PART`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_TYPE`, `GMT_CREATED` FROM "
            + GmsSystemTables.NDV_SKETCH_STATISTICS
            + " where `SCHEMA_NAME` = '%s' and `TABLE_NAME` in (%s)";

    private static final String DELETE_NDV =
        "DELETE FROM metadb." + GmsSystemTables.NDV_SKETCH_STATISTICS
            + " WHERE `SCHEMA_NAME` = '%s' AND `TABLE_NAME` in (%s)";

    private static final String INSERT_NDV =
        "INSERT INTO metadb." + GmsSystemTables.NDV_SKETCH_STATISTICS
            + " (`SCHEMA_NAME`, `TABLE_NAME`, `COLUMN_NAMES`, "
            + "`SHARD_PART`, `DN_CARDINALITY`, `COMPOSITE_CARDINALITY`, `SKETCH_BYTES`, `SKETCH_TYPE`, `GMT_CREATED`) VALUES %s;";
    private static final String RELOAD_STA = "reload statistics;";

    /**
     * batch size for insert statistics
     */
    private static final int BATCH_SIZE = 100;

    private static final String[] indent = new String[] {"", "  ", "    "};

    /**
     * whether return the content of file to client, For test only
     */
    private final boolean enableReturnContent;

    private StringBuilder contentRecord;

    public OutFileStatisticsCursor(
        ExecutionContext context, SpillerFactory spillerFactory, OutFileParams outFileParams) {
        super(false);
        TddlTypeFactoryImpl factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RelDataType dataType = factory.createSqlType(SqlTypeName.VARCHAR);
        this.enableReturnContent =
            context.getParamManager().getBoolean(ConnectionParams.SELECT_INTO_OUTFILE_STATISTICS_DUMP);
        if (this.enableReturnContent) {
            this.contentRecord = new StringBuilder();
            Field field = new Field(null, "RESULT", dataType);
            ColumnMeta c = new ColumnMeta(null, "RESULT", null, field);
            this.cursorMeta = CursorMeta.build(ImmutableList.of(c));
        } else {
            this.cursorMeta = CalciteUtils.buildDmlCursorMeta();
        }
        Field col = new Field(null, "sql", dataType);
        this.iColumnMeta = new ColumnMeta(null, "sql", null, col);

        outFileParams.setColumnMeata(Lists.newArrayList(iColumnMeta));
        SpillMonitor spillMonitor = context.getQuerySpillSpaceMonitor();
        this.context = context;
        singleStreamSpiller = (AsyncFileSingleBufferSpiller)
            spillerFactory.getStreamSpillerFactory().create(
                spillMonitor.tag(), Lists.newArrayList(iColumnMeta.getDataType()), spillMonitor.newLocalSpillMonitor(),
                outFileParams);
        String memoryName = getClass().getSimpleName() + "@" + System.identityHashCode(this);
        this.pool = context.getMemoryPool().getOrCreatePool(
            memoryName,
            context.getParamManager().getLong(ConnectionParams.SPILL_OUTPUT_MAX_BUFFER_SIZE),
            MemoryType.OPERATOR);
        memoryAllocator = pool.getMemoryAllocatorCtx();
        this.bufferRows = new ArrayList<>();
        this.catalogOnly = Pattern.matches(SQL_PATTERN, outFileParams.getFileName());
    }

    @Override
    protected Row doNext() {
        if (closed) {
            return null;
        }

        if (!catalogOnly) {
            String originalSql = context.getOriginSql();
            Pattern p = Pattern.compile("into outfile .* statistics", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(originalSql);
            targetSql = m.replaceAll("").replaceAll("\n", " ").trim();
            getExplainCostTrace();
            getExplainSimple();
        }

        getCatalog();

        // shuffle all rows
        if (bufferRows.size() > 0) {
            affectRow += bufferRows.size();
            spillFuture = singleStreamSpiller.spillRows(bufferRows.iterator(), memoryAllocator.getReservedAllocated());
        }

        getSpillFuture();
        memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);

        if (singleStreamSpiller != null) {
            singleStreamSpiller.closeWriter(false);
        }

        this.closed = true;

        if (enableReturnContent) {
            return new ArrayRow(this.cursorMeta, new Object[] {contentRecord.toString()});
        }
        ArrayRow arrayRow = new ArrayRow(this.cursorMeta, new Object[] {affectRow});
        return arrayRow;

    }

    void getExplainCostTrace() {
        consumeString(indent[0] + "-");
        consumeString(indent[1] + "STATISTIC_TRACE: |");
        ExecutionContext newExecutionContext = context.copy();
        newExecutionContext.newStatement();
        newExecutionContext.setParams(new Parameters());
        ExecutionPlan executionPlan =
            Planner.getInstance().plan("explain cost_trace " + targetSql, newExecutionContext);
        Cursor resultCursor = null;
        try {
            resultCursor = PlanExecutor.execute(executionPlan, newExecutionContext);
            Row row;
            while ((row = resultCursor.next()) != null) {
                String trace = row.getString(0);
                if (!trace.startsWith("STATISTIC TRACE INFO:\n")) {
                    continue;
                }
                String[] info = trace.split("\n");
                for (int i = 1; i < info.length; i++) {
                    outputTrace(info[i]);
                }
            }
        } finally {
            if (resultCursor != null) {
                resultCursor.close(new ArrayList<>());
            }
        }

    }

    // print explain simple
    void getExplainSimple() {
        consumeString(indent[1] + "SQL: |");
        consumeString(indent[2] + targetSql);
        consumeString(indent[1] + "PLAN: |");
        ExecutionContext newExecutionContext = context.copy();
        newExecutionContext.newStatement();
        newExecutionContext.setParams(new Parameters());
        ExecutionPlan executionPlan = Planner.getInstance().plan("explain simple " + targetSql, newExecutionContext);
        Cursor resultCursor = null;
        try {
            resultCursor = PlanExecutor.execute(executionPlan, newExecutionContext);
            Row row;
            while ((row = resultCursor.next()) != null) {
                outputPlan(row.getString(0));
            }
        } finally {
            if (resultCursor != null) {
                resultCursor.close(new ArrayList<>());
            }
        }
    }

    /**
     * print catalog
     */
    void getCatalog() {
        if (!catalogOnly) {
            consumeString(indent[1] + "CATALOG: |");
        }
        Map<String, SourceInUsed> sources = getSources();

        // set global variables
        // etc auto partition count
        prepareGlobalVariables();

        // create database
        prepareCreateDatabase(sources);

        // create local_disk
        prepareFileStore(sources);

        // create table
        prepareCreateTable(sources);

        // create view
        prepareCreateView(sources);

        // statistics
        prepareStatistics(sources);

        // reload statistics
        reloadStatistics(sources);

        // set session variables
        prepareSessionVariables();
    }

    private void outputTrace(String plan) {
        consumeString(indent[2] + plan);
    }

    private void outputPlan(String plan) {
        consumeString(indent[2] + plan);
    }

    private void outputCatalog(String sql) {
        if (!sql.endsWith(";")) {
            sql = sql + ";";
        }
        if (catalogOnly) {
            consumeString(sql);
        } else {
            consumeString(indent[2] + sql);
        }
    }

    /**
     * where we actually write outfile
     *
     * @param info the sql to be written
     */
    private void consumeString(String info) {
        try {
            long rowSize = info.length();
            bufferRows.add(new ArrayRow(new String[] {info}, rowSize));
            if (enableReturnContent) {
                contentRecord.append(info).append("\n");
            }
            memoryAllocator.allocateReservedMemory(rowSize);
        } catch (MemoryNotEnoughException t) {
            affectRow += bufferRows.size();
            spillFuture =
                singleStreamSpiller.spillRows(bufferRows.iterator(), memoryAllocator.getReservedAllocated());
            getSpillFuture();
            bufferRows = new ArrayList<>();
            memoryAllocator.releaseReservedMemory(memoryAllocator.getReservedAllocated(), true);
        } catch (Throwable t) {
            throw new TddlNestableRuntimeException(t);
        }
    }

    /**
     * collect all tables used in the sql
     */
    private Map<String, SourceInUsed> getSources() {
        Map<String, SourceInUsed> sources = new HashMap<>();

        Map<String, Set<String>> tablesUsed = new HashMap<>();
        RelNode unOptimizedPlan = context.getUnOptimizedPlan();
        TableScanFinder tsf = new TableScanFinder();
        unOptimizedPlan.accept(tsf);
        String baseSchema = context.getSchemaName().toLowerCase();

        for (Pair<String, TableScan> pair : tsf.getResult()) {
            TableScan tableScan = pair.getValue();
            if (tableScan instanceof LogicalView) {
                continue;
            }
            TableMeta meta = CBOUtil.getTableMeta(tableScan.getTable());
            final String schemaName = StringUtil.isEmpty(meta.getSchemaName()) ? baseSchema : meta.getSchemaName();
            String tableName = meta.getTableName();
            tablesUsed.computeIfAbsent(schemaName, x -> new HashSet<>());
            tablesUsed.get(schemaName).add(tableName);
        }

        for (Map.Entry<String, Set<String>> entry : tablesUsed.entrySet()) {
            sources.computeIfAbsent(entry.getKey(), x -> new SourceInUsed());
            sources.get(entry.getKey()).setTables(entry.getValue());
        }

        Map<String, Set<String>> views = PlannerContext.getPlannerContext(unOptimizedPlan).getViewMap();
        for (Map.Entry<String, Set<String>> entry : views.entrySet()) {
            sources.computeIfAbsent(entry.getKey(), x -> new SourceInUsed());
            sources.get(entry.getKey()).setViews(entry.getValue());
        }

        return sources;
    }

    /**
     * set global variables that can't be set in session
     */
    public void prepareGlobalVariables() {
        outputCatalog(String.format(SET_GLOBAL, ConnectionProperties.AUTO_PARTITION_PARTITIONS,
            DynamicConfig.getInstance().getAutoPartitionPartitions()));
    }

    public void prepareCreateDatabase(Map<String, SourceInUsed> sources) {
        for (String schemaName : sources.keySet()) {
            ExecutionContext newExecutionContext = context.copy();
            newExecutionContext.setTestMode(false);
            ExecutionPlan plan = Planner.getInstance().plan(String.format(SHOW_DB, schemaName), newExecutionContext);

            Cursor cursor = null;
            try {
                cursor = ExecutorHelper.execute(plan.getPlan(), newExecutionContext);
                Row row;
                while ((row = cursor.next()) != null) {
                    List<SQLStatement> statements = FastsqlUtils.parseSql(row.getString(1));
                    for (SQLStatement statement : statements) {
                        if (statement instanceof SQLCreateDatabaseStatement) {
                            SQLCreateDatabaseStatement create = (SQLCreateDatabaseStatement) statement;
                            create.setIfNotExists(true);
                            create.setComment(null);
                        }
                        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                            outputCatalog(statement.toString() + " MODE = 'auto'");
                        } else {
                            outputCatalog(statement.toString());
                        }
                    }
                }
            } finally {
                if (cursor != null) {
                    cursor.close(null);
                }
            }
        }
    }

    public void prepareFileStore(Map<String, SourceInUsed> sources) {
        AtomicBoolean withFileStore = new AtomicBoolean(false);
        for (Map.Entry<String, SourceInUsed> entry : sources.entrySet()) {
            SchemaManager schemaManager = context.getSchemaManager(entry.getKey());
            entry.getValue().getTables().ifPresent(tables ->
            {
                for (String tableName : tables) {
                    TableMeta tm = schemaManager.getTable(tableName);
                    if (tm != null && Engine.isFileStore(tm.getEngine())) {
                        withFileStore.set(true);
                    }
                }
            });
        }
        if (withFileStore.get()) {
            outputCatalog(CREATE_FILESTORE);
        }
    }

    public void prepareCreateTable(Map<String, SourceInUsed> sources) {
        for (Map.Entry<String, SourceInUsed> entry : sources.entrySet()) {
            String schemaName = entry.getKey();

            entry.getValue().getTables().ifPresent(tables ->
            {
                outputCatalog(String.format(USE_DB, schemaName));

                List<String> tableDef = Lists.newArrayList();
                for (String tableName : tables) {
                    // show create table result of full table name
                    ExecutionContext newExecutionContext = context.copy();
                    newExecutionContext.setTestMode(false);
                    ExecutionPlan plan =
                        Planner.getInstance().plan(String.format(SHOW_TB, schemaName, tableName), newExecutionContext);

                    Cursor cursor = null;
                    try {
                        cursor = ExecutorHelper.execute(plan.getPlan(), newExecutionContext);
                        Row row;
                        while ((row = cursor.next()) != null) {
                            // format sql
                            List<SQLStatement> statements = FastsqlUtils.parseSql(row.getString(1));
                            for (SQLStatement statement : statements) {
                                if (statement instanceof MySqlCreateTableStatement) {
                                    MySqlCreateTableStatement create = (MySqlCreateTableStatement) statement;
                                    create.setIfNotExiists(true);
                                    //ignore table comment
                                    create.setComment(null);
                                    for (SQLTableElement element : create.getTableElementList()) {
                                        // ignore column comment
                                        if (element instanceof SQLColumnDefinition) {
                                            ((SQLColumnDefinition) element).setComment((String) null);
                                        }
                                        // ignore index comment
                                        if (element instanceof MySqlKey) {
                                            ((MySqlKey) element).setComment(null);
                                        }
                                    }
                                    // with oss table, replace engine=oss to engine=local_disk
                                    Engine engine =
                                        context.getSchemaManager(schemaName).getTable(tableName).getEngine();
                                    if (Engine.isFileStore(engine)) {
                                        create.setEngine(Engine.LOCAL_DISK.name());
                                    }
                                    // with table group, add create tablegroup
                                    if (create.getTableGroup() != null) {
                                        outputCatalog(String.format(CREATE_TABLEGROUP,
                                            create.getTableGroup().getSimpleName()));
                                    }

                                }
                                tableDef.add(statement.toString().replace("\n", " ")
                                    .replace("\t", " "));
                            }
                        }
                    } finally {
                        if (cursor != null) {
                            cursor.close(null);
                        }
                    }
                }
                tableDef.forEach(this::outputCatalog);
            });
        }
    }

    public void prepareCreateView(Map<String, SourceInUsed> sources) {
        for (Map.Entry<String, SourceInUsed> entry : sources.entrySet()) {
            String schemaName = entry.getKey();
            entry.getValue().getViews().ifPresent(views -> {
                outputCatalog(String.format(USE_DB, schemaName));
                for (String view : views) {
                    ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();
                    if (viewManager == null) {
                        continue;
                    }
                    DrdsSystemTableView.Row row = viewManager.select(view);
                    outputCatalog(String.format(DROP_VIEW_IF_EXISTS, row.getViewName()));
                    outputCatalog(String.format(CREATE_VIEW, row.getViewName(),
                        row.getViewDefinition().replace("\n", " ").replace("\t", " ")));
                }
            });
        }
    }

    public void prepareStatistics(Map<String, SourceInUsed> sources) {
        try (Connection conn = MetaDbDataSource.getInstance().getDataSource().getConnection()) {
            for (Map.Entry<String, SourceInUsed> entry : sources.entrySet()) {
                String schemaName = entry.getKey();
                if (!entry.getValue().getTables().isPresent()) {
                    continue;
                }
                Set<String> tables = entry.getValue().getTables().get();

                List<String> tableList = tables.stream().map(x -> "'" + x + "'").collect(Collectors.toList());

                BatchRowConsumer consumer;
                // table statistics
                outputCatalog(String.format(DELETE_TBL, schemaName, String.join(",", tableList)));
                consumer = new BatchRowConsumer(BATCH_SIZE, INSERT_TBL);
                ResultSet rs = conn.prepareStatement(String.format(SELECT_TBL, schemaName, String.join(",", tableList)))
                    .executeQuery();
                while (rs.next()) {
                    List<String> param = new ArrayList<>(3);
                    param.add("'" + rs.getString("SCHEMA_NAME") + "'");
                    param.add("'" + rs.getString("TABLE_NAME") + "'");
                    param.add(String.valueOf(rs.getLong("row_count")));
                    consumer.append(String.join(",", param));
                }
                rs.close();
                consumer.flush();

                // column statistics
                outputCatalog(String.format(DELETE_COL, schemaName, String.join(",", tableList)));
                consumer = new BatchRowConsumer(BATCH_SIZE, INSERT_COL);
                rs = conn.prepareStatement(String.format(SELECT_COL, schemaName, String.join(",", tableList)))
                    .executeQuery();
                while (rs.next()) {
                    List<String> param = new ArrayList<>(9);
                    String currentSchema = rs.getString("SCHEMA_NAME");
                    String currentTable = rs.getString("TABLE_NAME");
                    String currentColumn = rs.getString("COLUMN_NAME");

                    param.add("'" + currentSchema + "'");
                    param.add("'" + currentTable + "'");
                    param.add("'" + currentColumn + "'");
                    param.add(String.valueOf(rs.getLong("CARDINALITY")));
                    param.add("'" + rs.getString("CMSKETCH") + "'");

                    boolean ignoreColumn = ignoreStringColumn(currentSchema, currentTable, currentColumn);
                    param.add(ignoreColumn ? "''" : "'" + rs.getString("HISTOGRAM") + "'");

                    if (ignoreColumn || StringUtils.isEmpty(rs.getString("TOPN"))) {
                        param.add(null);
                    } else {
                        param.add("'" + rs.getString("TOPN") + "'");
                    }
                    param.add(String.valueOf(rs.getLong("NULL_COUNT")));
                    param.add(String.valueOf(rs.getFloat("SAMPLE_RATE")));
                    consumer.append(String.join(",", param));
                }
                rs.close();
                consumer.flush();

                // ndv sketch
                outputCatalog(String.format(DELETE_NDV, schemaName, String.join(",", tableList)));
                consumer = new BatchRowConsumer(BATCH_SIZE, INSERT_NDV);
                rs = conn.prepareStatement(String.format(SELECT_NDV, schemaName, String.join(",", tableList)))
                    .executeQuery();
                while (rs.next()) {
                    List<String> param = new ArrayList<>(9);
                    param.add("'" + rs.getString("SCHEMA_NAME") + "'");
                    param.add("'" + rs.getString("TABLE_NAME") + "'");
                    param.add("'" + rs.getString("COLUMN_NAMES") + "'");
                    param.add("'" + rs.getString("SHARD_PART") + "'");
                    param.add(String.valueOf(rs.getLong("DN_CARDINALITY")));
                    param.add(String.valueOf(rs.getLong("COMPOSITE_CARDINALITY")));
                    param.add("''");
                    param.add("'" + rs.getString("SKETCH_TYPE") + "'");
                    param.add("from_unixtime(" + (rs.getTimestamp("GMT_CREATED").getTime() / 1000) + ")");
                    consumer.append(String.join(",", param));
                }
                rs.close();
                consumer.flush();
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * check whether to ignore the column
     *
     * @param currentSchema schema of the column
     * @param currentTable table of the column
     * @param currentColumn column name
     * @return true if enable ignore string and the column is a string
     */
    private boolean ignoreStringColumn(String currentSchema, String currentTable, String currentColumn) {
        // ignore string column histogram
        if (!context.getParamManager().getBoolean(ConnectionParams.STATISTICS_DUMP_IGNORE_STRING)) {
            return false;
        }
        if (currentSchema != null && currentTable != null && currentColumn != null) {
            TableMeta tableMeta = OptimizerContext.getContext(currentSchema)
                .getLatestSchemaManager().getTableWithNull(currentTable);
            if (tableMeta != null) {
                ColumnMeta columnMeta = tableMeta.getColumn(currentColumn);
                if (columnMeta != null) {
                    if (StatisticUtils.isStringColumn(columnMeta)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void reloadStatistics(Map<String, SourceInUsed> sources) {
        outputCatalog(RELOAD_STA);
    }

    public void prepareSessionVariables() {
        // get cmd_extra kv
        for (String key : context.getExtraCmds().keySet()) {
            if (!SystemPropertiesHelper.getConnectionProperties().contains(key.toUpperCase())) {
                continue;
            }
            if (ignoreSessionVariables.contains(key.toLowerCase())) {
                continue;
            }
            Object value = context.getExtraCmds().get(key);
            if (context.getDefaultExtraCmds() != null
                && context.getDefaultExtraCmds().containsKey(key)) {
                value = context.getDefaultExtraCmds().get(key);
                if (value == null) {
                    // use default value
                    continue;
                }
            }
            outputCatalog(String.format(SET_SESSION, key, value));
        }
    }

    static class SourceInUsed {
        Set<String> tables;
        Set<String> views;

        // todo: support udf
        Set<String> udf;

        public SourceInUsed() {
        }

        public Optional<Set<String>> getTables() {
            return Optional.ofNullable(tables);
        }

        public void setTables(Set<String> tables) {
            this.tables = tables;
        }

        public Optional<Set<String>> getViews() {
            return Optional.ofNullable(views);
        }

        public void setViews(Set<String> views) {
            this.views = views;
        }

        public Optional<Set<String>> getUdf() {
            return Optional.ofNullable(udf);
        }

        public void setUdf(Set<String> udf) {
            this.udf = udf;
        }
    }

    class BatchRowConsumer {
        private final int batchSize;

        private int currentSize;
        private final String sql;
        private StringBuilder sb;

        public BatchRowConsumer(int batchSize, String sql) {
            Preconditions.checkArgument(batchSize > 0);
            this.batchSize = batchSize;
            this.sql = sql;
            this.currentSize = 0;
            this.sb = new StringBuilder();
        }

        void append(String value) {
            if (currentSize > 0) {
                sb.append(",");
            }
            sb.append("(").append(value).append(")");
            currentSize++;
            if (currentSize >= batchSize) {
                flush();
                sb = new StringBuilder();

            }
        }

        void flush() {
            if (currentSize > 0) {
                outputCatalog(String.format(sql, sb));
                currentSize = 0;
            }
        }
    }

    public CursorMeta getCursorMeta() {
        return cursorMeta;
    }
}