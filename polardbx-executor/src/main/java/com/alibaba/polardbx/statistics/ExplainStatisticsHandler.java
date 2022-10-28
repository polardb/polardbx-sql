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

package com.alibaba.polardbx.statistics;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcUtils;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author shengyu
 */
public class ExplainStatisticsHandler {

    static String CARDINALITY = "cardinality";
    static String HISTOGRAM = "histogram";
    static String NULL_COUNT = "null_count";
    static String SAMPLE_RATE = "sample_rate";
    static String TOPN = "TOPN";
    static String COMPOSITE_CARDINALITY = "composite_cardinality";
    static String DEFAULT_SCHEMA = "defaltxxAPPName";

    private final ExecutionContext executionContext;
    private final StatisticsContext statisticsContext;
    private final ExecutionPlan executionPlan;
    private final String[] indent;
    public ExplainStatisticsHandler(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        this.executionContext = executionContext;
        this.executionPlan = executionPlan;
        statisticsContext = new StatisticsContext();
        indent = new String[4];
        indent[0] = "";
        indent[1] = "  ";
        indent[2] = "    ";
        indent[3] = "      ";
    }

    public static ResultCursor handleExplainStatistics(ExecutionContext executionContext, ExecutionPlan executionPlan) {
        ExplainStatisticsHandler explainStatisticsHandler =
            new ExplainStatisticsHandler(executionContext, executionPlan);
        return explainStatisticsHandler.explainStatistics();
    }

    private ResultCursor explainStatistics() {
        // schema -> table set
        Map<String, Set<String>> tablesUsed = new HashMap<>();

        getSql();
        getCreateTable(tablesUsed);
        getStatisticsMeta(tablesUsed);
        getConfig(tablesUsed);
        return buildResult();
    }

    private void getSql() {
        StringBuilder plan = new StringBuilder();

        String originalSql = executionContext.getOriginSql();
        Pattern p = Pattern.compile("explain statistics", Pattern.CASE_INSENSITIVE);
        Matcher m = p.matcher(originalSql);
        String result = m.replaceAll("");
        plan.append(indent[1]).append("- sql: |\n").append(indent[3]).
            append(result.replace("\n"," "));
        plan.append("\n").append(indent[2]).append("plan: |").append("\n");
        statisticsContext.setSql(plan.toString());
    }

    private void getCreateTable(Map<String, Set<String>> tablesUsed) {
        RelNode unOptimizedPlan = executionContext.getUnOptimizedPlan();
        TableScanFinder tsf = new TableScanFinder();

        unOptimizedPlan.accept(tsf);
        String baseSchema = executionContext.getSchemaName().toLowerCase();
        StringBuilder tableSchemas = new StringBuilder();

        // get ddl
        for (com.alibaba.polardbx.common.utils.Pair<String, TableScan> pair : tsf.getResult()) {
            TableScan tableScan = pair.getValue();
            if (tableScan instanceof LogicalView) {
                continue;
            }
            String fullTableName;
            String schemaName = CBOUtil.getTableMeta(tableScan.getTable()).getSchemaName();
            String tableName = CBOUtil.getTableMeta(tableScan.getTable()).getTableName();
            if (schemaName == null) {
                schemaName = baseSchema;
            }
            // make sure the table is not showed
            if (!tablesUsed.containsKey(schemaName)) {
                tablesUsed.put(schemaName, new HashSet<>());
            }
            if (!tablesUsed.get(schemaName).add(tableName)) {
                continue;
            }
            // get the full table name(ignore the schema name if it is schema in executionContext)
            if (baseSchema.equals(schemaName)) {
                fullTableName = tableName;
            } else {
                fullTableName = schemaName + "." + tableName;
            }
            // show create table result of full table name
            String sql = "show create table " + schemaName + "." + tableName + ";";
            ExecutionContext newExecutionContext = executionContext.copy();
            newExecutionContext.setTestMode(false);
            ExecutionPlan plan = Planner.getInstance().plan(sql, newExecutionContext);

            Cursor cursor = null;
            try {
                cursor = ExecutorHelper.execute(plan.getPlan(), newExecutionContext);

                Row row;
                while ((row = cursor.next()) != null) {
                    //ignore comment
                    List<SQLStatement> statements = FastsqlUtils.parseSql(row.getString(1));
                    StringBuilder sb = new StringBuilder();
                    for (SQLStatement statement : statements) {
                        if (statement instanceof MySqlCreateTableStatement) {
                            MySqlCreateTableStatement create = (MySqlCreateTableStatement) statement;
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
                        Engine engine;
                        if (Engine.isFileStore(engine = CBOUtil.getTableMeta(tableScan.getTable()).getEngine())) {
                            create.setEngine(engine.name());
                        }}
                        sb.append(statement.toString().replace("\n", " ")).append(";");
                    }
                    tableSchemas.append("\n").append(indent[1]).append(fullTableName).append(":\n")
                        .append(indent[2]).append(sb);
                }
            } finally {
                if (cursor != null) {
                    cursor.close(null);
                }
            }
        }
        statisticsContext.setTableSchemas(tableSchemas.toString());
    }

    private void getStatisticsMeta(Map<String, Set<String>> tablesUsed) {
        StringBuilder needanalyzeInfo = new StringBuilder();

        StringBuilder rowCountInfo = new StringBuilder();
        StringBuilder columnInfo = new StringBuilder();
        StringBuilder cardinalityInfo = new StringBuilder();

        for (Map.Entry<String, Set<String>> entry : tablesUsed.entrySet()) {
            //get statistics for each table
            Connection conn = null;
            PreparedStatement ps = null;
            ResultSet rs = null;
            try {
                String schema = entry.getKey();
                Set<String> schemaTables = entry.getValue();
                conn = MetaDbDataSource.getInstance().getDataSource().getConnection();
                if (conn == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DB_STATUS_EXECUTE, "conn is null");
                }
                //check row count
                Set<String> tablesWithRowCount = new HashSet<>();
                rs = conn.prepareStatement(checkRowCountSql(schema,schemaTables)).executeQuery();
                while (rs.next()) {
                    tablesWithRowCount.add(rs.getString("table_name"));
                }

                // table with full column statistics
                Set<String> tablesWithColumn = new HashSet<>();
                rs = conn.prepareStatement(checkColumnSql(schema,schemaTables)).executeQuery();
                while (rs.next()) {
                    String tableName = rs.getString("table_name").toLowerCase();
                    int columnCount = rs.getInt("column_count");
                    int validCount = 0;
                    for (ColumnMeta column : executionContext.getSchemaManager(schema).
                        getTable(tableName).getAllColumns()) {
                        if (!StatisticUtils.isBinaryOrJsonColumn(column)) {
                            validCount++;
                        }
                    }
                    if (validCount <= columnCount) {
                        tablesWithColumn.add(tableName);
                    }
                }

                // record table should be analyzed
                for (String tableName : schemaTables) {
                    if (tablesWithRowCount.contains(tableName) && tablesWithColumn.contains(tableName)) {
                        continue;
                    }
                    // table without rowcount or table without column statistics should be analyzed
                    if (sameSchema(schema)) {
                        needanalyzeInfo.append("\n").append(indent[1]).append("analyze table ")
                            .append(tableName).append(";");
                    } else {
                        needanalyzeInfo.append("\n").append(indent[1]).append("analyze table ").append(schema).
                            append(".").append(tableName).append(";");
                    }
                }

                //get row count
                rs = conn.prepareStatement(getRowCountSql(schema,schemaTables)).executeQuery();
                while (rs.next()) {
                    String tableName = rs.getString("table_name").toLowerCase();
                    long rowCount = rs.getLong("row_count");

                    if (sameSchema(schema)) {
                        rowCountInfo.append("\n").append(indent[1]).append(tableName)
                            .append(":\n").append(indent[2]).append(rowCount);
                    } else {
                        rowCountInfo.append("\n").append(indent[1]).append(schema).append(".").append(tableName).
                            append(":\n").append(indent[2]).append(rowCount);
                    }
                }

                //get column statistics
                rs = conn.prepareStatement(getColumnSql(schema,schemaTables)).executeQuery();
                while (rs.next()) {
                    String tableName = rs.getString("table_name").toLowerCase();
                    String column = rs.getString("column_name").toLowerCase();
                    // cardinality histogram null_count sample_rate TOPN
                    Map<String, Object> map = new TreeMap<>();
                    map.put(CARDINALITY, rs.getLong("cardinality"));
                    map.put(HISTOGRAM, rs.getString("histogram"));
                    map.put(NULL_COUNT, rs.getLong("null_count"));
                    map.put(SAMPLE_RATE, rs.getFloat("sample_rate"));
                    map.put(TOPN, rs.getString("TOPN"));

                    for (Map.Entry<String, Object> mapEntry : map.entrySet()) {
                        if (sameSchema(schema)) {
                            columnInfo.append("\n").append(indent[1]).append(tableName).append(".").
                                append(column).append(".").append(mapEntry.getKey()).append(":\n")
                                .append(indent[2]).append(mapEntry.getValue());
                        } else {
                            columnInfo.append("\n").append(indent[1]).append(schema).append(".").append(tableName).append(".").
                                append(column).append(".").append(mapEntry.getKey()).append(":\n")
                                .append(indent[2]).append(mapEntry.getValue());
                        }
                    }

                }

                // get composite cardinality, there is no such table in drds
                rs = conn.prepareStatement(getCardinalitySql(schema,schemaTables)).executeQuery();
                while (rs.next()) {
                    String tableName = rs.getString("table_name").toLowerCase();
                    String columns = rs.getString("column_names").toLowerCase();
                    long cardinality = rs.getLong("composite_cardinality");
                    if (sameSchema(schema)) {
                        cardinalityInfo.append("\n").append(indent[1]).append(tableName).append(".").
                            append(columns).append(".").append(COMPOSITE_CARDINALITY).append(":\n")
                            .append(indent[2]).append(cardinality);
                    } else {
                        cardinalityInfo.append("\n").append(indent[1]).append(schema).append(".").append(tableName).append(".").
                            append(columns).append(".").append(COMPOSITE_CARDINALITY).append(":\n")
                            .append(indent[2]).append(cardinality);
                    }
                }

                statisticsContext.setRowCount(rowCountInfo.toString());
                statisticsContext.setColumnStatistics(columnInfo.toString());
                statisticsContext.setCompositeCardinality(cardinalityInfo.toString());
                statisticsContext.setNeedAnalyze(needanalyzeInfo.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                JdbcUtils.close(rs);
                JdbcUtils.close(ps);
                JdbcUtils.close(conn);
            }
        }
    }

    private void getConfig(Map<String, Set<String>> tableNames) {
        StringBuilder config = new StringBuilder("\n");
        // get cmd_extra kv
        for (Map.Entry<String, Object> entry : executionContext.getExtraCmds().entrySet()) {
            String key = entry.getKey();
            if (executionContext.getDefaultExtraCmds() != null
                && executionContext.getDefaultExtraCmds().containsKey(key)) {
                config.append(indent[1]).append(key).append(":\n")
                    .append(indent[2]).append(executionContext.getDefaultExtraCmds().get(key)).append("\n");
            } else {
                config.append(indent[1]).append(key).append(":\n")
                    .append(indent[2]).append(entry.getValue()).append("\n");
            }
        }
        // it is assumed DEFAULT_SCHEMA is not used as any cmd_extra
        for (String schema : tableNames.keySet()) {
            if (DbInfoManager.getInstance().isNewPartitionDb(schema)) {
                config.append(indent[1]).append(sameSchema(schema)? DEFAULT_SCHEMA:schema).append(".isNew:\n")
                    .append(indent[2]).append(true).append("\n");
            } else {
                config.append(indent[1]).append(sameSchema(schema)? DEFAULT_SCHEMA:schema).append(".isNew:\n")
                    .append(indent[2]).append(false).append("\n");
            }
        }
        // get real db partition number
        for (Map.Entry<String, Set<String>> entry : tableNames.entrySet()) {
            String schemaName = entry.getKey();
            // don't calculate new partition
            if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                continue;
            }
            int countDB = -1;
            for (String tableName : entry.getValue()) {
                // don't show the topology of broadcast or single table
                if (executionContext.getSchemaManager(schemaName).getTddlRuleManager().isBroadCast(tableName)
                    || executionContext.getSchemaManager(schemaName).getTddlRuleManager().isBroadCast(tableName)) {
                    continue;
                }
                String sql = "show topology " + schemaName + "." + tableName + ";";
                ExecutionContext newExecutionContext = executionContext.copy();
                newExecutionContext.setTestMode(false);
                ExecutionPlan plan = Planner.getInstance().plan(sql, newExecutionContext);
                Cursor cursor = null;
                try {
                    cursor = ExecutorHelper.execute(plan.getPlan(), newExecutionContext);
                    Row row;
                    Set<String> groupName = new HashSet<>();
                    while ((row = cursor.next()) != null) {
                        String group = row.getString(1);
                        groupName.add(group);
                    }
                    countDB = groupName.size();
                    break;
                } finally {
                    if (cursor != null) {
                        cursor.close(new ArrayList<>());
                    }
                }

            }
            if (countDB > 0) {
                config.append(indent[1]).append(sameSchema(schemaName)? DEFAULT_SCHEMA: schemaName).append(".dbNumber:\n")
                    .append(indent[2]).append(countDB).append("\n");
            }
        }
        statisticsContext.setConfig(config.toString());
    }

    private ResultCursor buildResult() {
        ArrayResultCursor result = new ArrayResultCursor("statistics");

        result.addColumn("RESULT", DataTypes.StringType);
        result.addColumn("SUGGESTIONS", DataTypes.StringType);
        result.initMeta();

        if (!statisticsContext.isEmpty()) {
            result.addRow(new Object[] {

                "\n" + "SQL:\n" + statisticsContext.getSql()
                    +"\n" + "DDL:" +  statisticsContext.getTableSchemas()
                    +"\n" + "STATISTICS:" +statisticsContext.getRowCount()
                    + statisticsContext.getColumnStatistics()
                    + statisticsContext.getCompositeCardinality()
                    +"\n" + "CONFIG:" +  statisticsContext.getConfig(),
                statisticsContext.getNeedAnalyze()
            });
        } else {
            result.addRow(new Object[] {
                "",
                "  something goes wrong!\n"
            });
        }
        return result;
    }

    private boolean sameSchema(String schema) {
        return executionContext.getSchemaName().equalsIgnoreCase(schema);
    }

    static private String checkRowCountSql(String schema, Set<String> tables) {
        return "select distinct table_name from " + GmsSystemTables.TABLE_STATISTICS
            + " where schema_name='" + schema + "' and table_name in ('"
            + String.join("','", tables) + "');";
    }

    static private String getRowCountSql(String schema, Set<String> tables) {
        return "select table_name, row_count from " + GmsSystemTables.TABLE_STATISTICS
            + " where schema_name='" + schema + "' and table_name in ('"
            + String.join("','", tables) + "');";
    }

    static private String checkColumnSql(String schema, Set<String> tables) {
        return "select table_name, count(*) as column_count from " + GmsSystemTables.COLUMN_STATISTICS
                + " where table_name in ('"
                + String.join("','", tables) + "') group by table_name;";
    }

    static private String getColumnSql(String schema, Set<String> tables) {
        return "select table_name, column_name, cardinality, histogram, null_count, sample_rate, TOPN from "
                + GmsSystemTables.COLUMN_STATISTICS
                + " where schema_name='" + schema + "' and table_name in ('"
                + String.join("','", tables) + "');";
    }

    static private String getCardinalitySql(String schema, Set<String> tables) {
        return "select distinct table_name, column_names, composite_cardinality from " +
            GmsSystemTables.NDV_SKETCH_STATISTICS
            + " where schema_name='" + schema + "' and table_name in ('"
            + String.join("','", tables) + "');";
    }
    class StatisticsContext {
        String sql;
        String tableSchemas;
        String rowCount;
        String columnStatistics;
        String compositeCardinality;
        String config;
        String needAnalyze;

        public StatisticsContext() {
        }

        public String getSql() {
            return sql;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public String getTableSchemas() {
            return tableSchemas;
        }

        public void setTableSchemas(String tableSchemas) {
            this.tableSchemas = tableSchemas;
        }

        public String getRowCount() {
            return rowCount;
        }

        public void setRowCount(String rowCount) {
            this.rowCount = rowCount;
        }

        public String getColumnStatistics() {
            return columnStatistics;
        }

        public void setColumnStatistics(String cardinality) {
            this.columnStatistics = cardinality;
        }

        public String getCompositeCardinality() {
            return compositeCardinality;
        }

        public void setCompositeCardinality(String compositeCardinality) {
            this.compositeCardinality = compositeCardinality;
        }

        public String getConfig() {
            return config;
        }

        public void setConfig(String config) {
            this.config = config;
        }

        public String getNeedAnalyze() {
            return needAnalyze;
        }

        public void setNeedAnalyze(String needAnalyze) {
            this.needAnalyze = needAnalyze;
        }

        public boolean isEmpty() {
            return tableSchemas == null
                && rowCount == null
                && columnStatistics == null
                && needAnalyze == null;
        }

        public void print() {
            if (tableSchemas != null) {
                System.out.println(tableSchemas);
            }
            if (rowCount != null) {
                System.out.println(rowCount);
            }
            if (columnStatistics != null) {
                System.out.println(columnStatistics);
            }
            if (config != null) {
                System.out.println(config);
            }
            if (needAnalyze != null) {
                System.out.println(needAnalyze);
            }
        }
    }
}
