package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-10-25 10:12
 **/
public class ImplicitTableGroupChecker {

    public ImplicitTableGroupChecker() {
        ImplicitTableGroupUtil.setTableGroupConfigProvider(new ImplicitTableGroupUtil.TableGroupConfigProvider() {

            @Override
            public boolean isNewPartitionDb(String schemaName) {
                return true;
            }

            @Override
            public boolean isCheckTgNameValue() {
                return true;
            }

            @Override
            public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, boolean fromDelta) {
                return buildTableGroupConfig(schemaName, tableName.toLowerCase());
            }

            @Override
            public TableGroupConfig getTableGroupConfig(String schemaName, String tableName, String gsiName,
                                                        boolean fromDelta) {
                return buildTableGroupConfig(schemaName, tableName.toLowerCase() + "." + gsiName.toLowerCase());
            }

            @SneakyThrows
            @Override
            public ImplicitTableGroupUtil.PartitionColumnInfo getPartitionColumnInfo(String schemaName,
                                                                                     String tableName) {
                Map<String, Set<String>> gsiPartitionColumns = new HashMap<>();
                Set<String> tablePartitionColumns = new HashSet<>();

                try (Connection connection = ConnectionManager.getInstance().getDruidMetaConnection()) {
                    try (Statement stmt = connection.createStatement()) {
                        ResultSet resultSet = stmt.executeQuery(
                            String.format("select * from indexes where table_schema = '%s' and table_name = '%s'"
                                    + " and index_status = 4 and visible = 0 and index_table_name <> '' and index_column_type = 0",
                                schemaName, tableName)
                        );

                        while (resultSet.next()) {
                            String gsiName = resultSet.getString("index_name");
                            gsiName = StringUtils.substringBeforeLast(gsiName, "_$").toLowerCase();
                            String columnName = resultSet.getString("column_name").toLowerCase();
                            Set<String> set = gsiPartitionColumns.computeIfAbsent(gsiName, k -> new HashSet<>());
                            set.add(columnName);
                        }
                    }
                }
                try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("use " + schemaName);
                        ResultSet resultSet = stmt.executeQuery(String.format("show partitions from %s", tableName));
                        while ((resultSet.next())) {
                            String partCols1 = resultSet.getString("PART_COL");
                            String partCols2 = resultSet.getString("SUBPART_COL");

                            if (StringUtils.isNotBlank(partCols1)) {
                                tablePartitionColumns.addAll(Arrays.asList(partCols1.toLowerCase().split(",")));
                            }
                            if (StringUtils.isNotBlank(partCols2)) {
                                tablePartitionColumns.addAll(Arrays.asList(partCols2.toLowerCase().split(",")));
                            }
                        }
                    }
                }

                return new ImplicitTableGroupUtil.PartitionColumnInfo(schemaName, tableName,
                    tablePartitionColumns, gsiPartitionColumns);
            }

            @SneakyThrows
            private TableGroupConfig buildTableGroupConfig(String schemaName, String key) {
                Map<String, TableGroupConfig> map = new HashMap<>();
                try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute("use " + schemaName);
                        ResultSet resultSet = stmt.executeQuery(String.format(
                            "select * from information_schema.FULL_TABLE_GROUP where TABLE_SCHEMA = '%s'", schemaName));

                        while (resultSet.next()) {
                            String tgName = StringUtils.lowerCase(resultSet.getString("TABLE_GROUP_NAME"));
                            int manualCreate = resultSet.getInt("IS_MANUAL_CREATE");
                            String tables = StringUtils.lowerCase(resultSet.getString("TABLES"));

                            String[] array = StringUtils.split(tables, ",");
                            for (String item : array) {
                                TableGroupRecord tableGroupRecord = new TableGroupRecord();
                                tableGroupRecord.tg_name = tgName;
                                tableGroupRecord.manual_create = manualCreate;
                                TableGroupConfig tableGroupConfig = new TableGroupConfig();
                                tableGroupConfig.setTableGroupRecord(tableGroupRecord);
                                map.put(item.trim(), tableGroupConfig);
                            }
                        }
                    }
                }
                return map.getOrDefault(key, null);
            }
        });
    }

    public boolean checkSql(String schemaName, String tableName, String sql, Set<String> tgGroups) {
        return ImplicitTableGroupUtil.checkSql(schemaName, tableName, sql, tgGroups);
    }

    public String attachImplicitTg(String schemaName, String tableName, String sql) {
        return ImplicitTableGroupUtil.tryAttachImplicitTableGroup(schemaName, tableName, sql);
    }
}
