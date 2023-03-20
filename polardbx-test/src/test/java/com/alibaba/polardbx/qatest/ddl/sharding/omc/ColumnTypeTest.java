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

package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.constant.GsiConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETORY;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_GEOMETRYCOLLECTION;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_ID;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_JSON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_LINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTILINESTRING;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_MULTIPOLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POINT;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_POLYGON;
import static com.alibaba.polardbx.qatest.constant.TableConstant.C_SET;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.DEFAULT_PARTITIONING_DEFINITION;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;
import static org.junit.Assert.assertTrue;

public class ColumnTypeTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    // Supported charsets and collations are from CharsetFactoryImpl
    private static final String[][] CHARSET_PARAMS = new String[][] {
        new String[] {"big5", "big5_chinese_ci"},
        new String[] {"big5", "big5_bin"},

        new String[] {"latin1", "latin1_german1_ci"},
        new String[] {"latin1", "latin1_swedish_ci"},
        new String[] {"latin1", "latin1_danish_ci"},
        new String[] {"latin1", "latin1_german2_ci"},
        new String[] {"latin1", "latin1_bin"},
        new String[] {"latin1", "latin1_general_ci"},
        new String[] {"latin1", "latin1_general_cs"},
        new String[] {"latin1", "latin1_spanish_ci"},

        new String[] {"ascii", "ascii_general_ci"},
        new String[] {"ascii", "ascii_bin"},

        new String[] {"gbk", "gbk_chinese_ci"},
        new String[] {"gbk", "gbk_bin"},

        new String[] {"utf8", "utf8_general_ci"},
        new String[] {"utf8", "utf8_bin"},
        new String[] {"utf8", "utf8_unicode_ci"},
        new String[] {"utf8", "utf8_general_mysql500_ci"},

        new String[] {"utf8mb4", "utf8mb4_general_ci"},
        new String[] {"utf8mb4", "utf8mb4_bin"},
        new String[] {"utf8mb4", "utf8mb4_unicode_ci"},
        new String[] {"utf8mb4", "utf8mb4_unicode_520_ci"},

        new String[] {"utf16", "utf16_general_ci"},
        new String[] {"utf16", "utf16_bin"},
        new String[] {"utf16", "utf16_unicode_ci"},

        new String[] {"utf16le", "utf16le_general_ci"},
        new String[] {"utf16le", "utf16le_bin"},

        new String[] {"utf32", "utf32_general_ci"},
        new String[] {"utf32", "utf32_bin"},
        new String[] {"utf32", "utf32_unicode_ci"},
    };

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";
    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String DISABLE_OMC_CHECKER = "OMC_CHECK_AFTER_BACK_FILL=FALSE";
    private static final String SELECT_COLUMN_TYPE = "select COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, "
        + "CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, "
        + "CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE "
        + "from information_schema.columns "
        + "where table_name = \"%s\" and column_name = \"%s\"";

    @Test
    public void testModifyCharset() throws SQLException {
        String tableName = "omc_modify_charset";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql = String.format("create table %s (a int primary key, b varchar(20))", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        final String selectColumnType = String.format(SELECT_COLUMN_TYPE, tableName, "b");
        for (int i = 0; i < CHARSET_PARAMS.length; i++) {
            String alterSql =
                String.format("alter table %s modify column b varchar(20) character set %s collate %s", tableName,
                    CHARSET_PARAMS[i][0], CHARSET_PARAMS[i][1]);
            System.out.println(CHARSET_PARAMS[i][0] + " " + CHARSET_PARAMS[i][1]);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql + USE_OMC_ALGORITHM, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            assertTrue(assertSameTypeInfo(selectColumnType));
        }
    }

    @Test
    public void testModifyAllType() throws SQLException {
        String tableName = "omc_modify_all_type";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql = String.format("create table %s (a int primary key, b int) charset=utf8mb4", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);

        String hint = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION);

        final String selectColumnType = String.format(SELECT_COLUMN_TYPE, tableName, "b");
        for (String columnDef : COLUMN_DEF_MAP.values()) {
            String columnType = columnDef.substring(columnDef.lastIndexOf('`') + 1);
            columnType = columnType.substring(0, columnType.length() - 2);

            System.out.println(columnType);
            String alterSql = String.format("alter table %s modify column b %s", tableName, columnType);
            execDdlWithRetry(tddlDatabase1, tableName, hint + alterSql + USE_OMC_ALGORITHM, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            assertTrue(assertSameTypeInfo(selectColumnType));
        }
    }

    private boolean assertSameTypeInfo(String selectColumnType) throws SQLException {
        ResultSet tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, selectColumnType);
        ResultSet mysqlRs = JdbcUtil.executeQuerySuccess(mysqlConnection, selectColumnType);
        ResultSetMetaData rsmd = tddlRs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        StringBuilder tddlType = new StringBuilder();
        StringBuilder mysqlType = new StringBuilder();

        tddlRs.next();
        mysqlRs.next();
        boolean same = true;
        for (int i = 0; i < columnCount; i++) {
            String tddlCol = tddlRs.getString(i + 1);
            String mysqlCol = tddlRs.getString(i + 1);
            tddlType.append(tddlCol == null ? "null" : tddlCol).append(",");
            mysqlType.append(mysqlCol == null ? "null" : mysqlCol).append(",");
            if (!(tddlCol != null && tddlCol.equalsIgnoreCase(mysqlCol) || (isZeroOrNull(tddlCol)
                && isZeroOrNull(mysqlCol)))) {
                same = false;
            }
        }

        if (!same) {
            System.out.println(tddlType);
            System.out.println(mysqlType);
        }
        return same;
    }

    private boolean isZeroOrNull(String col) {
        return col == null || col.equalsIgnoreCase("0");
    }

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Test
    public void convertToNotNullableColumnTest() throws SQLException {
        String tableName = "omc_not_null_tbl_test";
        try (Connection conn = getPolardbxConnection()) {
            String createSql =
                String.format("create table %s (a int primary key, b int not null) dbpartition by hash(a)", tableName);
            JdbcUtil.executeUpdateSuccess(conn, createSql);
            String alterSql = String.format("alter table %s modify column b int not null", tableName);

            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("STRICT_TRANS_TABLES", conn);
            JdbcUtil.executeUpdateSuccess(conn,
                buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + alterSql + USE_OMC_ALGORITHM);
            setSqlMode("", conn);
            JdbcUtil.executeUpdateFailed(conn, buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + alterSql + USE_OMC_ALGORITHM,
                "");

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        }
    }

    @Ignore
    @Test
    public void fullTypeConversionTest() throws SQLException {
        String tableName = "omc_full_type_conversion_tbl";
        String tableDef = ExecuteTableSelect.getFullTypeTableDef(tableName, DEFAULT_PARTITIONING_DEFINITION);
        tableDef = tableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        tableDef = tableDef.replace("NOT NULL", "");
        tableDef = tableDef.replace(" timestamp ", " timestamp NULL ");
        tableDef = tableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        tableDef = tableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        tableDef = tableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");
        String refTableName = "omc_full_type_conversion_tbl_ref";
        String refTableDef = ExecuteTableSelect.getFullTypeTableDef(refTableName, DEFAULT_PARTITIONING_DEFINITION);
        refTableDef = refTableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        refTableDef = refTableDef.replace("NOT NULL", "");
        refTableDef = refTableDef.replace(" timestamp ", " timestamp NULL ");
        refTableDef = refTableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        refTableDef = refTableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        refTableDef = refTableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");

        Set<String> notSupportTypes = new HashSet<>();
        notSupportTypes.add(C_ID);
        notSupportTypes.add(C_SET);
        notSupportTypes.add(C_JSON);
        notSupportTypes.add(C_GEOMETORY);
        notSupportTypes.add(C_GEOMETRYCOLLECTION);
        notSupportTypes.add(C_LINESTRING);
        notSupportTypes.add(C_MULTILINESTRING);
        notSupportTypes.add(C_MULTIPOINT);
        notSupportTypes.add(C_MULTIPOLYGON);
        notSupportTypes.add(C_POINT);
        notSupportTypes.add(C_POLYGON);

        Map<String, List<String>> inserts = new HashMap<>(GsiConstant.buildGsiFullTypeTestInserts(tableName));
        for (String type : notSupportTypes) {
            inserts.remove(type);
        }

        String col = "COL_NAME";
        Set<String> allInsertSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : inserts.entrySet()) {
            for (String insert : entry.getValue()) {
                allInsertSet.add(insert.replace(entry.getKey(), col));
            }
        }

        List<String> allInserts = new ArrayList<>(allInsertSet);
        List<String> refAllInserts = new ArrayList<>();
        for (String insert : allInserts) {
            refAllInserts.add(insert.replace(tableName, refTableName));
        }

        System.out.println(allInserts);

        Set<String> columns = inserts.keySet();

        String alterSqlTmpl = "alter table %s modify column %s %s";

        String deleteSql = "delete from " + tableName + " where 1=1";
        String refDeleteSql = "delete from " + refTableName + " where 1=1";

        try (Connection conn = getPolardbxConnection()) {
            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            for (String columnDef : COLUMN_DEF_MAP.values()) {
                dropTableIfExists(conn, tableName);
                dropTableIfExists(conn, refTableName);
                JdbcUtil.executeUpdateSuccess(conn, tableDef);
                JdbcUtil.executeUpdateSuccess(conn, refTableDef);

                String targetColumnType = columnDef.substring(columnDef.lastIndexOf('`') + 1);
                String targetColumnName = columnDef.substring(columnDef.indexOf('`') + 1, columnDef.lastIndexOf('`'));
                System.out.println(targetColumnName);

                // Alter all to target column
                for (String column : columns) {
                    if (targetColumnName.equals(column)) {
                        continue;
                    }

                    System.out.println(column + " " + targetColumnName);

                    // First we clear table
                    JdbcUtil.executeUpdateSuccess(conn, deleteSql);
                    JdbcUtil.executeUpdateSuccess(conn, refDeleteSql);

                    // Then we insert values
                    allInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));
                    refAllInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));

                    // Now do some alter
                    String alterSql =
                        buildCmdExtra(OMC_FORCE_TYPE_CONVERSION) + String.format(alterSqlTmpl,
                            tableName, column, targetColumnType) + USE_OMC_ALGORITHM;
                    String refAlterSql = String.format(alterSqlTmpl, refTableName, column, targetColumnType);
                    JdbcUtil.executeUpdateSuccess(conn, alterSql);
                    JdbcUtil.executeUpdateSuccess(conn, refAlterSql);

                    // Finally, assert same
                    String selectSql = "select " + column + " from " + tableName + " order by id";
                    String refSelectSql = "select " + column + " from " + refTableName + " order by id";
                    try {
                        selectContentSameAssertWithDiffSql(selectSql, refSelectSql, null, conn, conn, false, true,
                            true);
                    } catch (AssertionError e) {
                        System.out.println(
                            "================================================================================");
                        System.out.printf("source type: %s; target type: %s %n", column, targetColumnName);
                        System.out.println(e);
                        System.out.println(
                            "================================================================================");
                    }
                }
            }

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        }
    }

    @Ignore
    @Test
    public void fullTypeConversionTestFastPath() throws SQLException {
        String tableName = "omc_full_type_conversion_fp_tbl";
        String tableDef = ExecuteTableSelect.getFullTypeTableDef(tableName, DEFAULT_PARTITIONING_DEFINITION);
        tableDef = tableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        tableDef = tableDef.replace("NOT NULL", "");
        tableDef = tableDef.replace(" timestamp ", " timestamp NULL ");
        tableDef = tableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        tableDef = tableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        tableDef = tableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");
        String refTableName = "omc_full_type_conversion_fp_tbl_ref";
        String refTableDef = ExecuteTableSelect.getFullTypeTableDef(refTableName, DEFAULT_PARTITIONING_DEFINITION);
        refTableDef = refTableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        refTableDef = refTableDef.replace("NOT NULL", "");
        refTableDef = refTableDef.replace(" timestamp ", " timestamp NULL ");
        refTableDef = refTableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        refTableDef = refTableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        refTableDef = refTableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");

        Set<String> notSupportTypes = new HashSet<>();
        notSupportTypes.add(C_ID);
        notSupportTypes.add(C_SET);
        notSupportTypes.add(C_JSON);
        notSupportTypes.add(C_GEOMETORY);
        notSupportTypes.add(C_GEOMETRYCOLLECTION);
        notSupportTypes.add(C_LINESTRING);
        notSupportTypes.add(C_MULTILINESTRING);
        notSupportTypes.add(C_MULTIPOINT);
        notSupportTypes.add(C_MULTIPOLYGON);
        notSupportTypes.add(C_POINT);
        notSupportTypes.add(C_POLYGON);

        Map<String, List<String>> inserts = new HashMap<>(GsiConstant.buildGsiFullTypeTestInserts(tableName));
        for (String type : notSupportTypes) {
            inserts.remove(type);
        }

        String col = "COL_NAME";
        Set<String> allInsertSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : inserts.entrySet()) {
            for (String insert : entry.getValue()) {
                allInsertSet.add(insert.replace(entry.getKey(), col));
            }
        }

        List<String> allInserts = new ArrayList<>(allInsertSet);
        List<String> refAllInserts = new ArrayList<>();
        for (String insert : allInserts) {
            refAllInserts.add(insert.replace(tableName, refTableName));
        }

        System.out.println(allInserts);

        for (String type : notSupportTypes) {
            inserts.remove(type);
        }

        Set<String> columns = inserts.keySet();

        String alterSqlTmpl = "alter table %s modify column %s %s";

        String deleteSql = "delete from " + tableName + " where 1=1";
        String refDeleteSql = "delete from " + refTableName + " where 1=1";

        try (Connection conn = getPolardbxConnection()) {
            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            for (String columnDef : COLUMN_DEF_MAP.values()) {
                String targetColumnType = columnDef.substring(columnDef.lastIndexOf('`') + 1);
                String targetColumnName = columnDef.substring(columnDef.indexOf('`') + 1, columnDef.lastIndexOf('`'));

                if (notSupportTypes.contains(targetColumnName)) {
                    continue;
                }

                dropTableIfExists(conn, tableName);
                dropTableIfExists(conn, refTableName);
                JdbcUtil.executeUpdateSuccess(conn, tableDef);
                JdbcUtil.executeUpdateSuccess(conn, refTableDef);

                System.out.println(targetColumnName);

                // Alter all to target column
                for (String column : columns) {
                    if (targetColumnName.equals(column)) {
                        continue;
                    }
                    // First we clear table
                    JdbcUtil.executeUpdateSuccess(conn, deleteSql);
                    JdbcUtil.executeUpdateSuccess(conn, refDeleteSql);

                    System.out.println(column + " " + targetColumnName);

                    allInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));
                    JdbcUtil.executeUpdateSuccess(conn,
                        "update " + tableName + " set " + targetColumnName + "=" + column); // Update

                    String refAlterSql = String.format(alterSqlTmpl, refTableName, column, targetColumnType);
                    refAllInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));
                    JdbcUtil.executeUpdateSuccess(conn, refAlterSql); // Alter

                    String selectSql = "select " + targetColumnName + " from " + tableName + " order by id";
                    String refSelectSql = "select " + column + " from " + refTableName + " order by id";

                    try {
                        selectContentSameAssertWithDiffSql(selectSql, refSelectSql, null, conn, conn, false, true,
                            true);
                    } catch (AssertionError e) {
                        System.out.println(
                            "================================================================================");
                        System.out.printf("source type: %s; target type: %s %n", column, targetColumnName);
                        System.out.println(e);
                        System.out.println(
                            "================================================================================");
                    }
                }
            }

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        }
    }

    @Ignore
    @Test
    public void supportedTypeConversionTest() throws SQLException {
        String tableName = "omc_supp_type_conversion_tbl";
        String tableDef = ExecuteTableSelect.getFullTypeTableDef(tableName, DEFAULT_PARTITIONING_DEFINITION);
        tableDef = tableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        tableDef = tableDef.replace("NOT NULL", "");
        tableDef = tableDef.replace(" timestamp ", " timestamp NULL ");
        tableDef = tableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        tableDef = tableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        tableDef = tableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");
        String refTableName = "omc_supp_type_conversion_tbl_ref";
        String refTableDef = ExecuteTableSelect.getFullTypeTableDef(refTableName, DEFAULT_PARTITIONING_DEFINITION);
        refTableDef = refTableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        refTableDef = refTableDef.replace("NOT NULL", "");
        refTableDef = refTableDef.replace(" timestamp ", " timestamp NULL ");
        refTableDef = refTableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        refTableDef = refTableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        refTableDef = refTableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");

        Set<String> notSupportTypes = new HashSet<>();
        notSupportTypes.add(C_ID);
        notSupportTypes.add(C_SET);
        notSupportTypes.add(C_JSON);
        notSupportTypes.add(C_GEOMETORY);
        notSupportTypes.add(C_GEOMETRYCOLLECTION);
        notSupportTypes.add(C_LINESTRING);
        notSupportTypes.add(C_MULTILINESTRING);
        notSupportTypes.add(C_MULTIPOINT);
        notSupportTypes.add(C_MULTIPOLYGON);
        notSupportTypes.add(C_POINT);
        notSupportTypes.add(C_POLYGON);

        Map<String, List<String>> inserts = new HashMap<>(GsiConstant.buildGsiFullTypeTestInserts(tableName));
        for (String type : notSupportTypes) {
            inserts.remove(type);
        }

        String col = "COL_NAME";
        Set<String> allInsertSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : inserts.entrySet()) {
            for (String insert : entry.getValue()) {
                allInsertSet.add(insert.replace(entry.getKey(), col));
            }
        }

        List<String> allInserts = new ArrayList<>(allInsertSet);
        List<String> refAllInserts = new ArrayList<>();
        for (String insert : allInserts) {
            refAllInserts.add(insert.replace(tableName, refTableName));
        }

        System.out.println(allInserts);

        Set<String> columns = inserts.keySet();

        String alterSqlTmpl = "alter table %s modify column %s %s";

        String deleteSql = "delete from " + tableName + " where 1=1";
        String refDeleteSql = "delete from " + refTableName + " where 1=1";

        try (Connection conn = getPolardbxConnection()) {
            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            for (String columnDef : COLUMN_DEF_MAP.values()) {
                String targetColumnType = columnDef.substring(columnDef.lastIndexOf('`') + 1);
                String targetColumnName = columnDef.substring(columnDef.indexOf('`') + 1, columnDef.lastIndexOf('`'));

                dropTableIfExists(conn, tableName);
                dropTableIfExists(conn, refTableName);
                JdbcUtil.executeUpdateSuccess(conn, tableDef);
                JdbcUtil.executeUpdateSuccess(conn, refTableDef);

                System.out.println(targetColumnName);

                // Alter all to target column
                for (String column : columns) {
                    Pair<String, String> pair = new Pair<>(column, targetColumnName);
                    if (targetColumnName.equals(column)) {
                        continue;
                    }

                    System.out.println(column + " " + targetColumnName);

                    // First we clear table
                    JdbcUtil.executeUpdateSuccess(conn, deleteSql);
                    JdbcUtil.executeUpdateSuccess(conn, refDeleteSql);

                    // Then we insert values
                    allInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));
                    refAllInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));

                    // Now do some alter
                    String alterSql =
                        String.format(alterSqlTmpl, tableName, column, targetColumnType) + USE_OMC_ALGORITHM;
                    String refAlterSql = String.format(alterSqlTmpl, refTableName, column, targetColumnType);
                    try {
                        JdbcUtil.executeUpdateSuccess(conn, alterSql);
                    } catch (AssertionError e) {
//                        System.out.println("================================================================================");
//                        System.out.printf("source type: %s; target type: %s %n", column, targetColumnName);
//                        System.out.println(e);
//                        System.out.println("================================================================================");
                        continue;
                    }
                    JdbcUtil.executeUpdateSuccess(conn, refAlterSql);

                    // Finally, assert same
                    String selectSql = "select " + column + " from " + tableName + " order by " + column;
                    String refSelectSql = "select " + column + " from " + refTableName + " order by " + column;
                    selectContentSameAssertWithDiffSql(selectSql, refSelectSql, null, conn, conn, false, true, true);
                }
            }

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        }
    }

    @Ignore
    @Test
    public void fullTypeConversionColumnMultiWriteTest() throws SQLException {
        String tableName = "omc_full_type_conversion_cmw_tbl";
        String tableDef = ExecuteTableSelect.getFullTypeTableDef(tableName, DEFAULT_PARTITIONING_DEFINITION);
        tableDef = tableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        tableDef = tableDef.replace("NOT NULL", "");
        tableDef = tableDef.replace(" timestamp ", " timestamp NULL ");
        tableDef = tableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        tableDef = tableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        tableDef = tableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");
        String refTableName = "omc_full_type_conversion_cmw_tbl_ref";
        String refTableDef = ExecuteTableSelect.getFullTypeTableDef(refTableName, DEFAULT_PARTITIONING_DEFINITION);
        refTableDef = refTableDef.replace("CURRENT_TIMESTAMP", "'0000-00-00 00:00:00'");
        refTableDef = refTableDef.replace("NOT NULL", "");
        refTableDef = refTableDef.replace(" timestamp ", " timestamp NULL ");
        refTableDef = refTableDef.replace(" timestamp(1) ", " timestamp(1) NULL ");
        refTableDef = refTableDef.replace(" timestamp(3) ", " timestamp(3) NULL ");
        refTableDef = refTableDef.replace(" timestamp(6) ", " timestamp(6) NULL ");

        Set<String> notSupportTypes = new HashSet<>();
        notSupportTypes.add(C_ID);
        notSupportTypes.add(C_SET);
        notSupportTypes.add(C_JSON);
        notSupportTypes.add(C_GEOMETORY);
        notSupportTypes.add(C_GEOMETRYCOLLECTION);
        notSupportTypes.add(C_LINESTRING);
        notSupportTypes.add(C_MULTILINESTRING);
        notSupportTypes.add(C_MULTIPOINT);
        notSupportTypes.add(C_MULTIPOLYGON);
        notSupportTypes.add(C_POINT);
        notSupportTypes.add(C_POLYGON);

        Map<String, List<String>> inserts = new HashMap<>(GsiConstant.buildGsiFullTypeTestInserts(tableName));
        for (String type : notSupportTypes) {
            inserts.remove(type);
        }

        String col = "COL_NAME";
        Set<String> allInsertSet = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : inserts.entrySet()) {
            for (String insert : entry.getValue()) {
                allInsertSet.add(insert.replace(entry.getKey(), col));
            }
        }

        List<String> allInserts = new ArrayList<>(allInsertSet);
        List<String> refAllInserts = new ArrayList<>();
        for (String insert : allInserts) {
            refAllInserts.add(insert.replace(tableName, refTableName));
        }

        System.out.println(allInserts);

        Set<String> columns = inserts.keySet();

        String alterSqlTmpl = "alter table %s modify column %s %s";

        String deleteSql = "delete from " + tableName + " where 1=1";
        String refDeleteSql = "delete from " + refTableName + " where 1=1";

        try (Connection conn = getPolardbxConnection()) {
            String sqlMode = JdbcUtil.getSqlMode(conn);
            setSqlMode("", conn);

            for (String columnDef : COLUMN_DEF_MAP.values()) {
                dropTableIfExists(conn, tableName);
                dropTableIfExists(conn, refTableName);
                JdbcUtil.executeUpdateSuccess(conn, tableDef);
                JdbcUtil.executeUpdateSuccess(conn, refTableDef);

                String targetColumnType = columnDef.substring(columnDef.lastIndexOf('`') + 1);
                String targetColumnName = columnDef.substring(columnDef.indexOf('`') + 1, columnDef.lastIndexOf('`'));
                System.out.println(targetColumnName);

                // Alter all to target column
                for (String column : columns) {
                    if (targetColumnName.equals(column)) {
                        continue;
                    }

                    System.out.println(column + " " + targetColumnName);
                    String columnMultiWriteHint =
                        String.format("COLUMN_DEBUG=\"ColumnMultiWrite:`%s`,`%s`\"", column, targetColumnName);

                    // First we clear table
                    JdbcUtil.executeUpdateSuccess(conn, deleteSql);
                    JdbcUtil.executeUpdateSuccess(conn, refDeleteSql);

                    // Then we insert values
                    allInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn,
                        buildCmdExtra(columnMultiWriteHint) + s.replace(col, column)));
                    refAllInserts.forEach(s -> JdbcUtil.executeUpdateSuccess(conn, s.replace(col, column)));

                    // Now do some alter
                    String refAlterSql = String.format(alterSqlTmpl, refTableName, column, targetColumnType);
                    JdbcUtil.executeUpdateSuccess(conn, refAlterSql);

                    // Finally, assert same
                    String selectSql = "select " + targetColumnName + " from " + tableName + " order by id";
                    String refSelectSql = "select " + column + " from " + refTableName + " order by id";
                    try {
                        selectContentSameAssertWithDiffSql(selectSql, refSelectSql, null, conn, conn, false, true,
                            true);
                    } catch (AssertionError e) {
                        System.out.println(
                            "================================================================================");
                        System.out.printf("source type: %s; target type: %s %n", column, targetColumnName);
                        System.out.println(e);
                        System.out.println(
                            "================================================================================");
                    }
                }
            }

            // Reset sql mode
            setSqlMode(sqlMode, conn);
        }
    }
}
