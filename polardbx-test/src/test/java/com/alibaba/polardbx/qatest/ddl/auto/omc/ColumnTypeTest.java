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

package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.constant.GsiConstant.COLUMN_DEF_MAP;
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
    private static final String SELECT_COLUMN_TYPE = "select COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, "
        + "CHARACTER_MAXIMUM_LENGTH, CHARACTER_OCTET_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, "
        + "CHARACTER_SET_NAME, COLLATION_NAME, COLUMN_TYPE "
        + "from information_schema.columns "
        + "where table_name = \"%s\" and column_name = \"%s\"";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testModifyCharset() throws SQLException {
        String tableName = "omc_modify_charset";
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        String createTableSql = String.format("create table %s (a int primary key, b varchar(20))", tableName);
        String partitionDef = " partition by hash(`a`) partitions 7";
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
        String partitionDef = " partition by hash(`a`) partitions 7";
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
                String.format("create table %s (a int primary key, b int not null) partition by hash(a) partitions 7",
                    tableName);
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
}
