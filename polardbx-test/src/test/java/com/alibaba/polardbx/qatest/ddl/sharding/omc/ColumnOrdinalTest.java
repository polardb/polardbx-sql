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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_DDL_RECORD_TABLE;
import static com.alibaba.polardbx.druid.sql.SQLUtils.normalize;
import static com.alibaba.polardbx.druid.sql.parser.SQLParserUtils.createSQLStatementParser;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
public class ColumnOrdinalTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String[] MODIFY_PARAMS = new String[] {
        "alter table %s modify column b bigint",
        "alter table %s modify column c bigint first",
        "alter table %s modify column d bigint after e",
    };

    private static final String[] CHANGE_PARAMS = new String[] {
        "alter table %s change column b bb bigint",
        "alter table %s change column c cc bigint first",
        "alter table %s change column d dd bigint after e",
        "alter table %s change column e `3` bigint after dd",
        "alter table %s change column `3` `\"f\"` int first",
        "alter table %s change column `dd` `UNIQUE` int after `\"f\"`",
    };

    private static final String MODIFY_COLUMNS = "a,b,c,d,e";

    private static final String CHANGE_COLUMNS = "a,bb,cc,`UNIQUE`,`\"f\"`";

    private static final String[] MODIFY_GSI_COLUMNS = {"c", "a", "b", "e", "d"};

    private static final String[] CHANGE_GSI_COLUMNS = {"\"f\"", "UNIQUE", "cc", "a", "bb"};

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC ";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    private final Boolean useInstantAddColumn;

    @Parameterized.Parameters(name = "{index}:useInstantAddColumn={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {Boolean.FALSE}, new Object[] {Boolean.TRUE});
    }

    public ColumnOrdinalTest(Boolean useInstantAddColumn) {
        this.useInstantAddColumn = useInstantAddColumn;
    }

    @Before
    public void init() {
        setGlobalSupportInstantAddColumn(useInstantAddColumn);
    }

    @Test
    public void testModifyColumnOrdinal() throws SQLException {
        String tableName = "omc_modify_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, MODIFY_PARAMS, MODIFY_COLUMNS, MODIFY_GSI_COLUMNS, true);
        testColumnOrdinalInternal(tableName, MODIFY_PARAMS, MODIFY_COLUMNS, MODIFY_GSI_COLUMNS, false);
    }

    @Test
    public void testChangeColumnOrdinal() throws SQLException {
        String tableName = "omc_change_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, CHANGE_PARAMS, CHANGE_COLUMNS, CHANGE_GSI_COLUMNS, true);
        testColumnOrdinalInternal(tableName, CHANGE_PARAMS, CHANGE_COLUMNS, CHANGE_GSI_COLUMNS, false);
    }

    private void testColumnOrdinalInternal(String tableName, String[] params, String columns, String[] gsiColumns,
                                           boolean withGsi)
        throws SQLException {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("cdc_omc");

        String createTableSql =
            String.format("create table %s (a int primary key, b int, c int, d int, e int)", tableName);
        String partitionDef = " dbpartition by hash(a) tbpartition by hash(a) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);
        repository.console(createTableSql);

        String gsiName1 = tableName + "_gsi_1";
        String gsiName2 = tableName + "_gsi_2";

        if (withGsi) {
            String createGsiSql1 =
                String.format("create global clustered index %s on %s(a) dbpartition by hash(a)", gsiName1,
                    tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(a) covering(c,d) dbpartition by hash(a)",
                    gsiName2, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
        }

        String insert = String.format("insert into %s values (1,2,3,4,5),(6,7,8,9,10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);

        for (String param : params) {
            String alterSql = String.format(param, tableName);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql + USE_OMC_ALGORITHM, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

            //cdc check
            repository.console(alterSql + USE_OMC_ALGORITHM);
            checkCdcDdlMark(alterSql + USE_OMC_ALGORITHM, tableName, repository);
        }

        insert = String.format("insert into %s values (2,3,4,5,6)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (13,4,5,6,7)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s values (4,5,6,7,8),(8,29,10,11,12)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (15,6,7,8,9),(19,10,11,12,13)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        if (withGsi) {
            checkGsi(tddlConnection, gsiName1);
            checkGsi(tddlConnection, gsiName2);

            // check column order for gsi
            String showSql = String.format("show full columns from %s", gsiName1);
            ResultSet rs = JdbcUtil.executeQuery(showSql, tddlConnection);
            List<List<Object>> result = JdbcUtil.getAllResult(rs);

            for (int i = 0; i < gsiColumns.length; i++) {
                Assert.assertTrue(result.get(i).get(0).toString().equalsIgnoreCase(gsiColumns[i]));
            }
        }
    }

    private void enableSetGlobalSession() {
        String sql = "set enable_set_global=true";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    private void setGlobalSupportInstantAddColumn(boolean supported) {
        enableSetGlobalSession();
        String sql = "set global support_instant_add_column=%s";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, supported ? "on" : "off"));
    }

    private void checkCdcDdlMark(String sql, String tableName, SchemaRepository repository) throws SQLException {
        //origin
        SchemaObject schemaObject = repository.findTable(tableName);
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        Set<String> originColumns =
            stmt1.getColumnDefinitions().stream()
                .map(c -> normalize(c.getColumnName()) + "." + c.getDataType().getName()
                    .toLowerCase()).collect(Collectors.toSet());

        //target
        DDLExtInfo extInfo = getDdlExtInfo(sql);
        Assert.assertTrue(StringUtils.isNotBlank(extInfo.getCreateSql4PhyTable()));
        SQLStatementParser parser = createSQLStatementParser(extInfo.getCreateSql4PhyTable(), DbType.mysql);
        SQLCreateTableStatement stmt2 = (SQLCreateTableStatement) parser.parseStatementList().get(0);
        Set<String> targetColumns = stmt2.getColumnDefinitions().stream()
            .map(c -> normalize(c.getColumnName()) + "." + c.getDataType().getName()
                .toLowerCase()).collect(Collectors.toSet());

        //compare
        Assert.assertEquals(originColumns, targetColumns);
    }

    private DDLExtInfo getDdlExtInfo(String sql) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ext from __cdc__." + CDC_DDL_RECORD_TABLE + " where ddl_sql like '%" + sql
                    + "%' order by id desc limit 1")) {
                while (resultSet.next()) {
                    String extStr = resultSet.getString(1);
                    if (StringUtils.isNotBlank(extStr)) {
                        return JSONObject.parseObject(extStr, DDLExtInfo.class);
                    }
                }
            }
        }
        return new DDLExtInfo();
    }

    @Ignore
    @Test
    public void generatedColumnTest() {
        String tableName = "dn_gen_col_alter_tbl";
        dropTableIfExists(tableName);
        String createTable =
            String.format("create table %s (a int primary key, b int, c int as (a), d int, e int)", tableName);
        String partDef = " dbpartition by hash(a)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTable + partDef);

        String alter = String.format("alter table %s modify column b bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column b bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column d bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s modify column d bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column b f bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column b f bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column d f bigint first, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");

        alter = String.format("alter table %s change column d f bigint after e, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, alter, "");
    }
}
