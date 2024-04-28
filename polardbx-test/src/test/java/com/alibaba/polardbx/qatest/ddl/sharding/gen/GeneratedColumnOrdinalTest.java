package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_DDL_RECORD_TABLE;
import static com.alibaba.polardbx.druid.sql.SQLUtils.normalize;
import static com.alibaba.polardbx.druid.sql.parser.SQLParserUtils.createSQLStatementParser;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
public class GeneratedColumnOrdinalTest extends DDLBaseNewDBTestCase {
    private static final String[] ALTER_PARAMS = new String[] {
        "alter table %s add column g1 int as (a+b) logical after c",
        "alter table %s add column g2 int as (a-b) logical first",
        "alter table %s add column g3 int as (a*b)logical ",
        "alter table %s add column g4 int as (a*b+10) logical after g2, add column g5 int as (a*b+20) logical after g2",
        "alter table %s add column g6 int as (a*b+30) logical first, add column g7 int as (a*b+40) logical first",
    };

    private static final String[] ALTER_PARAMS_GSI = new String[] {
        "alter table %s add column g1 int as (a+b) logical after c",
        "alter table %s add column g2 int as (a-b) logical first",
        "alter table %s add column g3 int as (a*b)",
    };

    private static final String ALTER_COLUMNS = "a,b,c,d,e";

    private static final String[] ALTER_GSI_COLUMNS = {"a", "b", "c", "d", "e", "g1", "g2", "g3"};

    private final Boolean useInstantAddColumn;

    @Parameterized.Parameters(name = "{index}:useInstantAddColumn={0}")
    public static List<Object[]> prepareDate() {
        return ImmutableList.of(new Object[] {Boolean.FALSE}, new Object[] {Boolean.TRUE});
    }

    public GeneratedColumnOrdinalTest(Boolean useInstantAddColumn) {
        this.useInstantAddColumn = useInstantAddColumn;
    }

    @Test
    public void testChangeColumnOrdinal() throws SQLException {
        setGlobalSupportInstantAddColumn(useInstantAddColumn);
        String tableName = "gen_col_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, ALTER_PARAMS_GSI, ALTER_COLUMNS, ALTER_GSI_COLUMNS, true);
        testColumnOrdinalInternal(tableName, ALTER_PARAMS, ALTER_COLUMNS, ALTER_GSI_COLUMNS, false);
    }

    private void testColumnOrdinalInternal(String tableName, String[] params, String columns, String[] gsiColumns,
                                           boolean withGsi)
        throws SQLException {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("cdc_gen");

        String createTableSql =
            String.format("create table %s (a int primary key, b int, c int, d int, e int)", tableName);
        String partitionDef = " dbpartition by hash(`a`)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);
        repository.console(createTableSql);

        String gsiName1 = tableName + "_gsi_1";

        if (withGsi) {
            String createGsiSql1 =
                String.format("create global clustered index %s on %s(a) dbpartition by hash(a)", gsiName1,
                    tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
        }

        String insert = String.format("insert into %s values (1,2,3,4,5),(6,7,8,9,10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);

        for (int i = 0; i < params.length; i++) {
            String tokenHints = "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID() + "\")*/";
            String alterSql = tokenHints + String.format(params[i], tableName);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql.replace("logical", ""));
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

            //cdc check
            repository.console(alterSql);
            checkCdcDdlMark(tokenHints, alterSql, tableName, repository);
        }

        insert = String.format("insert into %s(%s) values (13,4,5,6,7)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (15,6,7,8,9),(19,10,11,12,13)", tableName, columns);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        if (withGsi) {
            checkGsi(tddlConnection, gsiName1);

            // check column order for gsi
            String showSql =
                String.format("show full columns from %s", gsiName1);
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

    private void checkCdcDdlMark(String tokenHints, String sql, String tableName, SchemaRepository repository)
        throws SQLException {
        //origin
        SchemaObject schemaObject = repository.findTable(tableName);
        SQLCreateTableStatement stmt1 = (SQLCreateTableStatement) schemaObject.getStatement();
        Set<String> originColumns =
            stmt1.getColumnDefinitions().stream()
                .map(c -> normalize(c.getColumnName()) + "." + c.getDataType().getName()
                    .toLowerCase()).collect(Collectors.toSet());

        //target
        DDLExtInfo extInfo = getDdlExtInfo(tokenHints);
        Assert.assertTrue(StringUtils.isNotBlank(extInfo.getCreateSql4PhyTable()));
        SQLStatementParser parser = createSQLStatementParser(extInfo.getCreateSql4PhyTable(), DbType.mysql);
        SQLCreateTableStatement stmt2 = (SQLCreateTableStatement) parser.parseStatementList().get(0);
        Set<String> targetColumns = stmt2.getColumnDefinitions().stream()
            .map(c -> normalize(c.getColumnName()) + "." + c.getDataType().getName()
                .toLowerCase()).collect(Collectors.toSet());

        //compare
        Assert.assertEquals(originColumns, targetColumns);
    }

    private DDLExtInfo getDdlExtInfo(String tokenHints) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select ext from __cdc__." + CDC_DDL_RECORD_TABLE + " where ddl_sql like '%" + tokenHints
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
}
