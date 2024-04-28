package com.alibaba.polardbx.qatest.ddl.auto.partitionkey;

import com.alibaba.polardbx.druid.sql.repository.SchemaRepository;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_DDL_RECORD_TABLE;
import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

@NotThreadSafe
public class ModifyPartitionKeyOrderTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType);
    }

    private static final String[] MODIFY_PARAMS = new String[] {
        "alter table %s modify column b bigint",
        "alter table %s modify column b int first",
        "alter table %s modify column c bigint after e",
    };

    private static final String[] CHANGE_PARAMS = new String[] {
        "alter table %s change column b b bigint",
        "alter table %s change column b b int after c",
        "alter table %s change column c c bigint after d",
        "alter table %s change column c c int after e",
        "alter table %s change column b b bigint first",
    };

    private static final String MODIFY_COLUMNS = "a,b,c,d,e";

    private static final String[] MODIFY_GSI_COLUMNS = {"b", "a", "d", "e", "c"};

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testModifyColumnOrdinal() throws SQLException {
        String tableName = "omc_modify_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, MODIFY_PARAMS, true);
        testColumnOrdinalInternal(tableName, MODIFY_PARAMS, false);
    }

    @Test
    public void testChangeColumnOrdinal() throws SQLException {
        String tableName = "omc_change_column_ordinal_test_tbl";
        testColumnOrdinalInternal(tableName, CHANGE_PARAMS, true);
        testColumnOrdinalInternal(tableName, CHANGE_PARAMS, false);
    }

    private void testColumnOrdinalInternal(String tableName, String[] params, boolean withGsi)
        throws SQLException {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        SchemaRepository repository = new SchemaRepository(JdbcConstants.MYSQL);
        repository.setDefaultSchema("cdc_omc");

        String createTableSql =
            String.format("create table %s (a int primary key, b int, c int, d int, e int)", tableName);
        String partitionDef = " partition by hash(`b`) partitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createTableSql + partitionDef);
        JdbcUtil.executeUpdateSuccess(mysqlConnection, createTableSql);
        repository.console(createTableSql);

        String gsiName1 = tableName + "_gsi_1";
        String gsiName2 = tableName + "_gsi_2";

        if (withGsi) {
            String createGsiSql1 =
                String.format("create global clustered index %s on %s(c) partition by hash(c) partitions 3", gsiName1,
                    tableName);
            String createGsiSql2 =
                String.format("create global index %s on %s(a) covering(c,d) partition by hash(a) partitions 3",
                    gsiName2, tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql2);
        }

        String insert = String.format("insert into %s values (1,2,3,4,5),(6,7,8,9,10)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);

        for (int i = 0; i < params.length; i++) {
            String tokenHints = "/*+TDDL:CMD_EXTRA(CDC_RANDOM_DDL_TOKEN=\"" + UUID.randomUUID().toString() + "\")*/";
            String alterSql = tokenHints + String.format(params[i], tableName);
            execDdlWithRetry(tddlDatabase1, tableName, alterSql, tddlConnection);
            JdbcUtil.executeUpdateSuccess(mysqlConnection, alterSql);
            selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

            //cdc check
            repository.console(alterSql);
            checkCdcDdlMark(tokenHints, alterSql);
        }

        insert = String.format("insert into %s values (2,3,4,5,6)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (13,4,5,6,7)", tableName,
            ModifyPartitionKeyOrderTest.MODIFY_COLUMNS);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s values (4,5,6,7,8),(8,29,10,11,12)", tableName);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        insert = String.format("insert into %s(%s) values (15,6,7,8,9),(19,10,11,12,13)", tableName,
            ModifyPartitionKeyOrderTest.MODIFY_COLUMNS);
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, insert, null, false);
        selectContentSameAssert("select * from " + tableName, null, mysqlConnection, tddlConnection);

        if (withGsi) {
            checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName1));
            checkGsi(tddlConnection, getRealGsiName(tddlConnection, tableName, gsiName2));

            // check column order for gsi
            String showSql =
                String.format("show full columns from %s", getRealGsiName(tddlConnection, tableName, gsiName1));
            ResultSet rs = JdbcUtil.executeQuery(showSql, tddlConnection);
            List<List<Object>> result = JdbcUtil.getAllResult(rs);

            for (int i = 0; i < ModifyPartitionKeyOrderTest.MODIFY_GSI_COLUMNS.length; i++) {
                Assert.assertTrue(result.get(i).get(0).toString().equalsIgnoreCase(
                    ModifyPartitionKeyOrderTest.MODIFY_GSI_COLUMNS[i]));
            }

        }
    }

    private void checkCdcDdlMark(String tokenHints, String sql) throws SQLException {
        int visibility = getDdlExtInfo(tokenHints);
        Assert.assertEquals("ddl visibility is not valid for sql " + sql, 1, visibility);
    }

    private int getDdlExtInfo(String tokenHints) throws SQLException {
        int ret = 0;
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(
                "select visibility from __cdc__." + CDC_DDL_RECORD_TABLE + " where ddl_sql like '%" + tokenHints
                    + "%' order by id desc limit 1")) {
                while (resultSet.next()) {
                    ret = resultSet.getInt(1);
                }
            }
        }
        return ret;
    }
}
