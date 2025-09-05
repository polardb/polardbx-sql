package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class TableNameLimitTest extends DDLBaseNewDBTestCase {

    @Test
    public void testTableName() {
        String sql = "create table %s(a int, b int)";
        String tableName = "``";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect table name");

        tableName = "` `";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect table name");

        tableName = "''";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect table name");

        tableName = "\"\"";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect table name");
    }

    @Test
    public void testColumnName() {
        String sql = "create table test_table_name_limit_1(%s int, b int)";
        String columnName = "``";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, columnName), "Incorrect column name");

        columnName = "` `";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, columnName), "Incorrect column name");

        columnName = "''";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, columnName), "Incorrect column name");

        columnName = "\"\"";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, columnName), "Incorrect column name");

        columnName = "`__#alibaba_rds_row_id#__`";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, columnName), "Not supported");
    }

    @Test
    public void testAlterTable() {
        String tableName = "test_table_name_limit_2";
        String sql = "create table %s(a int, b int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "alter table %s modify column `` int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "unknown column");

        sql = "alter table %s modify column `__#alibaba_rds_row_id#__` int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "unknown column");

        sql = "alter table %s change column a `__#alibaba_rds_row_id#__` int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Not supported");

        sql = "alter table %s change column a `` int";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect column name");
    }


    @Test
    public void testRenameTable() {
        String tableName = "test_table_name_limit_3";
        String sql = "create table %s(a int, b int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(sql, tableName));

        sql = "rename table %s to ``";
        JdbcUtil.executeUpdateFailed(tddlConnection, String.format(sql, tableName), "Incorrect table name");
    }

    public boolean usingNewPartDb() {
        return true;
    }
}
