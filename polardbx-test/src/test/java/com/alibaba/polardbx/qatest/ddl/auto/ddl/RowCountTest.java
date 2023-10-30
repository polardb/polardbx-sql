package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import org.junit.Test;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class RowCountTest extends DDLBaseNewDBTestCase {
    private static final String tableName = "test_row_count";

    @Test
    public void testRowCountNoPk() {
        if (!useXproto()) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (c1 int, c2 int)";
        final String rowCount = "select row_count()";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createTable, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        final String insert = "insert into " + tableName + " values (1,1), (2,2)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        final String update = "update " + tableName + " set c2 = 1 where c1 = 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        // should return -1
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);
    }

    @Test
    public void testRowCountPk() {
        if (!useXproto()) {
            return; // JDBC does not support CLIENT_FOUND_ROWS flag
        }
        if (isMySQL80()) {
            return; // RDS80 does not support CLIENT_FOUND_ROWS flag(will support later)
        }

        dropTableIfExists(tableName);
        dropTableIfExistsInMySql(tableName);

        final String createTable = "create table " + tableName + " (c1 int primary key, c2 int)";
        final String rowCount = "select row_count()";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, createTable, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        final String insert = "insert into " + tableName + " values (1,1), (2,2)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        final String update = "update " + tableName + " set c2 = 1 where c1 = 1";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, update, null, true);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);

        // should return -1
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);
        selectContentSameAssert(rowCount, null, mysqlConnection, tddlConnection);
    }
}
