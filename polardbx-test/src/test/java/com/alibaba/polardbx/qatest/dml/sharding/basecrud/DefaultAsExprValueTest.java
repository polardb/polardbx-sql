package com.alibaba.polardbx.qatest.dml.sharding.basecrud;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.text.MessageFormat;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class DefaultAsExprValueTest extends CrudBasedLockTestCase {
    private static final String TABLE_NAME = "default_as_expr_value_test_auto";
    private static final String GSI_NAME = "g_" + TABLE_NAME;
    private static final String CREAT_TABLE = "CREATE TABLE `" + TABLE_NAME + "` ("
        + " `pk`  int(11) NOT NULL AUTO_INCREMENT,"
        + " `c1`  int(11) NULL,"
        + " `c2`  int(11) NOT NULL DEFAULT (MOD(10, 3)),"
        + " `c3`  int(11) NOT NULL DEFAULT (ABS(-2)),"
        + " `c4`  int(11) NOT NULL DEFAULT (CAST('3' AS SIGNED)),"
        + " `c5`  int(11) NOT NULL DEFAULT (ROUND(3.7)),"
        + " `c6`  varchar(20) NOT NULL DEFAULT (CONCAT(\"1\", \"23\")),"
        + " `c7`  varchar(20) NOT NULL DEFAULT (DATE_FORMAT(now(),_utf8mb4''%Y-%m-%d'')),"
        + "` c7_1`  varchar(20) NOT NULL DEFAULT (DATE_FORMAT(now(),_utf8mb4\"%Y-%m-%d\")),"
        + " `C8`  FLOAT NOT NULL DEFAULT (SQRT(10)),"
        + " `C9`  JSON NOT NULL DEFAULT (JSON_ARRAY()),"
        + " `C10` TIMESTAMP NOT NULL DEFAULT (TIMESTAMPADD(MINUTE, 5,''2003-01-02'')),"
        + " `C11` DATE NOT NULL DEFAULT (CURRENT_DATE + INTERVAL 1 YEAR),"
        + " `C12` varchar(20) NOT NULL DEFAULT (LPAD(''string'', 5, ''0'')),"
        + " `C13` tinyint(1) NOT NULL DEFAULT (TRUE),"
        + " PRIMARY KEY (`pk`)"
        + ") {0} ";
    private static final String SELECT_COLUMN = "c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13";

    @Before
    public void initData() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, MessageFormat.format(CREAT_TABLE, ""));
    }

    @After
    public void clearData() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP TABLE IF EXISTS " + TABLE_NAME);
    }

    // On Partition Table
    @Test
    public void insertDefaultExprPartitionTest() {
        if (!isMySQL80()) {
            return;
        }

        final String partitionMethod = "dbpartition by hash(`c12`) tbpartition by hash(`c3`) tbpartitions 4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, partitionMethod));

        // uk
        final String addUniqueKey = "alter table " + TABLE_NAME + " add unique index u_c10(`c10`)";

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, addUniqueKey, null, true);

        final String insert = "insert into " + TABLE_NAME + "(c1) values (0)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String select = "select " + SELECT_COLUMN + " from " + TABLE_NAME;
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);

        // gsi
        final String createGsi = "CREATE UNIQUE GLOBAL INDEX `" + GSI_NAME
            + "` ON `"
            + TABLE_NAME
            + "`(`c6`, `c7`) COVERING(`c2`, `c3`) dbpartition by hash(`c6`) tbpartition by hash(`c7`) tbpartitions 3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createGsi);

        final String delete = "delete from " + TABLE_NAME;
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, delete, null, true);

        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    // On Single Table
    @Test
    public void insertDefaultExprSingeTest() {
        if (!isMySQL80()) {
            return;
        }

        final String partitionMethod = "single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, partitionMethod));

        final String insert = "insert into " + TABLE_NAME + "(c1) values (1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String select = "select " + SELECT_COLUMN + " from " + TABLE_NAME;
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

    // On Broadcast Table
    @Test
    public void insertDefaultExprBroadcastTest() {
        if (!isMySQL80()) {
            return;
        }

        final String partitionMethod = "broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat.format(CREAT_TABLE, partitionMethod));

        final String insert = "insert into " + TABLE_NAME + "(c1) values (1)";
        executeOnMysqlAndTddl(mysqlConnection, tddlConnection, insert, null, true);

        String select = "select " + SELECT_COLUMN + " from " + TABLE_NAME;
        selectContentSameAssert(select, null, mysqlConnection, tddlConnection);
    }

}
