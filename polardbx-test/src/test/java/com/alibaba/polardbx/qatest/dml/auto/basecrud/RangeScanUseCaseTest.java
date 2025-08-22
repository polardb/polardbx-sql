package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.validator.DataOperator;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RangeScanUseCaseTest extends AutoCrudBasedLockTestCase {
    @Before
    public void before() {
        final String dropTable = "drop table if exists r_l_tp100";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);

        final String createTableInPolarDBX = "CREATE TABLE `r_l_tp100` (\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    KEY `auto_shard_key_c` USING BTREE (`c`),\n"
            + "    KEY `auto_shard_key_a` USING BTREE (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(TO_DAYS(`c`))\n"
            + "SUBPARTITION BY LIST(`a`)\n"
            + "(SUBPARTITION `sp2` VALUES IN (DEFAULT))\n"
            + "(PARTITION `p1` VALUES LESS THAN (737790),\n"
            + " PARTITION `p2` VALUES LESS THAN (745461),\n"
            + " PARTITION `p3` VALUES LESS THAN (748748));";

        final String createTableInMySQL = "CREATE TABLE `r_l_tp100` (\n"
            + "    `a` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `b` bigint(20) UNSIGNED NOT NULL,\n"
            + "    `c` datetime NOT NULL,\n"
            + "    `d` varchar(16) NOT NULL,\n"
            + "    `e` varchar(16) NOT NULL,\n"
            + "    KEY `auto_shard_key_c` USING BTREE (`c`),\n"
            + "    KEY `auto_shard_key_a` USING BTREE (`a`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY RANGE(TO_DAYS(`c`))\n"
            + "SUBPARTITION BY HASH(`a`)\n"
            + "SUBPARTITIONS 2\n"
            + "(PARTITION `p1` VALUES LESS THAN (737790),\n"
            + " PARTITION `p2` VALUES LESS THAN (745461),\n"
            + " PARTITION `p3` VALUES LESS THAN (748748));";

        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            createTableInMySQL,
            createTableInPolarDBX,
            null,
            false);

        final String insert = "INSERT INTO r_l_tp100 (a, b, c, d, e) VALUES\n"
            + "(3000, 2, '2042-01-01 00:00:00', 'a', 'b'),\n"
            + "( 600, 2, '2040-01-01 00:00:00', 'a', 'b'),\n"
            + "(1500, 2, '2020-01-01 00:00:00', 'a', 'b'),\n"
            + "(3000, 2, '2040-01-01 00:00:00', 'a', 'b'),\n"
            + "( 100, 2, '2010-01-01 00:00:00', 'a', 'b'),\n"
            + "( 200, 2, '2010-01-01 00:00:00', 'a', 'b'),\n"
            + "( 400, 2, '2010-01-01 00:00:00', 'a', 'b'),\n"
            + "( 500, 2, '2011-01-01 00:00:00', 'a', 'b');\n";

        DataOperator.executeOnMysqlAndTddl(mysqlConnection,
            tddlConnection,
            insert,
            insert,
            null,
            false);
    }

    @Test
    public void test() {
        String select = "select * from r_l_tp100 order by a desc";
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);

        select = "select * from r_l_tp100 order by a";
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);

        select = "select * from r_l_tp100 order by c desc";
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);

        select = "select * from r_l_tp100 order by c";
        DataValidator.selectContentSameAssert(select, null, tddlConnection, mysqlConnection);
    }

    @After
    public void after() {
        final String dropTable = "drop table if exists r_l_tp100";
        DataOperator.executeOnMysqlAndTddl(mysqlConnection, tddlConnection, dropTable, null);
    }
}
