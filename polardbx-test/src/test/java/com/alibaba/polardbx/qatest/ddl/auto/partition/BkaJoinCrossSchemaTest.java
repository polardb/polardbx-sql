package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BkaJoinCrossSchemaTest extends PartitionTestBase {
    static final String DATABASE_1 = "test_bka_join_cross_schema_1";
    static final String DATABASE_2 = "test_bka_join_cross_schema_2";

    static final String TABLE_NAME = "```gsi-``test_table```";

    @Before
    public void prepare() {
        dropDatabase();
        createDatabase();
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format("use %s", DATABASE_1));
        dropTableIfExists(TABLE_NAME);
        createTable();
        insertData();
    }

    @After
    public void cleanup() {
        dropDatabase();
    }

    @Test
    public void testBkaJoinCrossSchemaExecSuccess() {
        String sql = "SELECT * FROM `%s`.%s ORDER by `id`;";
        JdbcUtil.executeSuccess(tddlConnection, "use " + DATABASE_2);
        checkPlanUseBkaJoin(sql);
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(sql, DATABASE_1, TABLE_NAME));
    }

    private void checkPlanUseBkaJoin(String sql) {
        String explainResult =
            getExplainResult(tddlConnection, String.format(sql, DATABASE_1, TABLE_NAME)).toLowerCase();
        Assert.assertTrue(explainResult.contains("indexscan") && explainResult.contains("bkajoin"),
            "plan should be bka join");
    }

    private void dropDatabase() {
        String dropDatabase = "drop database if exists %s";
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropDatabase, DATABASE_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(dropDatabase, DATABASE_2));
    }

    private void createDatabase() {
        String createDatabase = "create database %s";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createDatabase, DATABASE_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(createDatabase, DATABASE_2));
    }

    private void createTable() {
        String createTable = "CREATE TABLE %s (\n"
            + "  `id` bigint(20) UNSIGNED NOT NULL,\n"
            + "  `gmt_create` datetime NOT NULL,\n"
            + "  `gmt_modified` datetime NOT NULL,\n"
            + "  `x_id` varchar(64) NOT NULL,\n"
            + "  `name` varchar(64) DEFAULT NULL,\n"
            + "  `p_id` varchar(64)  DEFAULT NULL,\n"
            + "  `is_deleted` tinyint(4) NOT NULL DEFAULT '0',\n"
            + "  `y_id` bigint(20) UNSIGNED DEFAULT '0',\n"
            + "  `t_id` varchar(64) DEFAULT '0',\n"
            + "  `type` varchar(64) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `uk_x_id` (`x_id`),\n"
            + "  UNIQUE KEY `uk_name_pk` (`name`, `p_id`),\n"
            + "  KEY `auto_shard_key_p_id` USING BTREE (`p_id`),\n"
            + "  UNIQUE GLOBAL KEY `gg_i-test_buyer` (`x_id`) COVERING (`id`, `p_id`, `is_deleted`, `type`) DBPARTITION BY HASH(`x_id`),\n"
            + "  GLOBAL INDEX `gg_i-test_seller`(`t_id`) COVERING (`id`, `p_id`) DBPARTITION BY HASH(`t_id`)\n"
            + ") ENGINE = InnoDB AUTO_INCREMENT = 100000000 DEFAULT CHARSET = utf8mb4 dbpartition by hash(`p_id`) tbpartition by hash(`p_id`) tbpartitions 20;";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createTable, TABLE_NAME));
    }

    private void insertData() {
        String insertData =
            "insert into %s values (100100001, '2023-04-25 23:51:54', '2023-04-25 23:51:54 ', 'jalgjaljasvsvs', 'name', 'p1', 0,0,'rpc', NULL);";
        JdbcUtil.executeSuccess(tddlConnection, String.format(insertData, TABLE_NAME));
    }
}
