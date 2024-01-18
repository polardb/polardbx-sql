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

package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch 2017年11月25日 上午2:00:02
 * @since 5.0.0
 */
public class InformationSchemaTest extends AutoReadBaseTestCase {

    private static final String INFORMATION_TEST_DB = "test_information_schema_db";
    private static final String INFORMATION_TEST_CASE_DB = "test_information_cASE_DB";

    private static final String CREATE_TABLE_FORMAT = "CREATE TABLE `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`integer_test` int(11) DEFAULT NULL,\n"
        + "\t`varchar_test` varchar(255) DEFAULT NULL,\n"
        + "\t`char_test` char(255) DEFAULT NULL,\n"
        + "\t`blob_test` blob,\n"
        + "\t`tinyint_test` tinyint(4) DEFAULT NULL,\n"
        + "\t`tinyint_1bit_test` tinyint(1) DEFAULT NULL,\n"
        + "\t`smallint_test` smallint(6) DEFAULT NULL,\n"
        + "\t`mediumint_test` mediumint(9) DEFAULT NULL,\n"
        + "\t`bit_test` bit(1) DEFAULT NULL,\n"
        + "\t`bigint_test` bigint(20) DEFAULT NULL,\n"
        + "\t`float_test` float DEFAULT NULL,\n"
        + "\t`double_test` double DEFAULT NULL,\n"
        + "\t`decimal_test` decimal(10, 0) DEFAULT NULL,\n"
        + "\t`date_test` date DEFAULT NULL,\n"
        + "\t`time_test` time DEFAULT NULL,\n"
        + "\t`datetime_test` datetime DEFAULT NULL,\n"
        + "\t`timestamp_test` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`year_test` year(4) DEFAULT NULL,\n"
        + "\t`mediumtext_test` mediumtext,\n"
        + "\tPRIMARY KEY (`pk`),\n"
        + "\tUNIQUE KEY (`char_test`, `integer_test`)\n"
        + ")";

    private static final String CREATE_TABLE_FORMAT_MIX_CASE = "CREATE TABLE `%s` (\n"
        + "\t`pk` bigint(11) NOT NULL,\n"
        + "\t`INTEGER_teST` int(11) DEFAULT NULL,\n"
        + "\t`varchaR_TEst` varchar(255) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`pk`)\n"
        + ")";
    private static final String[] mustContainTables = {
        "select_base_one_multi_db_multi_tb",
        "select_base_two_multi_db_multi_tb"};

    private static final String[] mustContainTableColumns = {
        // select_base_two_multi_db_multi_tb
        "select_base_two_multi_db_multi_tb.pk", "select_base_two_multi_db_multi_tb.varchar_test",
        "select_base_two_multi_db_multi_tb.integer_test",
        "select_base_two_multi_db_multi_tb.char_test",
        "select_base_two_multi_db_multi_tb.blob_test",
        "select_base_two_multi_db_multi_tb.tinyint_test",
        "select_base_two_multi_db_multi_tb.tinyint_1bit_test",
        "select_base_two_multi_db_multi_tb.smallint_test",
        "select_base_two_multi_db_multi_tb.mediumint_test",
        "select_base_two_multi_db_multi_tb.bit_test",
        "select_base_two_multi_db_multi_tb.bigint_test",
        "select_base_two_multi_db_multi_tb.float_test",
        "select_base_two_multi_db_multi_tb.double_test",
        "select_base_two_multi_db_multi_tb.decimal_test",
        "select_base_two_multi_db_multi_tb.date_test",
        "select_base_two_multi_db_multi_tb.time_test",
        "select_base_two_multi_db_multi_tb.datetime_test",
        "select_base_two_multi_db_multi_tb.timestamp_test",
        "select_base_two_multi_db_multi_tb.year_test",
        // select_base_one_multi_db_multi_tb
        "select_base_one_multi_db_multi_tb.pk", "select_base_one_multi_db_multi_tb.varchar_test",
        "select_base_one_multi_db_multi_tb.integer_test", "select_base_one_multi_db_multi_tb.char_test",
        "select_base_one_multi_db_multi_tb.blob_test", "select_base_one_multi_db_multi_tb.tinyint_test",
        "select_base_one_multi_db_multi_tb.tinyint_1bit_test", "select_base_one_multi_db_multi_tb.smallint_test",
        "select_base_one_multi_db_multi_tb.mediumint_test", "select_base_one_multi_db_multi_tb.bit_test",
        "select_base_one_multi_db_multi_tb.bigint_test", "select_base_one_multi_db_multi_tb.float_test",
        "select_base_one_multi_db_multi_tb.double_test", "select_base_one_multi_db_multi_tb.decimal_test",
        "select_base_one_multi_db_multi_tb.date_test", "select_base_one_multi_db_multi_tb.time_test",
        "select_base_one_multi_db_multi_tb.datetime_test", "select_base_one_multi_db_multi_tb.timestamp_test",
        "select_base_one_multi_db_multi_tb.year_test",
        "lowercase.INTEGER_teST", "lowercase.varchaR_TEst"};

    private static final String[] mustContainTableIndexes = {
        "select_base_one_multi_db_multi_tb.PRIMARY.pk",
        "select_base_two_multi_db_multi_tb.PRIMARY.pk"};

    private static final String[] mustContainSchema = {INFORMATION_TEST_DB, INFORMATION_TEST_CASE_DB};

    private static final Set<String> mustContainTablesSet = new HashSet<>();
    private static final Set<String> mustContainTableColumnsSet = new HashSet<>();
    private static final Set<String> mustContainTableIndexesSet = new HashSet<>();
    private static final Set<String> mustContainSchemaSet = new HashSet<>();

    static {
        for (String tableName : mustContainTables) {
            mustContainTablesSet.add(INFORMATION_TEST_DB.toLowerCase() + "." + tableName);
        }
        for (String tableColumnName : mustContainTableColumns) {
            mustContainTableColumnsSet.add(INFORMATION_TEST_DB.toLowerCase() + "." + tableColumnName);
        }
        for (String tableIndexName : mustContainTableIndexes) {
            mustContainTableIndexesSet.add(INFORMATION_TEST_DB.toLowerCase() + "." + tableIndexName);
        }
        for (String schemaName : mustContainSchema) {
            mustContainSchemaSet.add(schemaName.toLowerCase() + "." + schemaName);
        }
    }

    @Before
    public void prepareDb() {
        JdbcUtil.dropDatabase(tddlConnection, INFORMATION_TEST_DB);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + INFORMATION_TEST_DB + " mode = 'AUTO'");
        JdbcUtil.dropDatabase(tddlConnection, INFORMATION_TEST_CASE_DB);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + INFORMATION_TEST_CASE_DB + " mode = 'AUTO'");
        JdbcUtil.useDb(tddlConnection, INFORMATION_TEST_DB);
        for (String table : mustContainTables) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(CREATE_TABLE_FORMAT, table));
        }
        //加入其它table
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_TABLE_FORMAT, "select_base_three_multi_db_multi_tb"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_TABLE_FORMAT, "select_base_four_multi_db_multi_tb"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_TABLE_FORMAT, "select_base_five_multi_db_multi_tb"));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format(CREATE_TABLE_FORMAT_MIX_CASE, "LoWerCasE"));
    }

    @After
    public void clearDB() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + INFORMATION_TEST_CASE_DB);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + INFORMATION_TEST_DB);
    }

    @Test
    public void testTables() {
        String sql =
            String.format("select * from information_schema.tables where table_schema = '%s'", INFORMATION_TEST_DB);
        int size = assertContainsAllNames(sql, 1, 2, mustContainTablesSet);
        assertEqualsToShowTables(sql, 3);

        sql = String
            .format("select count(*) from information_schema.tables where table_schema = '%s'", INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = "select * from information_schema.tables limit 1";
        assertSizeEquals(sql, 1);

        sql = "select * from information_schema.tables limit 2,3";
        assertSizeEquals(sql, 3);

        sql = String.format("select * from information_schema.tables where table_schema = '%s'", INFORMATION_TEST_DB);
        size = assertContainsAllNames(sql, 1, 2, mustContainTablesSet);
        assertEqualsToShowTables(sql, 3);

        sql = String
            .format("select count(1) from information_schema.tables where table_schema in ('%s')",
                INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema,table_name,table_type from information_schema.tables where table_schema in ('%s')",
            INFORMATION_TEST_DB);
        assertContainsAllNames(sql, 0, 1, mustContainTablesSet);

        sql = String.format(
            "select table_schema,table_name,table_type from information_schema.tables where table_schema = (select database())");
        size = assertContainsAllNames(sql, 0, 1, mustContainTablesSet);
        assertEqualsToShowTables(sql, 2);

        sql = String.format("select count(*) from information_schema.tables where table_schema = (select database())");
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema as tableSchema,table_name as tableName,table_rows as rowCount,table_collation as `collate` from information_schema.tables where table_schema = (select database())");
        size = assertContainsAllNames(sql, 0, 1, mustContainTablesSet);
        assertEqualsToShowTables(sql, 2);

        sql = String.format(
            "select table_schema,table_name,table_type from information_schema.tables t where t.table_schema = '%s' order by t.table_name limit 1",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 1);

        sql = String.format(
            "select table_schema,table_name,table_type from information_schema.tables where table_schema in ('%s') order by table_name limit 2,3",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 3);

        sql = String.format(
            "select table_schema,table_name,table_type from information_schema.tables where table_schema = '%s' and table_name='%s'",
            INFORMATION_TEST_DB,
            "select_base_one_multi_db_multi_tb");
        assertSizeEquals(sql, 1);
    }

    @Test
    public void testColumns() {
        String sql =
            String.format("select * from information_schema.columns where table_schema = '%s'", INFORMATION_TEST_DB);
        int size = assertContainsAllTablesAndColumns(sql, 1, 2, 3, mustContainTableColumnsSet);

        sql = String
            .format("select count(*) from information_schema.columns where table_schema = '%s'", INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = "select * from information_schema.columns limit 1";
        assertSizeEquals(sql, 1);

        sql = "select * from information_schema.columns limit 4,5";
        assertSizeEquals(sql, 5);

        sql =
            String.format("select * from information_schema.columns where table_schema = '%s'", INFORMATION_TEST_DB);
        size = assertContainsAllTablesAndColumns(sql, 1, 2, 3, mustContainTableColumnsSet);

        sql = String
            .format("select count(1) from information_schema.columns where table_schema in ('%s')",
                INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema,table_name,column_name from information_schema.columns where table_schema in ('%s')",
            INFORMATION_TEST_DB);
        assertContainsAllTablesAndColumns(sql, 0, 1, 2, mustContainTableColumnsSet);

        sql = String.format(
            "select table_schema,table_name,column_name from information_schema.columns where table_schema = (select database())");
        size = assertContainsAllTablesAndColumns(sql, 0, 1, 2, mustContainTableColumnsSet);

        sql = String.format("select count(*) from information_schema.columns where table_schema = (select database())");
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema as tableSchema,table_name as tableName,column_name as columnName from information_schema.columns where table_schema = (select database())");
        size = assertContainsAllTablesAndColumns(sql, 0, 1, 2, mustContainTableColumnsSet);

        sql = String.format(
            "select table_schema,table_name,column_name from information_schema.columns c where c.table_schema = '%s' order by c.table_name,c.column_name limit 1",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 1);

        sql = String.format(
            "select table_schema,table_name,column_name from information_schema.columns where table_schema in ('%s') order by table_name,column_name limit 4,5",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 5);

        sql = String.format(
            "select table_schema,table_name,column_name from information_schema.columns where table_schema = '%s' and table_name='%s'",
            INFORMATION_TEST_DB,
            "select_base_one_multi_db_multi_tb");
        assertSizeEquals(sql, 20);
    }

    @Test
    public void testStatistics() {
        String sql =
            String.format("select * from information_schema.statistics where table_schema = '%s'",
                INFORMATION_TEST_DB);
        int size = assertContainsAllTablesAndIndexesAndColumns(sql, 1, 2, 5, 7, mustContainTableIndexesSet);

        sql = String
            .format("select count(*) from information_schema.statistics where table_schema = '%s'",
                INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = "select * from information_schema.statistics limit 1";
        assertSizeEquals(sql, 1);

        sql = "select * from information_schema.statistics limit 1,2";
        assertSizeEquals(sql, 2);

        sql = String.format("select * from information_schema.statistics where table_schema = '%s'",
            INFORMATION_TEST_DB);
        size = assertContainsAllTablesAndIndexesAndColumns(sql, 1, 2, 5, 7, mustContainTableIndexesSet);

        sql = String.format("select count(1) from information_schema.statistics where table_schema in ('%s')",
            INFORMATION_TEST_DB);
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema,table_name,index_name,column_name from information_schema.statistics where table_schema in ('%s')",
            INFORMATION_TEST_DB);
        assertContainsAllTablesAndIndexesAndColumns(sql, 0, 1, 2, 3, mustContainTableIndexesSet);

        sql = String.format(
            "select table_schema,table_name,index_name,column_name from information_schema.statistics where table_schema = (select database())");
        size = assertContainsAllTablesAndIndexesAndColumns(sql, 0, 1, 2, 3, mustContainTableIndexesSet);

        sql = String
            .format("select count(*) from information_schema.statistics where table_schema = (select database())");
        assertCountResult(sql, size);

        sql = String.format(
            "select table_schema as tableSchema,table_name as tableName,index_schema as indexSchema,index_name as indexName,column_name as columnName from information_schema.statistics where table_schema = (select database())");
        size = assertContainsAllTablesAndIndexesAndColumns(sql, 0, 1, 3, 4, mustContainTableIndexesSet);

        sql = String.format(
            "select table_schema,table_name,index_name,column_name from information_schema.statistics s where s.table_schema = '%s' order by s.table_name,s.column_name limit 1",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 1);

        sql = String.format(
            "select table_schema,table_name,index_name,column_name from information_schema.statistics where table_schema in ('%s') order by table_name,column_name limit 1,2",
            INFORMATION_TEST_DB);
        assertSizeEquals(sql, 2);
    }

    @Test
    public void testStatisticsWithCompoundKeys() {
        String tableName = "test_compound_keys";

        String dropSql = "drop table if exists " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        // Sharding table
        String createSql = "create table " + tableName
            + "(c1 int not null, c2 int, c3 int, primary key(c1,c2)) ";
        if (usingNewPartDb()) {
            createSql += " partition by key(c3) partitions 3";
        } else {
            createSql += " dbpartition by hash(c3)";
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // analyze to generate statistics
        String analyzeSql = "analyze table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeSql);

        String infoSchemaSql =
            "select count(*) from information_schema.statistics where table_schema = '" + INFORMATION_TEST_DB
                + "' and table_name='"
                + tableName + "'";
        assertCountResult(infoSchemaSql, 3);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        // Single table
        createSql = "create table " + tableName + "(c1 int not null, c2 int, c3 int, primary key(c1,c2))";
        if (usingNewPartDb()) {
            createSql += " single";
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // analyze to generate statistics
        analyzeSql = "analyze table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeSql);

        infoSchemaSql =
            "select count(*) from information_schema.statistics where table_schema = '" + INFORMATION_TEST_DB
                + "' and table_name='"
                + tableName + "'";
        assertCountResult(infoSchemaSql, 2);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);

        // Broadcast table
        createSql = "create table " + tableName
            + "(c1 int not null, c2 int, c3 int, c4 int, primary key(c1,c2), key(c3,c4)) broadcast";
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        // analyze to generate statistics
        analyzeSql = "analyze table " + tableName;
        JdbcUtil.executeUpdateSuccess(tddlConnection, analyzeSql);

        infoSchemaSql =
            "select count(*) from information_schema.statistics where table_schema = '" + INFORMATION_TEST_DB
                + "' and table_name='"
                + tableName + "'";
        assertCountResult(infoSchemaSql, 4);

        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql);
    }

    @Test
    public void testSchemata() {
        String sql = "select * from information_schema.schemata";
        int size = assertContainsAllNames(sql, 1, 1, mustContainSchemaSet);

        sql = "select count(*) from information_schema.schemata";
        assertCountResult(sql, size);
    }

    private String[] unsupportedYet = {"GLOBAL_STATUS", "GLOBAL_VARIABLES", "SESSION_STATUS", "SESSION_VARIABLES"};

    @Test
    public void testPartitions() {

        if (usingNewPartDb()) {
            /**
             * The performance of information_schema.partitions is too slow, it will lead to query timeout ,so ignore
             */
            return;
        }

        String[] tables = {"PARTITIONS"};

        for (String table : tables) {
            String sql;
            if (TStringUtil.equalsIgnoreCase(table, "PARTITIONS")) {
                // For better performance
                sql = String.format(
                    "select * from information_schema.%s where table_schema='%s' and table_name='update_delete_base_date_two_multi_db_one_tb' limit 10",
                    table, INFORMATION_TEST_DB);
            } else {
                sql = String.format("select * from information_schema.%s", table);
            }
            JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        }
    }

    @Test
    public void testOthers() {
        String[] tables = {
            "CHARACTER_SETS", "COLLATIONS", "COLLATION_CHARACTER_SET_APPLICABILITY",
            "COLUMN_PRIVILEGES", "ENGINES", "EVENTS", "FILES", "KEY_COLUMN_USAGE", "OPTIMIZER_TRACE", "PARAMETERS",
            "PLUGINS", "PROCESSLIST", "PROFILING", "REFERENTIAL_CONSTRAINTS", "ROUTINES",
            "SCHEMA_PRIVILEGES", "TABLESPACES", "TABLE_CONSTRAINTS", "TABLE_PRIVILEGES", "TRIGGERS",
            "USER_PRIVILEGES", "VIEWS", "MODULE", "MODULE_EVENT", "SCHEDULE_JOBS_HISTORY", "SCHEDULE_JOBS"};

        for (String table : tables) {
            String sql;
            if (TStringUtil.equalsIgnoreCase(table, "KEY_COLUMN_USAGE")
                || TStringUtil.equalsIgnoreCase(table, "PARTITIONS")
                || TStringUtil.equalsIgnoreCase(table, "TABLE_CONSTRAINTS")
                || TStringUtil.equalsIgnoreCase(table, "VIEWS")) {
                // For better performance
                sql = String
                    .format("select * from information_schema.%s where table_schema='%s'", table,
                        INFORMATION_TEST_DB);
            } else if (TStringUtil.equalsIgnoreCase(table, "REFERENTIAL_CONSTRAINTS")) {
                // For better performance
                sql = String.format("select * from information_schema.%s where constraint_schema='%s'", table,
                    INFORMATION_TEST_DB);
            } else {
                sql = String.format("select * from information_schema.%s", table);
            }
            JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        }
    }

    @Test
    public void testGlobalIndexes() {
        final String tableName = "test_information_schema_global_indexes";
        final String tableName2 = "test_information_schema_global_indexes2";
        final String dropSql = "DROP TABLE IF EXISTS ";
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);

        try {
            String sql = "";

            String createTblSql = "";
            String createGsiSql = "";
            String createTblWithGsiSql = "";
            if (!usingNewPartDb()) {

                // create a table with a GSI
                createTblSql = "CREATE TABLE " + tableName + " ( "
                    + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                    + "GLOBAL INDEX g_g1(g1) COVERING (c1) DBPARTITION BY HASH(g1) "
                    + ") DBPARTITION by hash(id)";

                // create another GSI for this table
                createGsiSql =
                    "CREATE GLOBAL INDEX g_g2 ON " + tableName + " (g2) COVERING (c1, c2) DBPARTITION by HASH(g2)";

                createTblWithGsiSql = "CREATE TABLE " + tableName2 + " ( "
                    + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                    + "CLUSTERED INDEX cluster_g1(g1, g2) DBPARTITION BY HASH(g1) "
                    + ") DBPARTITION by hash(id)";

            } else {
                // create a table with a GSI
                createTblSql = "CREATE TABLE " + tableName + " ( "
                    + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                    + "GLOBAL INDEX g_g1(g1) COVERING (c1) PARTITION BY KEY(g1) "
                    + "PARTITIONS 3"
                    + ") PARTITION by key(id) PARTITIONS 3";

                // create another GSI for this table
                createGsiSql = "CREATE GLOBAL INDEX g_g2 ON " + tableName
                    + " (g2) COVERING (c1, c2) PARTITION by KEY(g2) PARTITIONS 3";

                createTblWithGsiSql = "CREATE TABLE " + tableName2 + " ( "
                    + "id int, g1 int, g2 int, c1 int, c2 int, PRIMARY KEY (id), "
                    + "CLUSTERED INDEX cluster_g1(g1, g2) PARTITION BY KEY(g1) PARTITIONS 3"
                    + ") PARTITION by KEY(id) PARTITIONS 3";
            }

            JdbcUtil.executeUpdateSuccess(tddlConnection, createTblSql);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createGsiSql);
            // create another table with a cluster index
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, createTblWithGsiSql);

            // insert some data to make GSI size > 0
            for (int i = 0; i < 100; i++) {
                sql = String.format("INSERT INTO %s(id, g1, g2, c1, c2) VALUES (%d, %d, %d, %d, %d)",
                    tableName, i, i, i, i, i);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
                sql = String.format("INSERT INTO %s(id, g1, g2, c1, c2) VALUES (%d, %d, %d, %d, %d)",
                    tableName2, i, i, i, i, i);
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
            }

            checkGlobalIndexes(tableName, "g_g1", ImmutableList.of("g1"), ImmutableList.of("id", "c1"));
            checkGlobalIndexes(tableName, "g_g2", ImmutableList.of("g2"), ImmutableList.of("id", "c2"));
            checkGlobalIndexes(tableName2, "cluster_g1", ImmutableList.of("g1", "g2"),
                ImmutableList.of("id", "c1", "c2"));
        } finally {
            // drop tables
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, dropSql + tableName2);
        }
    }

    /**
     * test information schema tables by in expr in different params size
     */
    @Test
    public void testIn() {
        String sql =
            "select TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                + "where `TABLE_SCHEMA` = 'drds_mode' and CONSTRAINT_TYPE in ('PRIMARY KEY','UNIQUE KEY')";
        String sql1 =
            "select TABLE_SCHEMA,TABLE_NAME,CONSTRAINT_NAME,CONSTRAINT_TYPE from INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
                + "where `TABLE_SCHEMA` = 'drds_mode' and CONSTRAINT_TYPE in ('PRIMARY KEY')";
        JdbcUtil.executeSuccess(tddlConnection, sql);
        JdbcUtil.executeSuccess(tddlConnection, sql1);
    }

    private void checkGlobalIndexes(String tableName, String gsiName, List<String> indexColumns,
                                    List<String> coveringColumns) {
        // search information_schema.GLOBAL_INDEXES
        String sql = String.format("SELECT * FROM information_schema.GLOBAL_INDEXES "
                + "where SCHEMA = '%s' and TABLE = '%s' and KEY_NAME like '%s%%'", INFORMATION_TEST_DB, tableName,
            gsiName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        List<List<Object>> results = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(results.size() == 1);

        // 15 columns in GLOBAL_INDEXES, if the number of column in GLOBAL_INDEXES is changed, please modify this value
        final int columnCnt = 19;
        Assert.assertTrue(results.get(0).size() == columnCnt);

        List<String> result = results.get(0).stream()
            .map(obj -> obj == null ? "" : obj.toString())
            .collect(Collectors.toList());
        // index 0 is schema name
        Assert.assertTrue(INFORMATION_TEST_DB.equalsIgnoreCase(result.get(0)));
        // these two gsi should have the same table name (index 1)
        Assert.assertTrue(tableName.equalsIgnoreCase(result.get(1)));
        // index 3 is GSI name
        Assert.assertTrue(result.get(3) != null && result.get(3).toLowerCase().contains(gsiName.toLowerCase()));
        // index 4 is indexing columns
        for (String indexColumn : indexColumns) {
            Assert.assertTrue(StringUtils.containsIgnoreCase(result.get(4), indexColumn));
        }
        // index 5 is covering columns
        for (String coveringColumn : coveringColumns) {
            Assert.assertTrue(StringUtils.containsIgnoreCase(result.get(5), coveringColumn));
        }
        // index 14 is GSI size, since we use the result of a single shard to estimate the whole size,
        // it may be zero.
        Assert.assertTrue(Double.parseDouble(result.get(14)) >= 0);
    }

    private void assertSizeEquals(String sql, int expectedSize) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(JdbcUtil.getAllResult(rs).size() == expectedSize);
    }

    private void assertCountResult(String sql, int expectedSize) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        List<List<Object>> result = JdbcUtil.getAllResult(rs);
        Assert.assertTrue(result.size() == 1 && result.get(0).size() == 1);
        Assert.assertTrue(Integer.valueOf(result.get(0).get(0).toString()) == expectedSize);
    }

    private Map<String, List<Object>> buildTablenameToRecordMap(List<List<Object>> records, int tableSchemaIndex,
                                                                int tableNameIndex) {
        Map<String, List<Object>> map = Maps.newHashMap();
        for (List<Object> record : records) {
            map.put(((String) record.get(tableSchemaIndex)).toLowerCase() + "." + record.get(tableNameIndex), record);
        }

        return map;
    }

    private int assertContainsAllNames(String sql, int schemaIndex, int nameIndex, Set<String> mustContainNames) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "/*+TDDL:cmd_extra()*/" + sql);
        Map<String, List<Object>> map = buildTablenameToRecordMap(JdbcUtil.getAllResult(rs), schemaIndex, nameIndex);
        for (String name : mustContainNames) {
            org.junit.Assert.assertTrue(map.containsKey(name) || map.containsKey(name.toUpperCase())
                || map.containsKey(name.toLowerCase()));
        }
        return map.size();
    }

    private Map<String, List<Object>> buildTableColumnNameToRecordMap(List<List<Object>> records, int tableSchemaIndex,
                                                                      int tableNameIndex, int columnNameIndex) {
        Map<String, List<Object>> map = Maps.newHashMap();
        for (List<Object> record : records) {
            map.put(((String) record.get(tableSchemaIndex)).toLowerCase() + "." + ((String) record.get(
                tableNameIndex)).toLowerCase() + "."
                + record.get(columnNameIndex), record);
        }

        return map;
    }

    private Map<String, List<Object>> buildTableIndexColumnNameToRecordMap(List<List<Object>> records,
                                                                           int tableSchemaIndex,
                                                                           int tableNameIndex, int indexNameIndex,
                                                                           int columnNameIndex) {
        Map<String, List<Object>> map = Maps.newHashMap();
        for (List<Object> record : records) {
            map.put(((String) record.get(tableSchemaIndex)).toLowerCase() + "." + record.get(tableNameIndex) + "."
                + record.get(indexNameIndex) + "." + record.get(columnNameIndex), record);
        }

        return map;
    }

    private int assertContainsAllTablesAndColumns(String sql, int tableSchemaIndex, int tableNameIndex,
                                                  int columnNameIndex, Set<String> mustContainTableInfo) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Map<String, List<Object>> map = buildTableColumnNameToRecordMap(JdbcUtil.getAllResult(rs),
            tableSchemaIndex,
            tableNameIndex,
            columnNameIndex);
        for (String tableColumnName : mustContainTableInfo) {
            org.junit.Assert.assertTrue(map.containsKey(tableColumnName));
        }
        return map.size();
    }

    private int assertContainsAllTablesAndIndexesAndColumns(String sql, int tableSchemaIndex, int tableNameIndex,
                                                            int indexNameIndex, int columnNameIndex,
                                                            Set<String> mustContainTableInfo) {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        List<List<Object>> result = JdbcUtil.getAllResult(rs);
        Map<String, List<Object>> map = buildTableIndexColumnNameToRecordMap(result,
            tableSchemaIndex,
            tableNameIndex,
            indexNameIndex,
            columnNameIndex);
        for (String tableColumnName : mustContainTableInfo) {
            org.junit.Assert.assertTrue(map.containsKey(tableColumnName));
        }
        return result.size();
    }

    private void assertEqualsToShowTables(String sql, int tableNameIndex) {
        Set<String> showTableNames = new HashSet<>();
        Set<String> infoTableNames = new HashSet<>();

        try (ResultSet rsShow = JdbcUtil.executeQuerySuccess(tddlConnection, "show tables");
            ResultSet rsInfo = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (rsShow.next()) {
                showTableNames.add(rsShow.getString(1).toLowerCase());
            }
            while (rsInfo.next()) {
                infoTableNames.add(rsInfo.getString(tableNameIndex).toLowerCase());
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }

        List<String> nofoundTableNames = new ArrayList<>();

        if (showTableNames.size() >= infoTableNames.size()) {
            // Compare details
            for (String infoTableName : infoTableNames) {
                if (!showTableNames.contains(infoTableName)) {
                    nofoundTableNames.add(infoTableName);
                }
            }
            if (nofoundTableNames.size() > 0) {
                Assert.fail("The INFORMATION_SCHEMA.TABLES result contains tables '\n" + nofoundTableNames
                    + "\n' that doesn't exist in the result of SHOW TABLES");
            }
        } else {
            Assert.fail("Found different row count: show - " + showTableNames.size() + ", info - "
                + infoTableNames.size());
        }
    }

}
