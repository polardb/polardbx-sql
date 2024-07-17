package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.PartitionTableSqls;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.PartitionType;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 针对分区表的DDL打标测试
 * <p>
 * created by ziyang.lb
 **/

public class CdcRePartitionMain_TableGroupTest extends CdcRePartitionBaseTest {

    public CdcRePartitionMain_TableGroupTest() {
        this.dbName = "cdc_alter_tg_partition";
    }

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            sql = "drop database if exists " + dbName;
            stmt.execute(sql);
            sql = "create database " + dbName + " mode = 'auto' ";
            stmt.execute(sql);
            sql = "use " + dbName;
            stmt.execute(sql);

            testHotKeyExtract4Hash(dbName, stmt);
            testHotKeyExtract4HashKey(dbName, stmt);
            testHotKeySplitByGroup(dbName, stmt);
            testHotkeySplitByTableGroupAndTable(dbName, stmt);
            testReorgTableGroup(stmt);

            //test alter partition table group 表组相关的DDL测试
            boolean createWithGsi = true;
            doDDl(stmt, "t_range_1", "t_range_2", PartitionType.Range, createWithGsi);
            doDDl(stmt, "t_range_column_1", "t_range_column_2", PartitionType.RangeColumn, createWithGsi);

            doDDl(stmt, "t_hash_1", "t_hash_2", PartitionType.Hash, createWithGsi);
            doDDl(stmt, "t_hash_key_1", "t_hash_key_2", PartitionType.HashKey, createWithGsi);

            doDDl(stmt, "t_list_1", "t_list_2", PartitionType.List, createWithGsi);
            doDDl(stmt, "t_list_column_1", "t_list_column_2", PartitionType.ListColumn, createWithGsi);

            doDDl(stmt, "t_broadcast_1", "t_broadcast_2", PartitionType.Broadcast, createWithGsi);
            doDDl(stmt, "t_single_1", "t_single_2", PartitionType.Single, createWithGsi);

            //oss archive table
            //testMoveTablePartitionWithOss(stmt);
        }
    }

    private void doDDl(Statement stmt, String tableName1, String tableName2,
                       PartitionType partitionType, boolean createWithGsi)
        throws SQLException {
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        // Test Step 测试drop table if exists
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table if exists %s ", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName1, sql);

        // Test Step 测试drop table if exists
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table if exists %s ", tableName2);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName2, sql);

        // Test Step 测试建表DDL
        for (int i = 0; i < 2; i++) {
            String tableName = i == 0 ? tableName1 : tableName2;
            if (partitionType == PartitionType.Range) {
                sql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.RangeColumn) {
                sql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.Hash) {
                sql = String.format(PartitionTableSqls.CREATE_HASH_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.HashKey) {
                sql = String.format(PartitionTableSqls.CREATE_HASH_KEY_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.List) {
                sql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.ListColumn) {
                sql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.Broadcast) {
                sql = String.format(PartitionTableSqls.CREATE_BROADCAST_TABLE_SQL, tableName);
            } else if (partitionType == PartitionType.Single) {
                sql = String.format(PartitionTableSqls.CREATE_SINGLE_TABLE_SQL, tableName);
            } else {
                throw new RuntimeException("not supported ");
            }
            tokenHints = buildTokenHints();
            sql = tokenHints + sql;
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);
        }

        // 测试常规DDL，如加减列、重命名等
        tableName1 = testCommonDdl(checkContext, dbName, stmt, tableName1, partitionType, createWithGsi);

        // 测试表组相关操作
        if (partitionType.isPartitionTable()) {
            testTableGroupDdl(checkContext, stmt, dbName, tableName1, tableName2, partitionType, createWithGsi);
        }

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table %s", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName1, sql);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop table %s", tableName2);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName2, sql);
    }

    private void testHotKeySplitByGroup(String schemaName, Statement stmt) throws SQLException {
        // prepare tables and data
        List<String> tables = new ArrayList<>();
        tables.add("t_orders_hotkey_test_by_group_1");
        tables.add("t_orders_hotkey_test_by_group_2");
        for (String table : tables) {
            String sql1 = "CREATE TABLE `" + table + "` (\n"
                + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "        `seller_id` int(11) DEFAULT NULL,\n"
                + "        PRIMARY KEY (`id`),\n"
                + "        KEY `auto_shard_key_seller_id_id` USING BTREE (`seller_id`, `id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"
                + " PARTITION BY KEY(`seller_id`,`id`)  PARTITIONS 8";
            String sql2 = "insert into " + table
                + " (seller_id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(88),(88),(88),(88);";
            String sql3 =
                "insert into " + table + " (seller_id) select seller_id from " + table + " where seller_id=88;";
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
        }

        // execute
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(schemaName);

        String tableGroup = queryTableGroup(schemaName, tables.get(0));
        String tokenHints = buildTokenHints();
        String sql = tokenHints + " alter tablegroup " + tableGroup + " split into partitions 10 by hot value(88)";
        stmt.execute(sql);

        //check
        checkAfterAlterTableGroup(checkContext, tableGroup, sql, tables);
    }

    private void testHotkeySplitByTableGroupAndTable(String schemaName, Statement stmt) throws SQLException {
        // prepare tables and data
        List<String> tables = new ArrayList<>();
        tables.add("t_hotkey_test_by_group_and_table_1");
        tables.add("t_hotkey_test_by_group_and_table_2");
        for (String table : tables) {
            String sql1 = "CREATE TABLE `" + table + "` (\n"
                + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "        `seller_id` mediumint(11) DEFAULT NULL,\n"
                + "        PRIMARY KEY (`id`),\n"
                + "        KEY `auto_shard_key_seller_id_id` USING BTREE (`seller_id`, `id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"
                + " PARTITION BY KEY(`seller_id`,`id`)  PARTITIONS 8";
            String sql2 = "insert into " + table
                + " (seller_id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(88),(88),(88),(88);";
            String sql3 =
                "insert into " + table + " (seller_id) select seller_id from " + table + " where seller_id=88;";
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
        }

        // execute
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(schemaName);

        // alter table group by table
        String tableGroup = queryTableGroup(schemaName, tables.get(0));
        String tokenHints = buildTokenHints();
        String sql = tokenHints + " alter tablegroup by table " + tables.get(0) +
            " split into partitions 1 by hot value(88)";
        stmt.execute(sql);
        checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tables.get(0), tables.get(1)));
    }

    private void testHotKeyExtract4HashKey(String schemaName, Statement stmt) throws SQLException {
        // prepare table and data
        List<String> tables = new ArrayList<>();
        tables.add("t_orders_hotkey_extract_1");
        tables.add("t_orders_hotkey_extract_2");
        for (String table : tables) {
            String sql1 = "CREATE TABLE `" + table + "` (\n"
                + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "        `seller_id` int(11) DEFAULT NULL,\n"
                + "        PRIMARY KEY (`id`),\n"
                + "        KEY `auto_shard_key_seller_id_id` USING BTREE (`seller_id`, `id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"
                + " PARTITION BY KEY(`seller_id`)  PARTITIONS 8";
            String sql2 = "insert into " + table
                + " (seller_id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(88),(88),(88),(88);";
            String sql3 =
                "insert into " + table + " (seller_id) select seller_id from " + table + " where seller_id=88;";
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
        }

        // execute
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(schemaName);

        String tableGroup = queryTableGroup(schemaName, tables.get(0));
        String tokenHints = buildTokenHints();
        String sql = tokenHints + " alter tablegroup " + tableGroup + " extract to partition by hot value(88[]);";
        stmt.execute(sql);

        // check
        String rewriteSql = String.format("ALTER TABLEGROUP %s SPLIT INTO  PARTITIONS 1 BY HOT VALUE(88)", tableGroup);
        checkAfterAlterTableGroup(checkContext, tableGroup, rewriteSql, tables);
    }

    private void testHotKeyExtract4Hash(String schemaName, Statement stmt) throws SQLException {
        // prepare
        List<String> tables = new ArrayList<>();
        String tableName1 = "t_orders_hotkey_extract_3_" + RandomUtils.nextLong();
        String tableName2 = "t_orders_hotkey_extract_4_" + RandomUtils.nextLong();
        tables.add(tableName1);
        tables.add(tableName2);
        for (String table : tables) {
            String sql1 = "CREATE TABLE `" + table + "` (\n"
                + "        `id` int(11) NOT NULL AUTO_INCREMENT BY GROUP,\n"
                + "        `seller_id` int(11) DEFAULT NULL,\n"
                + "        PRIMARY KEY (`id`),\n"
                + "        KEY `auto_shard_key_seller_id_id` USING BTREE (`seller_id`, `id`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"
                + " PARTITION BY HASH(`seller_id`) PARTITIONS 8";
            String sql2 = "insert into " + table
                + " (seller_id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(9),(9),(9),(10),(88),(88),(88),(88);";
            String sql3 =
                "insert into " + table + " (seller_id) select seller_id from " + table + " where seller_id=88;";
            stmt.executeUpdate(sql1);
            stmt.executeUpdate(sql2);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
            stmt.executeUpdate(sql3);
        }

        // execute
        DdlCheckContext ddlCheckContext = newDdlCheckContext();
        ddlCheckContext.updateAndGetMarkList(schemaName);

        String tableGroup = queryTableGroup(schemaName, tables.get(0));
        String tokenHints = buildTokenHints();
        String sql = tokenHints + " alter tablegroup " + tableGroup + " extract to partition by hot value(88[]);";
        stmt.execute(sql);

        // check
        String expectSql =
            String.format("ALTER TABLEGROUP %s SPLIT INTO  PARTITIONS 1 BY HOT VALUE(88)", tableGroup);
        checkAfterAlterTableGroup(ddlCheckContext, tableGroup, expectSql,
            Lists.newArrayList(tableName1, tableName2));
    }

    private void testReorgTableGroup(Statement stmt) throws SQLException {
        // prepare table and data
        String tableGroup = StringUtils.isBlank(serverId) ? PartitionTableSqls.REORG_TABLE_GROUP_PREFIX :
            PartitionTableSqls.REORG_TABLE_GROUP_PREFIX + "_" + serverId;
        String createSql1 = String.format(PartitionTableSqls.CREATE_REORG_TABLE_GROUP, tableGroup);
        String createSql2 = String.format(PartitionTableSqls.CREATE_REORG_TABLE_1, tableGroup);
        String createSql3 = String.format(PartitionTableSqls.CREATE_REORG_TABLE_2, tableGroup);
        stmt.executeUpdate(createSql1);
        stmt.executeUpdate(createSql2);
        stmt.executeUpdate(createSql3);

        // execute and check
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        String reorgSql = String.format(PartitionTableSqls.REORG_TABLE_GROUP, tableGroup);
        String tokenHints = buildTokenHints();
        String rewriteSql = tokenHints + reorgSql;
        stmt.execute(rewriteSql);

        checkAfterAlterTableGroup(checkContext, tableGroup, rewriteSql,
            Lists.newArrayList("sp_list_list_1", "sp_list_list_2"));
    }

    private String testCommonDdl(DdlCheckContext checkContext, String schemaName, Statement stmt, String tableName1,
                                 PartitionType partitionType, boolean testWithGsi)
        throws SQLException {

        String sql;
        String tokenHints;

        //--------------------------------------------------------------------------------
        //-----------------------------------Test Columns---------------------------------
        //--------------------------------------------------------------------------------
        // Test Step 加列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format(
            "alter table %s add column add1 varchar(20) not null default '111'", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 加列，指定列顺序
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String
                .format("alter table %s add column add2 varchar(20) not null default '222' after b", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 加列+减列
        tokenHints = buildTokenHints();
        sql =
            tokenHints + String.format("alter table %s add column add3 bigint default 0,drop column add2", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 更改列精度
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s modify add1 varchar(50) not null default '111'", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 更改列
        tokenHints = buildTokenHints();
        sql = tokenHints + String
            .format("alter table %s change column add1 add111 varchar(50) not null default '111'", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add111", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step 减列
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s drop column add3", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        //--------------------------------------------------------------------------------
        //-------------------------------Test Local Indexes-------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add index idx_test(`b`)", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("alter table %s add unique idx_job(`c`)", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("create index idx_gmt on %s(`d`)", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        // Test Step
        // 对于含有聚簇索引的表，引擎不支持一个语句里drop两个index，所以一个语句包含两个drop的sql就不用测试了
        // 否则会报错：optimize error by Do not support multi ALTER statements on table with clustered index
        // tokenHints = buildTokenHints();
        // sql = tokenHints + String.format("alter table %s drop index idx_test", tableName1);
        // stmt.execute(sql);
        // Assert.assertEquals(sql, getDdlRecordSql(tokenHints));

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("drop index idx_gmt on %s", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        //--------------------------------------------------------------------------------
        //--------------------------------------Test Gsi----------------------------------
        //--------------------------------------------------------------------------------
        // single表和broadcast表，不支持gsi，无需测试
        if (partitionType.isPartitionTable() && testWithGsi) {
            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format(
                "CREATE GLOBAL INDEX g_i_test ON %s (`e`) PARTITION BY HASH(`e`)", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s add GLOBAL INDEX g_i_test11 ON %s (`f`) COVERING "
                + "(`GMT_CREATED`) PARTITION BY HASH(`f`)", tableName1, tableName1);
            //+ "add column add1 varchar(20) not null default '111'";//gsi不支持混合模式，省事儿了，不用测了
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("drop index g_i_test on %s", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s drop index g_i_test11", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s add clustered index `idx1122`(`e`) "
                + "PARTITION BY RANGE COLUMNS(e) (\n"
                + "PARTITION p0001 VALUES LESS THAN (100000000),\n"
                + "PARTITION p0002 VALUES LESS THAN (200000000),\n"
                + "PARTITION p0003 VALUES LESS THAN (300000000),\n"
                + "PARTITION p0004 VALUES LESS THAN (400000000),\n"
                + "PARTITION p0005 VALUES LESS THAN MAXVALUE\n"
                + ")", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("alter table %s drop index idx1122", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

            // Test Step
            tokenHints = buildTokenHints();
            sql = tokenHints + String.format(
                "alter table %s add column ccc111 timestamp default current_timestamp", tableName1);
            stmt.execute(sql);
            commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);
        }

        //--------------------------------------------------------------------------------
        //------------------------------------Test Truncate ------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        // 如果是带有GSI的表，会报错: Does not support truncate table with global secondary index，so use t_ddl_test_zzz for test
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("truncate table %s", tableName1);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);

        //--------------------------------------------------------------------------------
        //------------------------------------Test rename --------------------------------
        //--------------------------------------------------------------------------------
        // Test Step
        // 如果是带有GSI的表，会报错: Does not support modify primary table 't_ddl_test' cause global secondary index exists,so use t_ddl_test_yyy for test

        tokenHints = buildTokenHints();
        String newTableName = tableName1 + "_new";
        sql = tokenHints + String.format("rename table %s to %s", tableName1, newTableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, schemaName, tableName1, sql);
        return newTableName;
    }

    private void testTableGroupDdl(DdlCheckContext checkContext, Statement stmt, String schemaName, String tableName1,
                                   String tableName2, PartitionType partitionType, boolean createWithGsi)
        throws SQLException {
        String sql;
        String tokenHints;
        String tableGroup = queryTableGroup(schemaName, tableName2);
        checkContext.updateAndGetMarkList(schemaName);

        if (partitionType.isSupportAddDropValues()) {
            tokenHints = buildTokenHints();
            sql = tokenHints + getAddValuesSql(tableGroup, partitionType);
            stmt.execute(sql);
            checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));

            tokenHints = buildTokenHints();
            sql = tokenHints + getDropValuesSql(tableGroup, partitionType);
            stmt.execute(sql);
            checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));
        }

        if (partitionType.isSupportAddPartition()) {
            tokenHints = buildTokenHints();
            sql = tokenHints + getAddPartitionSql(tableGroup, partitionType);
            stmt.execute(sql);
            checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));
        }

        List<String> splitSqls = getSplitPartitionSql(tableGroup, tableName1, partitionType);
        for (String sqlItem : splitSqls) {
            tokenHints = buildTokenHints();
            sql = tokenHints + sqlItem;
            stmt.execute(sql);
            checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));
        }

        tokenHints = buildTokenHints();
        sql = tokenHints + getMergePartitionSql(tableGroup, partitionType);
        stmt.execute(sql);
        checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));

        if (partitionType.isSupportMovePartition()) {
            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql(tableGroup, tableName1, partitionType);
            stmt.execute(sql);
            checkAfterAlterTableGroupWithoutActualMark(checkContext, sql, Lists.newArrayList(tableName1, tableName2));
        }

        tokenHints = buildTokenHints();
        sql = tokenHints + getTruncatePartitionSql(tableName1, partitionType);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName1, sql);

        tokenHints = buildTokenHints();
        sql = tokenHints + getTruncatePartitionSql(tableName2, partitionType);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName2, sql);

        if (partitionType.isSupportDropPartition()) {
            tokenHints = buildTokenHints();
            sql = tokenHints + getDropPartitionSql(tableGroup, partitionType);
            stmt.execute(sql);
            checkAfterAlterTableGroup(checkContext, tableGroup, sql, Lists.newArrayList(tableName1, tableName2));
        }
    }

    private String getAddPartitionSql(String tableGroup, PartitionType partitionType) {
        if (partitionType == PartitionType.Range) {
            return String
                .format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p4 VALUES LESS THAN (2030))", tableGroup);
        } else if (partitionType == PartitionType.RangeColumn) {
            return String
                .format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p5 VALUES LESS THAN ('2041-01-01'))", tableGroup);
        } else if (partitionType == PartitionType.List) {
            return String
                .format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p5 VALUES IN (2060,2062))",
                    tableGroup);
        } else if (partitionType == PartitionType.ListColumn) {
            return String
                .format("ALTER TABLEGROUP %s ADD PARTITION (PARTITION p5 VALUES IN ('2060-01-01','2062-01-01'))",
                    tableGroup);
        }

        throw new RuntimeException("not supported partition type " + partitionType);
    }

    private String getAddValuesSql(String tableGroup, PartitionType partitionType) {
        if (partitionType == PartitionType.List) {
            return String
                .format("ALTER TABLEGROUP %s MODIFY PARTITION p0 ADD VALUES (1988,1989)",
                    tableGroup);
        } else if (partitionType == PartitionType.ListColumn) {
            return String
                .format("ALTER TABLEGROUP %s MODIFY PARTITION p0 ADD VALUES ('1988-01-01','1989-01-01')",
                    tableGroup);
        }

        throw new RuntimeException("not supported partition type " + partitionType);
    }

    private String getDropValuesSql(String tableGroup, PartitionType partitionType) {
        if (partitionType == PartitionType.List) {
            return String
                .format("ALTER TABLEGROUP %s MODIFY PARTITION p0 DROP VALUES (1990,1991)",
                    tableGroup);
        } else if (partitionType == PartitionType.ListColumn) {
            return String
                .format("ALTER TABLEGROUP %s MODIFY PARTITION p0 DROP VALUES ('1990-01-01','1991-01-01')",
                    tableGroup);
        }

        throw new RuntimeException("not supported partition type " + partitionType);
    }

    private String getDropPartitionSql(String tableGroup, PartitionType partitionType) {
        return String.format("ALTER TABLEGROUP %s DROP PARTITION p4", tableGroup);
    }

    private List<String> getSplitPartitionSql(String tableGroup, String tableName, PartitionType partitionType)
        throws SQLException {
        if (partitionType == PartitionType.Range) {
            return Lists.newArrayList(String
                .format("ALTER TABLEGROUP %s split PARTITION p2 INTO "
                    + "(PARTITION p21 VALUES LESS THAN (2005),PARTITION p22 VALUES LESS THAN (2010))", tableGroup));
        } else if (partitionType == PartitionType.RangeColumn) {
            return Lists.newArrayList(String.
                format("ALTER TABLEGROUP %s split PARTITION p2 into "
                        + "(PARTITION p21 VALUES LESS THAN ('2005-01-01'), PARTITION p22 VALUES LESS THAN ('2011-01-01'))",
                    tableGroup));
        } else if (partitionType == PartitionType.Hash) {
            List<String> list = getPartitionList(tableName);
            return Lists.newArrayList(
                String.format("ALTER TABLEGROUP %s SPLIT PARTITION %s", tableGroup, list.get(0))
            );
        } else if (partitionType == PartitionType.HashKey) {
            List<String> list = getPartitionList(tableName);
            return Lists.newArrayList(
                String.format("ALTER TABLEGROUP %s SPLIT PARTITION %s", tableGroup, list.get(0))
            );
        } else if (partitionType == PartitionType.List) {
            return Lists.newArrayList(
                String.format("ALTER TABLEGROUP %s SPLIT PARTITION p2 INTO "
                        + "(PARTITION p21 VALUES in (2010,2012), PARTITION p22 VALUES in (2013))",
                    tableGroup)
            );
        } else if (partitionType == PartitionType.ListColumn) {
            return Lists.newArrayList(
                String.format("ALTER TABLEGROUP %s SPLIT PARTITION p2 INTO "
                        + "(PARTITION p21 VALUES in ('2010-01-01','2012-01-01'), PARTITION p22 VALUES in ('2013-01-01'))",
                    tableGroup)
            );
        }
        throw new RuntimeException("not supported partition type " + partitionType);
    }

    private String getMergePartitionSql(String tableGroup, PartitionType partitionType) {
        return String.format("ALTER TABLEGROUP %s merge PARTITIONS p3,p4 to p4", tableGroup);
    }

    private String getTruncatePartitionSql(String tableName, PartitionType partitionType) throws SQLException {
        List<String> list = getPartitionList(tableName);
        return String.format("ALTER TABLE %s TRUNCATE PARTITION %s", tableName, list.get(0));
    }

}
