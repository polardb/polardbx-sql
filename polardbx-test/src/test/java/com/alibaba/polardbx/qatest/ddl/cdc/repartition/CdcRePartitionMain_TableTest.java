package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.PartitionTableSqls;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.PartitionType;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-12 17:27
 **/
public class CdcRePartitionMain_TableTest extends CdcRePartitionBaseTest {

    public CdcRePartitionMain_TableTest() {
        dbName = "cdc_alter_tb_partition";
    }

    @Test
    public void testAlterTablePartition() throws SQLException {

        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "create database " + dbName + " mode = auto");
        JdbcUtil.executeUpdate(tddlConnection, "use " + dbName);

        try (Statement stmt = tddlConnection.createStatement()) {
            //test partition by
            testChangPartitionWithPartitionBy(stmt);

            //test split with hot key
            testHotKeySplitByTable(stmt);

            //test alter partition table with split
            testSplitTablePartition(stmt, PartitionType.Range);
            testSplitTablePartition(stmt, PartitionType.RangeColumn);
            testSplitTablePartition(stmt, PartitionType.List);
            testSplitTablePartition(stmt, PartitionType.ListColumn);
            testSplitTablePartition(stmt, PartitionType.Hash);
            testSplitTablePartition(stmt, PartitionType.HashKey);

            //test alter partition table with merge
            testMergeTablePartition(stmt, PartitionType.Range);
            testMergeTablePartition(stmt, PartitionType.RangeColumn);
            testMergeTablePartition(stmt, PartitionType.List);
            testMergeTablePartition(stmt, PartitionType.ListColumn);
            testMergeTablePartition(stmt, PartitionType.Hash);
            testMergeTablePartition(stmt, PartitionType.HashKey);

            //test alter partition table with add
            testAddTablePartition(stmt, PartitionType.Range);
            testAddTablePartition(stmt, PartitionType.RangeColumn);
            testAddTablePartition(stmt, PartitionType.List);
            testAddTablePartition(stmt, PartitionType.ListColumn);

            //test alter partition table with drop
            testDropTablePartition(stmt, PartitionType.Range);
            testDropTablePartition(stmt, PartitionType.RangeColumn);
            testDropTablePartition(stmt, PartitionType.List);
            testDropTablePartition(stmt, PartitionType.ListColumn);

            //test alter partition table with add values
            testAddValues(stmt, PartitionType.List);
            testAddValues(stmt, PartitionType.ListColumn);

            //test alter partition table with drop values
            testDropValues(stmt, PartitionType.List);
            testDropValues(stmt, PartitionType.ListColumn);

            //test alter partition table with move
            testMoveTablePartition(stmt, PartitionType.Range);
            testMoveTablePartition(stmt, PartitionType.RangeColumn);
            testMoveTablePartition(stmt, PartitionType.List);
            testMoveTablePartition(stmt, PartitionType.ListColumn);
            testMoveTablePartition(stmt, PartitionType.Hash);
            testMoveTablePartition(stmt, PartitionType.HashKey);
        }
    }

    private void testHotKeySplitByTable(Statement stmt) throws SQLException {
        String tableName = "t_orders_hotkey_test_by_table_1";
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        // create table
        String createSql = "CREATE TABLE `" + tableName + "` (\n"
            + "        `id` int(11) NOT NULL AUTO_INCREMENT,\n"
            + "        `seller_id` int(11) DEFAULT NULL,\n"
            + "        PRIMARY KEY (`id`),\n"
            + "        KEY `auto_shard_key_seller_id_id` USING BTREE (`seller_id`, `id`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4"
            + " PARTITION BY KEY(`seller_id`,`id`)  PARTITIONS 8";
        createTable(checkContext, tableName, stmt, createSql);

        // insert data
        String insertSql1 = "insert into " + tableName
            + " (seller_id) values(1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(88),(88),(88),(88);";
        String insertSql2 = "insert into " + tableName + " (seller_id) "
            + "select seller_id from " + tableName + " where seller_id=88;";
        stmt.executeUpdate(insertSql1);
        stmt.executeUpdate(insertSql2);
        stmt.executeUpdate(insertSql2);
        stmt.executeUpdate(insertSql2);

        // do split and check
        String tokenHints = buildTokenHints();
        String sql = tokenHints + " alter table " + tableName + " split into partitions 20 by hot value(88)";
        stmt.execute(sql);
        checkAfterAlterTablePartition(checkContext, sql, tableName);
    }

    private void testSplitTablePartition(Statement stmt, PartitionType partitionType)
        throws SQLException {
        logger.info("start to test split table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.Range) {
            tableName = "t_range_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p1 INTO \n"
                + "(PARTITION p10 VALUES LESS THAN (1994),\n"
                + "PARTITION p11 VALUES LESS THAN(1996),\n"
                + "PARTITION p12 VALUES LESS THAN(2000))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p2 "
                + "AT(2005) INTO (partition p21, partition p22)", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.RangeColumn) {
            tableName = "t_range_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p2 INTO \n"
                + "(PARTITION p10 VALUES LESS THAN ('2005-01-01'),\n"
                + "PARTITION p11 VALUES LESS THAN('2008-01-01'),\n"
                + "PARTITION p12 VALUES LESS THAN('2011-01-01'))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p2 INTO \n"
                + "(PARTITION p21 VALUES IN(2010),\n"
                + "PARTITION p22 VALUES IN(2012),\n"
                + "PARTITION p23 VALUES IN(2013))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s EXTRACT TO PARTITION pnew BY HOT VALUE(1990)", tableName);
            stmt.execute(sql);
            String rewriteSql = String.format("ALTER TABLE %s split PARTITION p0 into "
                + "(PARTITION pnew VALUES IN((1990)) , "
                + "PARTITION `p0` VALUES IN (1991,1992) ENGINE = InnoDB)", tableName);
            checkAfterAlterTablePartition(checkContext, rewriteSql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p3 INTO \n"
                + "(PARTITION p31 VALUES IN('2020-01-01'),\n"
                + "PARTITION p32 VALUES IN('2022-01-01'))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.Hash) {
            tableName = "t_hash_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p1", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.HashKey) {
            tableName = "t_hash_key_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_KEY_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT PARTITION p1", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

            for (int i = 0; i < 100; i++) {
                sql = String.format("insert into %s values(%s,now(),now(),'a',now(),%s,1,1,1,1)",
                    tableName, i + 1, i < 50 ? 1000 : 2000);
                stmt.execute(sql);
            }

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s EXTRACT TO PARTITION hp1 "
                + "BY HOT VALUE(1000)", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext,
                String.format("ALTER TABLE %s SPLIT INTO hp1 PARTITIONS 1 BY HOT VALUE(1000)", tableName), tableName);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s SPLIT INTO hp2 PARTITIONS 5 "
                + "BY HOT VALUE(2000)", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testMergeTablePartition(Statement stmt, PartitionType partitionType)
        throws SQLException {
        logger.info("start to test merge table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.Range) {
            tableName = "t_range_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.RangeColumn) {
            tableName = "t_range_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.Hash) {
            tableName = "t_hash_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.HashKey) {
            tableName = "t_hash_key_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_KEY_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s MERGE PARTITIONS p2,p3 to p23", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testAddTablePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test add table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.Range) {
            tableName = "t_range_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s ADD PARTITION (PARTITION p99 values less than(2030))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.RangeColumn) {
            tableName = "t_range_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s ADD PARTITION (PARTITION p99 values less than('2041-01-01'))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format(
                "ALTER TABLE %s ADD PARTITION (PARTITION p99 values in (2053,2054))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format(
                "ALTER TABLE %s ADD PARTITION (PARTITION p99 values in ('2053-01-01','2054-01-01'))", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testDropTablePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test drop table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.Range) {
            tableName = "t_range_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s DROP PARTITION p1,p2", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.RangeColumn) {
            tableName = "t_range_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s DROP PARTITION p1,p2", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s DROP PARTITION p1,p2", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String.format("ALTER TABLE %s DROP PARTITION p1,p2", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testAddValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table partition add values with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s MODIFY PARTITION p0 ADD VALUES (1988,1989)", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s MODIFY PARTITION p0 ADD VALUES ('1988-01-01','1989-01-01')", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testDropValues(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test modify table partition drop values with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s MODIFY PARTITION p0 DROP VALUES (1990,1991)", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + String
                .format("ALTER TABLE %s MODIFY PARTITION p0 DROP VALUES ('1990-01-01','1991-01-01')", tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testMoveTablePartitionWithOss(Statement stmt) throws SQLException {
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //prepare base resource
        String baseTableName1 = "t_hash_table_innodb_" + System.currentTimeMillis();
        String baseTableCreateSql1 = String.format("CREATE TABLE `%s` (\n"
            + "\t`id` bigint(20) DEFAULT NULL,\n"
            + "\t`gmt_modified` datetime NOT NULL,\n"
            + "\tPRIMARY KEY (`gmt_modified`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`gmt_modified`)\n"
            + "PARTITIONS 8\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n", baseTableName1, Calendar.getInstance().get(Calendar.YEAR) - 1);
        createTable(checkContext, baseTableName1, stmt, baseTableCreateSql1);

        String baseTableName2 = "t_hash_table_innodb_" + System.currentTimeMillis();
        String baseTableCreateSql2 = String.format("CREATE TABLE `%s` (\n"
            + "\t`id` bigint(20) DEFAULT NULL,\n"
            + "\t`gmt_modified` datetime NOT NULL,\n"
            + "\tPRIMARY KEY (`gmt_modified`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
            + "PARTITION BY KEY(`gmt_modified`)\n"
            + "PARTITIONS 8\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH '%s-01-01'\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n", baseTableName2, Calendar.getInstance().get(Calendar.YEAR) - 1);
        createTable(checkContext, baseTableName2, stmt, baseTableCreateSql2);

        try {
            String fileStoragePath = "'file_uri' ='file:///tmp/orc_" + System.currentTimeMillis() + "/'";
            String createDataSourceSql = "create filestorage local_disk with (" + fileStoragePath + ");";
            stmt.execute(createDataSourceSql);
        } catch (SQLException t) {
            if (!StringUtils.contains(t.getMessage(), "FileStorage LOCAL_DISK already exists ")) {
                throw t;
            }
        }

        // test alter table move partition
        String ossTableName0 = "oss_0";
        String ossTableCreateSql0 = "create table " + ossTableName0 + " like `" + baseTableName1 +
            "` engine = 'local_disk' archive_mode = 'TTL'";
        stmt.execute(ossTableCreateSql0);

        String tokenHints = buildTokenHints();
        String sql = tokenHints + getMovePartitionSql4Table(ossTableName0);
        stmt.execute(sql);
        Assert.assertEquals(0, getDdlRecordInfoListByToken(tokenHints).size());

        // test alter tablegroup move partiton
        String ossTableName1 = "oss_1";
        String ossTableCreateSql1 = "create table " + ossTableName1 + " like `" + baseTableName2 +
            "` engine = 'local_disk' archive_mode = 'TTL'";
        stmt.execute(ossTableCreateSql1);

        String tableGroup = queryTableGroup(dbName, ossTableName1);
        tokenHints = buildTokenHints();
        sql = tokenHints + getMovePartitionSql(tableGroup, ossTableName1, PartitionType.Hash);
        stmt.execute(sql);
        Assert.assertEquals(0, getDdlRecordInfoListByToken(tokenHints).size());
    }

    private void testMoveTablePartition(Statement stmt, PartitionType partitionType) throws SQLException {
        logger.info("start to test move table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        //create table
        if (partitionType == PartitionType.Range) {
            tableName = "t_range_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.RangeColumn) {
            tableName = "t_range_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_RANGE_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.List) {
            tableName = "t_list_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.ListColumn) {
            tableName = "t_list_column_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_LIST_COLUMN_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.Hash) {
            tableName = "t_hash_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else if (partitionType == PartitionType.HashKey) {
            tableName = "t_hash_key_table_" + System.currentTimeMillis();
            String createSql = String.format(PartitionTableSqls.CREATE_HASH_KEY_TABLE_SQL, tableName);
            createTable(checkContext, tableName, stmt, createSql);

            tokenHints = buildTokenHints();
            sql = tokenHints + getMovePartitionSql4Table(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testChangPartitionWithPartitionBy(Statement statement) throws SQLException {
        List<String> modes = Lists.newArrayList("single", "broadcast", "");
        List<String> types = Lists.newArrayList("ByHash", "ByList", "ByRange", "ByValue", "ByUdfHash");
        String createTemplate = "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `HoXnTC2GKooe` DATE NOT NULL COMMENT '7bcNGv6JIzT5S',\n"
            + "  `6T` MEDIUMINT ZEROFILL COMMENT 'FLY',\n"
            + "  `Sj6a0mr5h` TIMESTAMP UNIQUE COMMENT 'fPLG4KPU',\n"
            + "  `jMADtMgEJAj` SMALLINT UNSIGNED UNIQUE COMMENT 'gJEHVGxsh',\n"
            + "  `qpxruVH23` DATE,\n"
            + "  `sdjfls` varchar(100),\n"
            + "  `IvaHy8xl9rhsU4` SMALLINT(4) UNSIGNED NOT NULL,\n"
            + "  `sKz3` MEDIUMINT UNSIGNED ZEROFILL COMMENT 'oKR59Tzxfy',\n"
            + "  PRIMARY KEY (`6T`, `jMADtMgEJAj` DESC)\n"
            + ") %s DEFAULT CHARSET = `utf8mb4` DEFAULT COLLATE = `utf8mb4_general_ci`";
        for (String mode : modes) {
            for (String type : types) {
                DdlCheckContext checkContext = newDdlCheckContext();
                checkContext.updateAndGetMarkList(dbName);

                String tableName = "t_cdc_" + mode + "_" + type.toLowerCase();
                String createSql = String.format(createTemplate, tableName, mode);
                createTable(checkContext, tableName, statement, createSql);

                String alterSql;
                if ("ByHash".endsWith(type)) {
                    alterSql = "ALTER TABLE `" + tableName + "` PARTITION BY HASH (`IvaHy8xl9rhsU4`)";
                } else if ("ByList".endsWith(type)) {
                    alterSql = "alter table " + tableName + "   partition by list columns (`jMADtMgEJAj`)\n"
                        + "  ( partition p0 values in (10,30),\n"
                        + "  partition p1 values in (20,40) )";
                } else if ("ByRange".endsWith(type)) {
                    alterSql = "ALTER TABLE `" + tableName + "` PARTITION BY RANGE COLUMNS(`sdjfls`,`sKz3`) (\n"
                        + "PARTITION p1 VALUES LESS THAN ('abc', maxvalue),\n"
                        + "PARTITION p2 VALUES LESS THAN ('efg', maxvalue),\n"
                        + "PARTITION p3 VALUES LESS THAN ('hij', maxvalue)\n"
                        + ")";
                } else if ("ByValue".endsWith(type)) {
                    // not support yet
                    continue;
                } else if ("ByUdfHash".endsWith(type)) {
                    // not support yet
                    continue;
                } else {
                    throw new RuntimeException("unsupported type " + type);
                }

                statement.execute(alterSql);
                checkAfterAlterTablePartition(checkContext, alterSql, tableName);
            }
        }
    }

    private void createTable(DdlCheckContext checkContext, String tableName, Statement stmt,
                             String ddl)
        throws SQLException {
        checkContext.updateAndGetMarkList(dbName);
        String tokenHints = buildTokenHints();
        final String sql = tokenHints + ddl;
        stmt.execute(sql);
        checkAfterCreateTable(checkContext, tableName, sql);
    }
}
