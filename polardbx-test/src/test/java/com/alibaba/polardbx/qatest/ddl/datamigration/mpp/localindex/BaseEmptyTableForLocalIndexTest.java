package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.localindex;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange.PkTest;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@NotThreadSafe
@RunWith(Parameterized.class)
public class BaseEmptyTableForLocalIndexTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public BaseEmptyTableForLocalIndexTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("empty_table", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public void repartitionTest(String schemaName, String tableName, String createTableStmt, String alterTableStmt)
        throws SQLException {
        repartitionTest(schemaName, tableName, createTableStmt, alterTableStmt, null);

    }
    public void repartitionTest(String schemaName, String tableName, String createTableStmt, String alterTableStmt, List<String> gsiNames)
        throws SQLException {
        repartitionTest(schemaName, tableName, createTableStmt, alterTableStmt, gsiNames, true);
    }
    public void repartitionTest(String schemaName, String tableName, String createTableStmt, String alterTableStmt, List<String> gsiNames, Boolean isAuto)
        throws SQLException {
        if(isAuto) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                " create database if not exists " + schemaName + " mode = auto");
        }else{
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                " create database if not exists " + schemaName + " mode = drds");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, " use " + schemaName);
        String buildLocalIndexLaterHint = "/*+TDDL:cmd_extra(GSI_BUILD_LOCAL_INDEX_LATER=true,GSI_BACKFILL_BY_PK_RANGE=true,GSI_BACKFILL_BY_PK_PARTITION=false)*/";
        String nonBuildLocalIndexLaterHint = "/*+TDDL:cmd_extra(GSI_BUILD_LOCAL_INDEX_LATER=false,GSI_BACKFILL_BY_PK_RANGE=false,GSI_BACKFILL_BY_PK_PARTITION=false)*/";
        String compareTableName = tableName.replace("original", "compare");
        String createOriginalTableStmt = String.format(createTableStmt, tableName);
        String createCompareTableStmt = createOriginalTableStmt.replace("original", "compare");
        String alterOriginalTableStmt = buildLocalIndexLaterHint + String.format(alterTableStmt, tableName);
        String alterCompareTableStmt = nonBuildLocalIndexLaterHint + alterOriginalTableStmt.replace("original", "compare");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + compareTableName);

        JdbcUtil.executeUpdateSuccess(tddlConnection, createCompareTableStmt);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createOriginalTableStmt);

        JdbcUtil.executeUpdateSuccess(tddlConnection, alterOriginalTableStmt);
        JdbcUtil.executeUpdateSuccess(tddlConnection, alterCompareTableStmt);

        String stmt1 = DdlStateCheckUtil.getGsiCreateTable(tddlConnection, schemaName, tableName, tableName);
        String stmt2 = DdlStateCheckUtil.getGsiCreateTable(tddlConnection, schemaName, compareTableName, compareTableName);
        logger.info("result: ref1 " + stmt1);
        logger.info("result: ref2 " + stmt2);
        if(!DdlStateCheckUtil.compareForLocalIndex(stmt1, stmt2)){
            logger.info("bad result");
            throw new RuntimeException();
        }

        if(gsiNames != null){
            for(String gsiName:gsiNames){
                String compareGsiName = gsiName.replace("original", "compare");
                String gsiStmt1 = DdlStateCheckUtil.getGsiCreateTableStmt(tddlConnection, schemaName, tableName, gsiName);
                String gsiStmt2 = DdlStateCheckUtil.getGsiCreateTableStmt(tddlConnection, schemaName, compareTableName, compareGsiName);
                logger.info("result: ref1 " + gsiStmt1);
                logger.info("result: ref2 " + gsiStmt2);
                if(!DdlStateCheckUtil.compareForLocalIndex(gsiStmt1, gsiStmt2)){
                    logger.info("bad result");
                    throw new RuntimeException();
                }
            }
        }
        if(!DdlStateCheckUtil.checkTableStatus(tddlConnection, null, tableName)){
            throw new RuntimeException(" check table error for table " + tableName);
        }
    }

    @Test
    public void testRepartitionForPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "auto_partition_table_repartition_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") partition by hash(b) partitions 16;";

        String repartitionStmt =
            " alter table %s partition by hash(c) partitions 16;";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testRepartition1ForPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "auto_partition_table_repartition1_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a), LOCAL KEY(a)"
                + ") partition by hash(b) partitions 16;";

        String repartitionStmt =
            " alter table %s partition by hash(c) partitions 16;";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }


    @Test
    public void testModifyPartitionKeyForPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "modify_partition_key_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") partition by hash(b) partitions 16;";

        String repartitionStmt =
            " alter table %s modify column b varchar(32)";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testExpandPartitionKeyForPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "expand_partition_key_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") partition by hash(b) partitions 16;";

        String repartitionStmt =
            " alter table %s partition by key(b, c) partitions 16";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testSingleToPartitionTable1() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "expand_partition_key_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") single;";

        String repartitionStmt =
            " alter table %s partition by key(a) partitions 1";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testRemovePartitionKeyForPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "remove_partition_original";
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") partition by hash(b) partitions 16;";

        String repartitionStmt =
            " alter table %s remove partitioning";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testRemovePartitionKeyForAutoPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "remove_partition_for_auto_partition_original";
        String createTableStmt =
            "/*+TDDL:cmd_extra(AUTO_PARTITION=true)*/create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ")";

        String repartitionStmt =
            " alter table %s remove partitioning";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testBroadcastForAutoPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "broadcast_auto_partition_table_original";
        String createTableStmt =
            "/*+TDDL:cmd_extra(AUTO_PARTITION=true)*/create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ")";

        String repartitionStmt =
            " alter table %s broadcast ";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testChangePartitionNumForAutoPartitionTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "change_partition_num_original";
        String createTableStmt =
            "/*+TDDL:cmd_extra(AUTO_PARTITION=true)*/create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ")";

        String repartitionStmt =
            " alter table %s partitions 31";
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt);
    }

    @Test
    public void testRepartitionWithGsi() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "something_original";
        String createTableStmt =
            "CREATE TABLE `%s` (\n"
                + "\t`a` int(11) DEFAULT NULL,\n"
                + "\t`b` int(11) DEFAULT NULL,\n"
                + "\t`c` int(11) DEFAULT NULL,\n"
                + "\tGLOBAL INDEX `g_i_b_original` (`b`) COVERING (`a`)\n"
                + "\t\tPARTITION BY KEY(`b`)\n"
                + "\t\tPARTITIONS 3,\n"
                + "\tKEY `auto_shard_key_a` USING BTREE (`a`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
                + "PARTITION BY KEY(`a`)\n"
                + "PARTITIONS 8";

        String repartitionStmt =
            " alter table %s partition by hash(b) partitions 32";
        List<String> gsiNames = Lists.newArrayList("g_i_b_original");
        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt, gsiNames);
    }

//    @Test
//    public void testAddClusterIndex() throws Exception{
//        String schemaName = "pk_range_test_drds";
//        String originalTableName = "add_cluster_index_original";
//        String createTableStmt =
//            "CREATE PARTITION TABLE `add_cluster_index_original` (\n"
//                + "\t`t` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
//                + "\t`x` int(11) DEFAULT '3',\n"
//                + "\t`y` int(11) DEFAULT NULL,\n"
//                + "\t`z` int(11) DEFAULT NULL,\n"
//                + "\t`i` int(11) NOT NULL,\n"
//                + "\t`j` decimal(6, 2) DEFAULT NULL,\n"
//                + "\t`order_id` varchar(20) DEFAULT NULL,\n"
//                + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
//                + "\tLOCAL KEY `_local_c_i_idx_with_clustered_o` USING BTREE (`seller_id`),\n"
//                + "\tCLUSTERED INDEX `c_i_idx_with_clustered_o_original` USING BTREE(`seller_id`) DBPARTITION BY HASH(`seller_id`) tbpartition by hash(`seller_id`)\n"
//                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4";
//        String repartitionStmt =
//            " create clustered index g_i_i_original on %s(i)";
//        List<String> gsiNames = Lists.newArrayList("g_i_i_orignal", "c_i_idx_with_clustered_o_original");
//        repartitionTest(schemaName, originalTableName, createTableStmt, repartitionStmt, gsiNames, false);
//    }
}