package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.cdc.util.CdcTestUtil.getServerId4Check;

/**
 * 针对子分区表的DDL打标测试
 * <p>
 * 纯一级分区级别变更操作:对一级分区进行的操作不涉及子分区
 * 非纯一级分区级别变更操作:对一级分区进行操作的时候定义子分区, 主要包括三种分区操作:split, add, reorganize
 *
 * @author yudong
 * @since 2023/1/31 15:15
 **/
public class CdcRePartitionSub_SecondTest extends CdcRePartitionBaseTest {

    public CdcRePartitionSub_SecondTest() {
        dbName = "cdc_sub_partition_second";
    }

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        DdlCheckContext checkContext = newDdlCheckContext();
        checkContext.updateAndGetMarkList(dbName);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists " + dbName;
            stmt.execute(sql);
            List<DdlRecordInfo> ddlRecordInfoList = getDdlRecordInfoListByToken(tokenHints);
            Assert.assertEquals(sql, ddlRecordInfoList.get(0).getDdlSql());
            Assert.assertEquals(getServerId4Check(serverId), ddlRecordInfoList.get(0).getDdlExtInfo().getServerId());

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database " + dbName + " mode = 'auto' ";
            stmt.execute(sql);
            ddlRecordInfoList = getDdlRecordInfoListByToken(tokenHints);
            Assert.assertEquals(sql, ddlRecordInfoList.get(0).getDdlSql());
            Assert.assertEquals(getServerId4Check(serverId), ddlRecordInfoList.get(0).getDdlExtInfo().getServerId());

            sql = "use " + dbName;
            stmt.execute(sql);

            // ================ 测试对包含子分区的表做纯子分区级变更操作 ================ //

            testSplitSubPartition(checkContext, stmt, SubPartitionType.Key_Key);
            testMergeSubPartition(checkContext, stmt, SubPartitionType.Key_Key);
            testMoveSubPartition(checkContext, stmt, SubPartitionType.Key_Key);
            testAddSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
            testDropSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
            testModifySubPartitionDropSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
            testModifySubPartitionAddValues(checkContext, stmt, SubPartitionType.List_List);
            testModifySubPartitionDropValues(checkContext, stmt, SubPartitionType.List_List);
            testTruncateSubPartition(checkContext, stmt, SubPartitionType.Key_Key);
            testReorganizeSubPartition(checkContext, stmt, SubPartitionType.List_List);

            // ================ 测试对包含子分区的表做非纯一级分区级变更操作 ================ //

            testSplitPartitionWithSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
            testAddPartitionWithSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
            testReorganizePartitionWithSubPartition(checkContext, stmt, SubPartitionType.Range_Range);
        }
    }

    // ===================== 纯子分区级的变更操作 ==================== //

    private void testSplitSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test split table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.Key_Key) {
            sqlForTP = "alter %s %%s split subpartition sp1";
            sqlForNTP = "alter %s %%s split subpartition p1sp1";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testMergeSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test merge table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.Key_Key) {
            sqlForTP = "alter %s %%s merge subpartitions sp1,sp2 to sp12";
            sqlForNTP = "alter %s %%s merge subpartitions p1sp1,p1sp2 to p1sp12";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testMoveSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test move table sub partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;

        if (partitionType == SubPartitionType.Key_Key) {
            // 模版化定义的子分区不支持move操作
            tableName = createTable(checkContext, stmt, partitionType, false);

            checkContext.updateAndGetMarkList(dbName);
            tokenHints = buildTokenHints();
            sql = tokenHints + getMoveSubPartitionSqlForTable(tableName);
            stmt.execute(sql);
            checkAfterAlterTablePartition(checkContext, sql, tableName);
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testAddSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test add table partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.Range_Range) {
            sqlForTP = "alter %s %%s add subpartition ( subpartition sp3 values less than ( 3000 ) )";
            sqlForNTP =
                "alter %s %%s modify partition p1 add subpartition ( subpartition p1sp3 values less than ( 3000 ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testDropSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test drop sub partition with partition type " + partitionType);
        String sqlForTP;

        if (partitionType == SubPartitionType.Range_Range) {
            // 从特定分区删除非模版化子分区，使用modify partition drop subpartition
            sqlForTP = "alter %s %%s drop subpartition sp2";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, null);
    }

    private void testModifySubPartitionDropSubPartition(DdlCheckContext checkContext, Statement stmt,
                                                        SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test modify partition drop sub partition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == SubPartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s modify partition p1 drop subpartition p1sp1";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, null, sqlForNTP);
    }

    private void testModifySubPartitionAddValues(DdlCheckContext checkContext, Statement stmt,
                                                 SubPartitionType partitionType) throws SQLException {
        logger.info("start to test modify table sub partition add values with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.List_List) {
            sqlForTP = "alter %s %%s modify subpartition sp1 add values ( 100, 200 )";
            sqlForNTP = "alter %s %%s modify subpartition p1sp1 add values ( 100, 200 )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testModifySubPartitionDropValues(DdlCheckContext checkContext, Statement stmt,
                                                  SubPartitionType partitionType) throws SQLException {
        logger.info("start to test modify table sub partition drop values with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.List_List) {
            sqlForTP = "alter %s %%s modify subpartition sp1 drop values ( 2000 )";
            sqlForNTP = "alter %s %%s modify subpartition p1sp1 drop values ( 2000 )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    private void testTruncateSubPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test truncate sub partition with partition type " + partitionType);
        String sql;
        String tableName;

        if (partitionType == SubPartitionType.Key_Key) {
            checkContext.updateAndGetMarkList(dbName);

            // 模版化定义的子分区不支持截断
            String formatSql = "alter %s %%s truncate subpartition p1sp1";
            tableName = createTable(checkContext, stmt, partitionType, true);
            String tokenHints = buildTokenHints();
            sql = String.format(formatSql, "table");
            sql = tokenHints + String.format(sql, tableName);
            stmt.execute(sql);

            commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);

        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }
    }

    private void testReorganizeSubPartition(DdlCheckContext checkContext, Statement stmt,
                                            SubPartitionType partitionType)
        throws SQLException {
        logger.info("start to test reorganize table sub partition with partition type " + partitionType);
        String sqlForTP;
        String sqlForNTP;

        if (partitionType == SubPartitionType.List_List) {
            sqlForTP = "alter %s %%s reorganize subpartition sp1 into "
                + "( subpartition sp11 values in (1000), subpartition sp12 values in (2000) )";
            sqlForNTP = "alter %s %%s reorganize subpartition p1sp1 into "
                + "( subpartition p1sp11 values in (1000), subpartition p1sp12 values in (2000) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sqlForTP, sqlForNTP);
    }

    // ===================== 非纯一级分区级的变更操作 ==================== //

    private void testSplitPartitionWithSubPartition(DdlCheckContext checkContext, Statement stmt,
                                                    SubPartitionType partitionType) throws SQLException {
        logger.info("start to test split partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == SubPartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s split partition p1 into (\n"
                + "partition p11 values less than ( to_days('2010-01-01') ) subpartitions 4 (\n"
                + "  subpartition p11sp1 values less than (500),\n"
                + "  subpartition p11sp2 values less than (1000),\n"
                + "  subpartition p11sp3 values less than (1500),\n"
                + "  subpartition p11sp4 values less than (2000)\n"
                + "),\n"
                + "partition p12 values less than ( to_days('2020-01-01') ) subpartitions 3 (\n"
                + "  subpartition p12sp1 values less than (500),\n"
                + "  subpartition p12sp2 values less than (1000),\n"
                + "  subpartition p12sp3 values less than (2000)\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, null, sqlForNTP);
    }

    private void testAddPartitionWithSubPartition(DdlCheckContext checkContext, Statement stmt,
                                                  SubPartitionType partitionType) throws SQLException {
        logger.info("start to test add partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == SubPartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s add partition (\n"
                + "partition p3 values less than ( to_days('2022-01-01') ) subpartitions 2 (\n"
                + "  subpartition p3sp1 values less than ( 1000 ),\n"
                + "  subpartition p3sp2 values less than ( 2000 )\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, null, sqlForNTP);
    }

    private void testReorganizePartitionWithSubPartition(DdlCheckContext checkContext, Statement stmt,
                                                         SubPartitionType partitionType)
        throws SQLException {
        logger.info(
            "start to test reorganize partition with sub partition definition with partition type " + partitionType);
        String sqlForNTP;

        if (partitionType == SubPartitionType.Range_Range) {
            sqlForNTP = "alter %s %%s reorganize partition p1 into (\n"
                + "partition p11 values less than ( to_days('2010-01-01') ) subpartitions 4 (\n"
                + "  subpartition p11sp1 values less than (500),\n"
                + "  subpartition p11sp2 values less than (1000),\n"
                + "  subpartition p11sp3 values less than (1500),\n"
                + "  subpartition p11sp4 values less than (2000)\n"
                + "),\n"
                + "partition p12 values less than ( to_days('2020-01-01') ) subpartitions 3 (\n"
                + "  subpartition p12sp1 values less than (500),\n"
                + "  subpartition p12sp2 values less than (1000),\n"
                + "  subpartition p12sp3 values less than (2000)\n"
                + ")\n"
                + ")";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, null, sqlForNTP);
    }

}
