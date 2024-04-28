package com.alibaba.polardbx.qatest.ddl.cdc.repartition;

import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-12-13 10:29
 **/
public class CdcRePartitionSub_FirstBaseTest extends CdcRePartitionBaseTest {

    public void testRePartitionDdl() throws SQLException {
        JdbcUtil.executeUpdate(tddlConnection, "drop database if exists " + dbName);
        JdbcUtil.executeUpdate(tddlConnection, "create database " + dbName + " mode=auto");
        JdbcUtil.executeUpdate(tddlConnection, "use " + dbName);

        try (Statement stmt = tddlConnection.createStatement()) {
            // ================ 测试对包含子分区的表做纯一级分区级变更操作 ================ //

            for (SubPartitionType partitionType : getSubPartitionTypes()) {
                DdlCheckContext checkContext = newDdlCheckContext();
                checkContext.updateAndGetMarkList(dbName);

                testSplitPartition(checkContext, stmt, partitionType);
                testMergePartition(checkContext, stmt, partitionType);
                testMovePartition(checkContext, stmt, partitionType);

                if (isSupportReorganizePartition(partitionType)) {
                    testReorganizePartition(checkContext, stmt, partitionType);
                }

                if (isSupportModifyPartition(partitionType)) {
                    testAddPartition(checkContext, stmt, partitionType);
                    testDropPartition(checkContext, stmt, partitionType);
                    testTruncatePartition(checkContext, stmt, partitionType);
                }

                if (isSupportModifyPartitionWithValues(partitionType)) {
                    testModifyPartitionAddValues(checkContext, stmt, partitionType);
                    testModifyPartitionDropValues(checkContext, stmt, partitionType);
                }
            }
        }
    }

    protected Set<SubPartitionType> getSubPartitionTypes() {
        throw new UnsupportedOperationException("unsupport");
    }

    // ===================== 纯一级分区级的变更操作 ==================== //

    private void testSplitPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test split table partition with partition type " + partitionType);
        String sql;

        if (KeyPartitionSet.contains(partitionType) || HashPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1";
        } else if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values less than ( to_days('2010-01-01') ),\n"
                + "  partition p12 values less than ( to_days('2020-01-01') ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + " ( partition p11 values in ( to_days('2020-01-01') ),\n"
                + "   partition p12 values in ( to_days('2020-01-02') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values less than ( '2010-01-01', 'abc' ),\n"
                + "  partition p12 values less than ( '2020-01-01', 'abc') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s split partition p1 into \n"
                + "( partition p11 values in ( ('2020-01-01', 'abc') ),\n"
                + "  partition p12 values in ( ('2020-01-02', 'abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testMergePartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test merge table partition with partition type " + partitionType);

        String sql = "alter %s %%s merge partitions p1,p2 to p12";

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testMovePartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test move table partition with partition type " + partitionType);
        String tableName;
        String sql;
        String tokenHints;

        tableName = createTable(checkContext, stmt, partitionType, true);
        tokenHints = buildTokenHints();
        sql = tokenHints + getMovePartitionSqlForTable(tableName);
        stmt.execute(sql);
        checkAfterAlterTablePartition(checkContext, sql, tableName);

        tableName = createTable(checkContext, stmt, partitionType, false);
        tokenHints = buildTokenHints();
        sql = tokenHints + getMovePartitionSqlForTable(tableName);
        stmt.execute(sql);
        checkAfterAlterTablePartition(checkContext, sql, tableName);
    }

    private void testAddPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info("start to test add table partition with partition type " + partitionType);
        String sql;

        if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values less than ( to_days('2022-01-01') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values less than ( '2022-01-01', 'abc' ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql =
                "alter %s %%s add partition ( partition p3 values in ( to_days('2022-01-01'),to_days('2022-01-02') ) )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s add partition ( partition p3 values in ( ('2022-01-01','abc'),('2022-01-02','abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testDropPartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test drop table partition with partition type " + partitionType);
        String sql = "alter %s %%s drop partition p1";
        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testModifyPartitionAddValues(DdlCheckContext checkContext, Statement stmt,
                                              SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test modify table partition add values with partition type " + partitionType);
        String sql;

        if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 add values ( to_days('2020-02-01'), to_days('2020-02-02') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 add values ( ('2020-02-01', 'abc'),('2020-02-02', 'abc') )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testModifyPartitionDropValues(DdlCheckContext checkContext, Statement stmt,
                                               SubPartitionType partitionType) throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test modify table partition drop values with partition type " + partitionType);
        String sql;

        if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 drop values ( to_days('2020-01-01') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s modify partition p1 drop values ( ('2020-01-01', 'abc') )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }

    private void testTruncatePartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info("start to test truncate partition with partition type " + partitionType);
        String tableName;
        String sql;
        String formatSql = "alter %s %%s truncate partition p1";

        tableName = createTable(checkContext, stmt, partitionType, true);

        checkContext.updateAndGetMarkList(dbName);
        String tokenHints = buildTokenHints();
        sql = String.format(formatSql, "table");
        sql = tokenHints + String.format(sql, tableName);
        stmt.execute(sql);
        commonCheckExistsAfterDdl(checkContext, dbName, tableName, sql);
    }

    private void testReorganizePartition(DdlCheckContext checkContext, Statement stmt, SubPartitionType partitionType)
        throws SQLException {
        AsyncDDLBaseNewDBTestCase.logger.info(
            "start to test reorganize table partition with partition type " + partitionType);
        String sql;

        if (RangePartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values less than ( to_days('2010-01-01') ),\n"
                + "  partition p12 values less than ( to_days('2020-01-01') ) )";
        } else if (ListPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + " ( partition p11 values in ( to_days('2020-01-01') ),\n"
                + "   partition p12 values in ( to_days('2020-01-02') ) )";
        } else if (RangeColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values less than ( '2010-01-01', 'abc' ),\n"
                + "  partition p12 values less than ( '2020-01-01', 'abc') )";
        } else if (ListColumnsPartitionSet.contains(partitionType)) {
            sql = "alter %s %%s reorganize partition p1 into \n"
                + "( partition p11 values in ( ('2020-01-01', 'abc') ),\n"
                + "  partition p12 values in ( ('2020-01-02', 'abc') ) )";
        } else {
            throw new RuntimeException("not supported partition type : " + partitionType);
        }

        doAlterPartition(checkContext, stmt, partitionType, sql, sql);
    }
}
