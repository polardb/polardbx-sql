package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class RemovePartitioningTest extends DDLBaseNewDBTestCase {

    @Test
    public void testRemovePartitioning() {
        String sql1 = "drop table if exists remove_partitioning_t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 =
            "create table remove_partitioning_t1(a int primary key, b varchar(100), index idxb(b)) partition by key(b)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "trace alter table remove_partitioning_t1 remove partitioning";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        List<List<String>> traceResult = getTrace(tddlConnection);
        for (List<String> row : traceResult) {
            for (String col : row) {
                if (col != null && col.toLowerCase().contains("CdcGsiDdlMarkTask".toLowerCase())) {
                    Assert.fail("CdcGsiDdlMarkTask should not be executed");
                }
            }
        }
    }

    @Test
    public void testRemovePartitioningWithAutoPartitionFalse() {
        String sql1 = "set auto_partition = false";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        try {
            sql1 = "drop table if exists remove_partitioning_t2";
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = "create table remove_partitioning_t2(a int primary key, b int) partition by key(a)";
            JdbcUtil.executeSuccess(tddlConnection, sql1);

            sql1 = "alter table remove_partitioning_t2 remove partitioning WITH TABLEGROUP = wumu_tgi1 IMPLICIT";
            JdbcUtil.executeSuccess(tddlConnection, sql1);

            sql1 = String.format(
                "select count(1) from information_schema.partitions where table_schema = '%s' and table_name = 'remove_partitioning_t2'",
                tddlDatabase1);
            try (ResultSet rs = JdbcUtil.executeQuery(sql1, tddlConnection)) {
                if (rs.next()) {
                    int partitions = rs.getInt(1);
                    Assert.assertTrue(partitions == 1);
                } else {
                    Assert.fail();
                }
            } catch (SQLException e) {
                Assert.fail();
            }
        } finally {
            sql1 = "set auto_partition = true";
            JdbcUtil.executeSuccess(tddlConnection, sql1);
        }
    }

    @Test
    public void testRemovePartitioningWithAutoPartitionTrue() {
        String sql1 = "drop table if exists remove_partitioning_t2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = "create table remove_partitioning_t2(a int primary key, b int) partition by key(a)";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = "alter table remove_partitioning_t2 remove partitioning";
        JdbcUtil.executeSuccess(tddlConnection, sql1);

        sql1 = String.format(
            "select count(1) from information_schema.partitions where table_schema = '%s' and table_name = 'remove_partitioning_t2'",
            tddlDatabase1);
        try (ResultSet rs = JdbcUtil.executeQuery(sql1, tddlConnection)) {
            if (rs.next()) {
                int partitions = rs.getInt(1);
                Assert.assertTrue(partitions == 3);
            } else {
                Assert.fail();
            }
        } catch (SQLException e) {
            Assert.fail();
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
