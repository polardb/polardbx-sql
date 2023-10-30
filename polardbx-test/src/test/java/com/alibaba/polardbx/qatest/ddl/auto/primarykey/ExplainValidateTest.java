package com.alibaba.polardbx.qatest.ddl.auto.primarykey;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExplainValidateTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testExplainDropAddPrimaryKey() {
        String tableName = "modify_pk_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint, d bigint, e bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s drop primary key, add primary key(b)", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testExplainDropAddPrimaryKeyWithGsi() {
        String tableName = "modify_pk_test_tbl3" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint, d bigint, e bigint, "
                + "global index gsi1(a,b,c) partition by key(b)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s drop primary key, add primary key(b)", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    @Test
    public void testExplainAddPrimaryKeyWithGsi() {
        String tableName = "modify_pk_test_tbl4" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, d bigint, e bigint,"
                + "global index gsi2(a,b,c) partition by key(b)) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s add primary key(a)", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }
    }
}