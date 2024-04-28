package com.alibaba.polardbx.qatest.ddl.auto.partitionkey;

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
    public void testExplainModifyPartitionKey() {
        String tableName = "modify_sk_test_tbl" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b bigint, c bigint, d bigint, e bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s modify column a varchar(10)", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
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

        sql = String.format("explain alter table %s modify column a varchar(11) comment \"test\"", tableName);
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs2);
        }

        sql = String.format("explain alter table %s modify column a varchar(12)", tableName);
        ResultSet rs3 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs3);
        }

        sql = String.format("explain alter table %s modify column a varchar(12) character set gbk", tableName);
        ResultSet rs4 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.getString(1).contains("CHARACTER SET gbk"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs3);
        }
    }

    @Test
    public void testExplainModifyPartitionKeyOMC() {
        String tableName = "modify_sk_test_tbl_omc" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11), b bigint, c bigint, d bigint, e bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s modify column a varchar(12)", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
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

        sql = String.format("explain alter table %s modify column a varchar(12), ALGORITHM=OMC", tableName);
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs2);
        }
    }

    @Test
    public void testExplainModifyPartitionKeyWithGsi() {
        String tableName = "modify_sk_test_tbl_gsi" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_sk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b varchar(10), c bigint, d bigint, e bigint) partition by key(`b`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add global index %s (a, b ,c) partition by key(a) partitions 3",
            tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s modify column b varchar(10) comment \"test\"", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("explain alter table %s modify column b varchar(8)", tableName);
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs2);
        }

        sql = String.format("explain alter table %s modify column b varchar(12)", tableName);
        ResultSet rs3 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs3);
        }

        sql = String.format("explain alter table %s modify column b int", tableName);
        ResultSet rs4 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs4);
        }

        sql = String.format("explain alter table %s modify column a varchar(10)", tableName);
        ResultSet rs5 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs5);
        }

        sql = String.format("explain alter table %s modify column a varchar(12)", tableName);
        ResultSet rs6 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs6);
        }

        sql = String.format("explain alter table %s modify column a varchar(11) comment \"test\"", tableName);
        ResultSet rs7 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs7);
        }

        sql = String.format("explain alter table %s modify column a int(12)", tableName);
        ResultSet rs8 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs8);
        }
    }

    @Test
    public void testExplainModifyPartitionKeyWithGsi2() {
        String tableName = "modify_sk_test_tbl_gsi2" + RandomUtils.getStringBetween(1, 5);
        String indexName = "modify_sk_gsi_test_tbl_idx2" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b varchar(10), c bigint, d bigint, e bigint) partition by key(`a`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add global index %s (a, b ,c) partition by key(b) partitions 3",
            tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s modify column b varchar(10) comment \"test\"", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("explain alter table %s modify column b varchar(8)", tableName);
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs2);
        }

        sql = String.format("explain alter table %s modify column b varchar(12)", tableName);
        ResultSet rs3 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs3);
        }

        sql = String.format("explain alter table %s modify column b int", tableName);
        ResultSet rs4 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs4);
        }

        sql = String.format("explain alter table %s modify column a varchar(10)", tableName);
        ResultSet rs5 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs5);
        }

        sql = String.format("explain alter table %s modify column a varchar(12)", tableName);
        ResultSet rs6 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs6);
        }

        sql = String.format("explain alter table %s modify column a varchar(11) comment \"test\"", tableName);
        ResultSet rs7 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs7);
        }

        sql = String.format("explain alter table %s modify column a int(12)", tableName);
        ResultSet rs8 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs8);
        }
    }

    @Test
    public void testExplainChangePartitionKeyWithGsi() {
        String tableName = "change_sk_test_tbl_gsi" + RandomUtils.getStringBetween(1, 5);
        String indexName = "change_sk_gsi_test_tbl_idx" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String sql = String.format(
            "create table %s (a varchar(11) primary key, b varchar(10), c bigint, d bigint, e bigint) partition by key(`b`) partitions 3",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s add global index %s (a, b ,c) partition by key(a) partitions 3",
            tableName, indexName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("explain alter table %s change column b b varchar(10) comment \"test\"", tableName);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs);
        }

        sql = String.format("explain alter table %s change column b b varchar(8)", tableName);
        ResultSet rs2 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs2.next());
            Assert.assertTrue(rs2.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs2);
        }

        sql = String.format("explain alter table %s change column b b varchar(12)", tableName);
        ResultSet rs3 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs3.next());
            Assert.assertTrue(rs3.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs3);
        }

        sql = String.format("explain alter table %s change column b b int", tableName);
        ResultSet rs4 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs4.next());
            Assert.assertTrue(rs4.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs4);
        }

        sql = String.format("explain alter table %s change column a a varchar(10)", tableName);
        ResultSet rs5 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs5.next());
            Assert.assertTrue(rs5.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs5);
        }

        sql = String.format("explain alter table %s change column a a varchar(12)", tableName);
        ResultSet rs6 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs6.next());
            Assert.assertTrue(rs6.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs6);
        }

        sql = String.format("explain alter table %s change column a a varchar(11) comment \"test\"", tableName);
        ResultSet rs7 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs7.next());
            Assert.assertTrue(rs7.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs7);
        }

        sql = String.format("explain alter table %s change column a a int(12)", tableName);
        ResultSet rs8 = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("ALTER_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("CREATE_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("DROP_TABLE"));
            Assert.assertTrue(rs8.next());
            Assert.assertTrue(rs8.getString(1).contains("EXCLUDE_RESOURCE"));
        } catch (SQLException e) {
            throw new RuntimeException("", e);
        } finally {
            JdbcUtil.close(rs8);
        }
    }
}
