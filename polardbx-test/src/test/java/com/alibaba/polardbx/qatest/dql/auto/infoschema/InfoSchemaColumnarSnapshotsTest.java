package com.alibaba.polardbx.qatest.dql.auto.infoschema;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InfoSchemaColumnarSnapshotsTest extends AutoReadBaseTestCase {
    @Test
    public void testColumnarSnapshot() throws SQLException, InterruptedException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
        Assert.assertTrue(rs.next());
        long tso0 = rs.getLong(1);
        System.out.println(tso0);
        Thread.sleep(1000);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
        Assert.assertTrue(rs.next());
        long tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            Thread.sleep(1000);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 < tso1);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            Thread.sleep(1000);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+10:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 < tso1);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            Thread.sleep(1000);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '-10:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 < tso1);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00')");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00')");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 < tso1);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00')");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+10:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00')");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 > tso1);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '+2:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00')");
            Assert.assertTrue(rs.next());
            tso0 = rs.getLong(1);
            System.out.println(tso0);
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = '-10:00'");
            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00')");
            Assert.assertTrue(rs.next());
            tso1 = rs.getLong(1);
            System.out.println(tso1);
            Assert.assertTrue(tso0 < tso1);

        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set time_zone = 'SYSTEM'");
        }

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        Thread.sleep(1000);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '+2:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        Thread.sleep(1000);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '+10:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 > tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        Thread.sleep(1000);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now(), '-10:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00', '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00', '+2:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00', '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00', '+10:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 > tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00', '+2:00')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00', '-10:00')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 14:00:00', 'SYSTEM')");
        Assert.assertTrue(rs.next());
        tso0 = rs.getLong(1);
        System.out.println(tso0);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO('2024-01-01 15:00:00', 'SYSTEM')");
        Assert.assertTrue(rs.next());
        tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);
    }

    @Test
    public void testInformationSchemaColumnarSnapshots() throws SQLException, InterruptedException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
        Assert.assertTrue(rs.next());
        long tso0 = rs.getLong(1);
        System.out.println(tso0);
        Thread.sleep(1000);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT TIME_TO_TSO(now())");
        Assert.assertTrue(rs.next());
        long tso1 = rs.getLong(1);
        System.out.println(tso1);
        Assert.assertTrue(tso0 < tso1);

        final String schema = "testInformationSchemaColumnarSnapshots_schema";
        final String table = "testInformationSchemaColumnarSnapshots_table";
        final String index = "testInformationSchemaColumnarSnapshots_index";

        try (Connection connection = getMetaConnection()) {
            try {
                String sql = "delete from columnar_table_mapping where table_schema = '" + schema + "'";
                JdbcUtil.executeUpdateSuccess(connection, sql);
                sql = "delete from columnar_checkpoints where checkpoint_type = 'mock'";
                JdbcUtil.executeUpdateSuccess(connection, sql);

                sql =
                    "insert into columnar_table_mapping (table_schema, table_name, index_name, latest_version_id, status, type) "
                        + "values ('" + schema + "', '" + table + "', '" + index + "', 0, 'PUBLIC', 'snapshot')";
                JdbcUtil.executeUpdateSuccess(connection, sql);
                rs = JdbcUtil.executeQuerySuccess(connection, "select table_id from columnar_table_mapping "
                    + "where table_schema = '" + schema + "' and table_name = '" + table
                    + "' and index_name = '" + index + "'");
                Assert.assertTrue(rs.next());
                long tableId = rs.getLong(1);
                // index level
                sql =
                    "insert into columnar_checkpoints (logical_schema, logical_table, checkpoint_tso, binlog_tso, info, offset, checkpoint_type) values "
                        + "('" + schema + "', '" + tableId + "', '" + tso0 + "', '" + tso0 + "', 'force', '{}', 'mock')";
                JdbcUtil.executeUpdateSuccess(connection, sql);
                // global level
                sql = "insert into columnar_checkpoints (logical_schema, checkpoint_tso, binlog_tso, info, offset, checkpoint_type) values "
                    + "('polardbx', '" + tso1 + "', '" + tso1 + "', 'force', '{}', 'mock')";
                JdbcUtil.executeUpdateSuccess(connection, sql);

                sql = "select * from information_schema.columnar_snapshots where schema_name = '" + schema + "'";
                rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
                Assert.assertTrue(rs.next());
                Assert.assertTrue(schema.equalsIgnoreCase(rs.getString("SCHEMA_NAME")));
                Assert.assertTrue(table.equalsIgnoreCase(rs.getString("TABLE_NAME")));
                Assert.assertTrue(index.equalsIgnoreCase(rs.getString("INDEX_NAME")));
                Assert.assertEquals(tso0, rs.getLong("TSO"));

                sql = "select * from information_schema.columnar_snapshots where schema_name = 'polardbx' and tso = " + tso1;
                rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
                Assert.assertTrue(rs.next());
                Assert.assertTrue("polardbx".equalsIgnoreCase(rs.getString("SCHEMA_NAME")));
                Assert.assertEquals(tso1, rs.getLong("TSO"));
            } finally {
                String sql = "delete from columnar_table_mapping where table_schema = '" + schema + "'";
                JdbcUtil.executeIgnoreErrors(connection, sql);
                sql = "delete from columnar_checkpoints where checkpoint_type = 'mock'";
                JdbcUtil.executeIgnoreErrors(connection, sql);
            }
        }
    }
}
