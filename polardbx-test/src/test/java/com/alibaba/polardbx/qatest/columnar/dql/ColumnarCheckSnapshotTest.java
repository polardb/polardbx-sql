package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnarCheckSnapshotTest extends ColumnarReadBaseTestCase {

    private static final String TABLE_1 = "test_check_snapshot";
    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";
    private static final String DROP_TABLE = "drop table if exists %s";
    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";
    private static final String CHECK_SNAPSHOT = "check columnar snapshot %s";
    private static final int PART_COUNT = 4;

    @Before
    public void prepareTable() throws SQLException {
        dropTable();

        try (Connection tddlConnection = getColumnarConnection()) {
            String sql = String.format(CREATE_TABLE, TABLE_1);
            JdbcUtil.executeSuccess(tddlConnection, sql);
            for (int i = 0; i < 100; ++i) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, i, i));
            }
            ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_1, TABLE_1,
                "c1", "c1", PART_COUNT);
            for (int i = 100; i < 200; ++i) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, i, i));
            }
        }
    }

    @After
    public void dropTable() throws SQLException {
        try (Connection tddlConnection = getColumnarConnection()) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_1));
        }
    }

    @Test
    public void testCheckSnapshot() throws SQLException {
        try (Connection tddlConnection = getColumnarConnection()) {
            String sql = String.format(CHECK_SNAPSHOT, TABLE_1);
            try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
                int rowCount = 0;
                while (rs.next()) {
                    ++rowCount;
                    Assert.assertEquals("OK", rs.getString("Result"));
                    Assert.assertEquals("", rs.getString("Diff"));
                    Assert.assertEquals("NONE", rs.getString("Advice"));
                }
                Assert.assertEquals(PART_COUNT, rowCount);
            }

            String tableId = null;
            sql = "show columnar status";
            try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
                while (rs.next()) {
                    String schemaName = rs.getString("SCHEMA_NAME");
                    String tableName = rs.getString("TABLE_NAME");
                    if (schemaName.equalsIgnoreCase(DB_NAME) && tableName.equalsIgnoreCase(TABLE_1)) {
                        tableId = rs.getString("ID");
                        break;
                    }
                }
            }

            Assert.assertNotNull(tableId);
            String partitionName = "p1";
            // inject failure
            sql = String.format(
                "update metadb.files set file_name = concat('fake_', file_name) where logical_schema_name='%s' and logical_table_name='%s' and file_type='TABLE_FILE' and partition_name='%s'",
                DB_NAME, tableId, partitionName);
            JdbcUtil.executeSuccess(tddlConnection, sql);

            sql = String.format(CHECK_SNAPSHOT, TABLE_1);
            boolean partitionMatch = false;
            try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
                while (rs.next()) {
                    if (partitionName.equalsIgnoreCase(rs.getString("Partition_Name"))) {
                        partitionMatch = true;
                        if ("Found inconsistent snapshot".equalsIgnoreCase(rs.getString("Result"))) {
                            Assert.assertEquals("RELOAD COLUMNARMANAGER SNAPSHOT", rs.getString("Advice"));
                            String diff = rs.getString("Diff");
                            Assert.assertTrue(diff.contains("expected but not present orc: [fake_"));
                            Assert.assertTrue(diff.contains("present but not expected orc: [" + DB_NAME));
                        }
                    }
                }
            }
            Assert.assertTrue(partitionMatch);

            // restore
            sql = String.format(
                "update metadb.files set file_name = SUBSTRING(file_name, 6) where logical_schema_name='%s' and logical_table_name='%s' and file_type='TABLE_FILE' and partition_name='%s'",
                DB_NAME, tableId, partitionName);
            JdbcUtil.executeSuccess(tddlConnection, sql);
            JdbcUtil.executeSuccess(tddlConnection, "RELOAD COLUMNARMANAGER SNAPSHOT");
            sql = String.format(CHECK_SNAPSHOT, TABLE_1);
            JdbcUtil.executeSuccess(tddlConnection, sql);
        }
    }

}
