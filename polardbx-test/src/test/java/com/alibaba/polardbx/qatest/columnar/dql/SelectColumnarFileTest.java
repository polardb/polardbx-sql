package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SelectColumnarFileTest extends ColumnarReadBaseTestCase {

    private static final String TABLE_1 = "select_columnar_file_test";
    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";
    private static final String DROP_TABLE = "drop table if exists %s";
    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";
    private static final String DELETE_DATA = "delete from %s where c1 = %s";
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
            for (int i = 0; i < 100; ++i) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(DELETE_DATA, TABLE_1, i));
            }
            ColumnarUtils.waitColumnarOffset(tddlConnection);
        }
    }

    @After
    public void dropTable() throws SQLException {
        try (Connection tddlConnection = getColumnarConnection()) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_1));
        }
    }

    @Test
    public void test() throws SQLException {
        String partitionName = "p1";
        String sql;
        String tableId = null;

        String csvFileName = null;
        String orcFileName = null;
        String delFileName = null;

        try (Connection tddlConnection = getColumnarConnection()) {
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

            JdbcUtil.executeSuccess(tddlConnection, "RELOAD COLUMNARMANAGER");
            JdbcUtil.executeSuccess(tddlConnection, "RELOAD COLUMNARMANAGER CACHE");
            JdbcUtil.executeSuccess(tddlConnection, "RELOAD COLUMNARMANAGER SNAPSHOT");
            JdbcUtil.executeSuccess(tddlConnection, "RELOAD COLUMNARMANAGER SCHEMA");

            Assert.assertNotNull(tableId);
            sql = String.format(
                "select file_name from metadb.files where logical_schema_name='%s' and logical_table_name='%s' and file_type='TABLE_FILE' and partition_name='%s'",
                DB_NAME, tableId, partitionName);

            try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
                while (rs.next()) {
                    String fileName = rs.getString("file_name");
                    if (fileName.endsWith(".csv")) {
                        csvFileName = fileName;
                    } else if (fileName.endsWith(".orc")) {
                        orcFileName = fileName;
                    } else if (fileName.endsWith(".del")) {
                        delFileName = fileName;
                    }
                }
            }

            Assert.assertNotNull(csvFileName);
            Assert.assertNotNull(orcFileName);
            Assert.assertNotNull(delFileName);

            // TODO(siyun): select orc file content
            JdbcUtil.executeSuccess(tddlConnection, String.format("select columnar_file('%s')", orcFileName));

            int lineCount = 0;
            try (ResultSet rs =
                JdbcUtil.executeQuery(String.format("select columnar_file('%s')", csvFileName), tddlConnection)) {
                while (rs.next()) {
                    int position = Integer.parseInt(rs.getString(2));
                    Assert.assertEquals(lineCount++, position);
                    Assert.assertEquals(rs.getString(3), rs.getString(4));
                }
            }
            Assert.assertTrue(lineCount > 0);

            lineCount = 0;
            try (ResultSet rs =
                JdbcUtil.executeQuery(String.format("select columnar_file('%s')", delFileName), tddlConnection)) {
                while (rs.next()) {
                    String bitmapString = rs.getString(3);
                    for (String deletePos : bitmapString.substring(1, bitmapString.length() - 1).split(",")) {
                        Assert.assertEquals(lineCount++, Integer.parseInt(deletePos));
                    }
                }
            }
            Assert.assertTrue(lineCount > 0);
        }
    }

}
