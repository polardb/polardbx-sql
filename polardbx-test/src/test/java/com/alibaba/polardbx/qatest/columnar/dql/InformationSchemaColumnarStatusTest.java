package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.alibaba.polardbx.qatest.columnar.dql.FullTypeTest.waitForSync;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXAutoDBName1;

public class InformationSchemaColumnarStatusTest extends ReadBaseTestCase {
    private static final String TABLE_1 = "test_columnar_status_1";
    private static final String TABLE_2 = "test_columnar_status_2";
    private static final String TABLE_3 = "test_columnar_status_3";
    private static final String TABLE_4 = "test_columnar_status_4";

    private static final String[] TABLES = {
        TABLE_1, TABLE_2, TABLE_3, TABLE_4
    };

    private static final String CREATE_TABLE = "CREATE TABLE %s (id int primary key, k int) partition by hash(id)";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String INSERT_DATA = "insert into %s values (%d, %d)";
    private static final String DELETE_DATA = "delete from %s where 1=1 limit %d";

    private static final String INFORMATION_SCHEMA_SQL = "select * from information_schema.columnar_status "
        + "where table_schema = '%s' and table_name = '%s'";

    private static final int COUNT = 10000;
    private static final int INSERT_COUNT = 909;
    private static final int DELETE_COUNT = 1010;
    private static final int PART_COUNT = 4;

    @BeforeClass
    public static void prepare() throws Exception {
        dropTable();
        prepareData();
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());
            for (String table : TABLES) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, table));
            }
        }
    }

    private static void prepareData() throws Exception {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXAutoDBName1());

            // All four columnar index have orc
            for (String table : TABLES) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, table));
                for (int i = 0; i < COUNT; i++) {
                    JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, table, i, i + 1));
                }
                JdbcUtil.createColumnarIndex(tddlConnection, "col_" + table, table, "id", "id", PART_COUNT);
            }

            // TABLE_2: orc + csv
            for (int i = 0; i < INSERT_COUNT; i++) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_2, i + COUNT, i + COUNT + 1));
            }

            // TABLE_3: orc + del
            JdbcUtil.executeSuccess(tddlConnection, String.format(DELETE_DATA, TABLE_3, DELETE_COUNT));

            // TABLE_4: orc + csv + del
            for (int i = 0; i < INSERT_COUNT; i++) {
                JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_4, i + COUNT, i + COUNT + 1));
            }
            JdbcUtil.executeSuccess(tddlConnection, String.format(DELETE_DATA, TABLE_4, DELETE_COUNT));

            waitForSync(tddlConnection);
        }
    }

    @Test
    public void testAllOrc() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(INFORMATION_SCHEMA_SQL, polardbXAutoDBName1(), TABLE_1))) {
            checkSingleTable(rs, TABLE_1, 0, 0, 0, 0);
        }
    }

    @Test
    public void testOrcCsv() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(INFORMATION_SCHEMA_SQL, polardbXAutoDBName1(), TABLE_2))) {
            checkSingleTable(rs, TABLE_2, PART_COUNT, INSERT_COUNT, 0, 0);
        }
    }

    @Test
    public void testOrcDel() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(INFORMATION_SCHEMA_SQL, polardbXAutoDBName1(), TABLE_3))) {
            checkSingleTable(rs, TABLE_3, 0, 0, PART_COUNT, DELETE_COUNT);
        }
    }

    @Test
    public void testOrcCsvDel() throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(INFORMATION_SCHEMA_SQL, polardbXAutoDBName1(), TABLE_4))) {
            checkSingleTable(rs, TABLE_4, PART_COUNT, INSERT_COUNT, PART_COUNT, DELETE_COUNT);
        }
    }

    private void checkSingleTable(ResultSet rs, String tableName, long csvFileCount, long csvRowCount,
                                  long delFileCount, long delRowCount) throws SQLException {
        Assert.assertTrue(rs.next());

        Assert.assertEquals(polardbXAutoDBName1(), rs.getString("TABLE_SCHEMA"));
        Assert.assertEquals(tableName, rs.getString("TABLE_NAME"));
        Assert.assertEquals("PUBLIC", rs.getString("STATUS"));
        Assert.assertEquals(PART_COUNT, rs.getLong("PARTITION_NUM"));
        Assert.assertEquals(PART_COUNT, rs.getLong("ORC_FILE_COUNT"));
        Assert.assertEquals(COUNT, rs.getLong("ORC_ROW_COUNT"));
        Assert.assertEquals(csvFileCount, rs.getLong("CSV_FILE_COUNT"));
        if (delFileCount == 0) {
            Assert.assertEquals(csvRowCount, rs.getLong("CSV_ROW_COUNT"));
        }
        Assert.assertEquals(delFileCount, rs.getLong("DEL_FILE_COUNT"));
        if (csvFileCount == 0) {
            Assert.assertEquals(delRowCount, rs.getLong("DEL_ROW_COUNT"));
        }

        // for orc + csv + del, we count total rows
        if (csvFileCount > 0 && delFileCount > 0) {
            Assert.assertEquals(
                COUNT + csvRowCount - delRowCount,
                rs.getLong("ROW_COUNT")
            );
        }

        // Must return exactly one row
        Assert.assertFalse(rs.next());
    }
}
