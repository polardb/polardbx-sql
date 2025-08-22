package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.is;

public class ExplainAlterTableGroupMovePartitionTest extends DDLBaseNewDBTestCase {
    public static String DB_NAME = "test_explain_move_partition";
    private static final String ENABLE_PHYSICAL_BACKFILL_HINT = "/*+TDDL:CMD_EXTRA(physical_backfill_enable=TRUE)*/";
    private static final String TABLE_NAME1 = "TABLE_NAME1_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME2 = "TABLE_NAME2_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME3 = "TABLE_NAME3_" + RandomUtils.getStringBetween(4, 6);
    private static final String TABLE_NAME4 = "TABLE_NAME4_" + RandomUtils.getStringBetween(4, 6);

    final static String KEY_KEY_PART =
        "partition by key(id) partitions 3 subpartition by key(bigint_col) subpartitions 1;";

    @BeforeClass
    public static void setUpClass() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            createTable();
            insertData(100_000);
        }
    }

    @Test
    public void testMovePartitionGroup() {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        Map<String, List<String>> storageAndPartitions = showTopologyByStorage(tddlConnection, TABLE_NAME1);
        if (storageAndPartitions.size() <= 1) {
            return;
        }
        List<String> storages = new ArrayList<>();
        List<List<String>> partitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
            String storage = entry.getKey();
            storages.add(storage);
            partitions.add(entry.getValue());
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            for (int i = 0; i < storages.size(); i++) {
                int j = ((i == storages.size() - 1) ? 0 : i + 1);
                String movePgSql =
                    String.format(
                        "explain %s alter tablegroup by table " + TABLE_NAME1
                            + " move subpartitions %s to '%s'",
                        ENABLE_PHYSICAL_BACKFILL_HINT, String.join(",", partitions.get(i)), storages.get(j));
                System.out.println(movePgSql);
                ResultSet resultSet = stmt.executeQuery(movePgSql);
                List<String> logicalBackfillTaskInfos = new ArrayList<>();
                List<String> physicalBackfillTaskInfos = new ArrayList<>();
                List<String> allTaskInfos = new ArrayList<>();
                while (resultSet.next()) {
                    String taskInfo = resultSet.getString(1);
                    if (taskInfo.toLowerCase().indexOf("LOGICAL_BACKFILL".toLowerCase()) != -1) {
                        logicalBackfillTaskInfos.add(taskInfo);
                    } else if (taskInfo.toLowerCase().indexOf("PHYSICAL_BACKFILL".toLowerCase()) != -1) {
                        physicalBackfillTaskInfos.add(taskInfo);
                    }
                    allTaskInfos.add(taskInfo);
                }

                Assert.assertThat(allTaskInfos.toString(), physicalBackfillTaskInfos.size(),
                    is(partitions.get(i).size()));
                Assert.assertThat(allTaskInfos.toString(), logicalBackfillTaskInfos.size(),
                    is(3));
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        try {
            JdbcUtil.useDb(tddlConnection, DB_NAME);
            JdbcUtil.executeUpdate(tddlConnection,"rebalance database explain=true");
        } catch (Exception exception) {
            //pass
        }
    }

    private static void createTable() throws SQLException {
        String sql = "CREATE TABLE " + TABLE_NAME1 + " (" +
            "id INT AUTO_INCREMENT PRIMARY KEY," +
            "tinyint_col TINYINT," +
            "smallint_col SMALLINT," +
            "mediumint_col MEDIUMINT," +
            "int_col INT," +
            "bigint_col BIGINT," +
            "decimal_col DECIMAL(10, 2)," +
            "float_col FLOAT," +
            "double_col DOUBLE," +
            "bit_col BIT(8)," +
            "char_col CHAR(10)," +
            "varchar_col VARCHAR(255)," +
            "binary_col BINARY(10)," +
            "varbinary_col VARBINARY(255)," +
            "tinyblob_col TINYBLOB," +
            "blob_col BLOB," +
            "mediumblob_col MEDIUMBLOB," +
            "longblob_col LONGBLOB," +
            "date_col DATE," +
            "datetime_col DATETIME," +
            "timestamp_col TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
            "time_col TIME," +
            "year_col YEAR," +
            "enum_col ENUM('A', 'B', 'C')," +
            "set_col SET('X', 'Y', 'Z')) ";

        sql = sql + KEY_KEY_PART;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME2, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME3, TABLE_NAME1));
            stmt.executeUpdate(String.format("create table %s like %s", TABLE_NAME4, TABLE_NAME1));
        }
    }

    private static void insertData(int count) {
        String sql = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        int log2N = (int) Math.ceil(Math.log(count) / Math.log(2));
        String insertSelect = "INSERT INTO " + TABLE_NAME1
            + "(tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, "
            + "float_col, double_col, bit_col, char_col, varchar_col, binary_col, varbinary_col, date_col, datetime_col, "
            + "timestamp_col, time_col, year_col, enum_col, set_col) "
            + "SELECT tinyint_col, smallint_col, mediumint_col, int_col, bigint_col, decimal_col, float_col, double_col, bit_col, char_col,"
            + " varchar_col, binary_col, varbinary_col, date_col, datetime_col, timestamp_col, time_col, year_col, enum_col, set_col FROM "
            + TABLE_NAME1;
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            try {
                JdbcUtil.useDb(connection, DB_NAME);
                PreparedStatement pstmt = connection.prepareStatement(sql);
                pstmt.setByte(1, (byte) (Math.random() * 256));
                pstmt.setShort(2, (short) (Math.random() * Short.MAX_VALUE));
                pstmt.setInt(3, ((int) (Math.random() * Integer.MAX_VALUE)) & 0x007FFFFF);
                pstmt.setInt(4, (int) (Math.random() * Integer.MAX_VALUE));
                pstmt.setLong(5, Math.abs(new Random().nextLong()));
                pstmt.setBigDecimal(6, BigDecimal.valueOf(RandomUtils.getIntegerBetween(1, 199) * 1000, 2));
                pstmt.setFloat(7, (float) (Math.random() * 1000));
                pstmt.setDouble(8, Math.random() * 1000);
                pstmt.setByte(9, (byte) (RandomUtils.getIntegerBetween(0, 127)));
                pstmt.setString(10, "char" + (int) (Math.random() * 10));
                pstmt.setString(11, "varchar" + (int) (Math.random() * 100));
                pstmt.setBytes(12, ("binary" + (int) (Math.random() * 10)).getBytes());
                pstmt.setBytes(13, ("varbinary" + (int) (Math.random() * 100)).getBytes());
                pstmt.setDate(14, Date.valueOf(LocalDate.now()));
                pstmt.setTimestamp(15, Timestamp.valueOf(LocalDateTime.now()));
                pstmt.setTimestamp(16, Timestamp.valueOf(LocalDateTime.now()));
                pstmt.setTime(17, Time.valueOf(LocalTime.now()));
                pstmt.setInt(18, Year.now().getValue());
                pstmt.setString(19, "A");
                pstmt.setString(20, "X,Y");

                pstmt.executeUpdate();
                do {
                    JdbcUtil.executeUpdateSuccess(connection, insertSelect);
                    log2N--;
                } while (log2N > 0);
            } catch (Exception exception) {
                exception.printStackTrace();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Map<String, List<String>> showTopologyByStorage(Connection conn, String tbName) {
        Map<String, List<String>> storageAndPartitions = new HashMap<>();
        String sql = "show topology " + tbName;

        ResultSet rs = JdbcUtil.executeQuerySuccess(conn, sql);
        try {
            while (rs.next()) {
                String partitionName = rs.getString("PARTITION_NAME");
                String subpartitionName = rs.getString("SUBPARTITION_NAME");
                String storageId = rs.getString("DN_ID");
                if (StringUtils.isEmpty(subpartitionName)) {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new ArrayList<>()).add(partitionName);
                } else {
                    storageAndPartitions.computeIfAbsent(storageId, k -> new ArrayList<>()).add(subpartitionName);
                }
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        } finally {
            JdbcUtil.close(rs);
        }
        return storageAndPartitions;
    }
}
