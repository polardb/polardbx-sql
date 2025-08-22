package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.is;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */

@NotThreadSafe
public class AlterTableGroupMovePartitionFailPointInjectTest extends DDLBaseNewDBTestCase {

    final static String DB_NAME = "AlterTableGroupMovePartitionFailPointInjectTest";

    public AlterTableGroupMovePartitionFailPointInjectTest() {

    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
        }
    }

    @After
    public void after() throws SQLException {
        FailPoint.disable("FP_CATCHUP_TASK_SUSPEND");
    }

    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testPhysicalPartition() throws SQLException {
        String pattern = "CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 5\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 6\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()\n"
            + ";";
        String primaryTableName1 = "testPhysicalPartition1";
        String primaryTableName2 = "testPhysicalPartition2";
        String createTableSql = String.format(pattern, primaryTableName1);
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(createTableSql);
            stmt.executeUpdate(String.format("create table %s like %s", primaryTableName2, primaryTableName1));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @FP_CATCHUP_TASK_SUSPEND = '1000'");
        Set<String> jobIds = scheduleMovePartition(primaryTableName1);
        for (String jobId : jobIds) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.executeUpdate(String.format("pause ddl %s", jobId));
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                }

                do {
                    ResultSet rs = stmt.executeQuery("show ddl " + jobId);
                    if (rs.next()) {
                        String state = rs.getString("STATE");
                        if (state.equalsIgnoreCase("ROLLBACK_TO_READY")) {
                            try {
                                Thread.sleep(1000);
                                continue;
                            } catch (Exception e) {
                            }
                        }
                    }
                    try {
                        stmt.executeUpdate(String.format("continue ddl %s", jobId));
                        break;
                    } catch (SQLException ex) {
                        if (!ex.getMessage().contains("ROLLBACK_TO_READY")) {
                            Assert.fail(ex.getMessage());
                        }
                    }
                } while (true);
            } catch (SQLException e) {
                if (!e.getMessage().contains("The ddl job does not exist")) {
                    Assert.fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testPartitioningTable() throws SQLException {
        String pattern = "CREATE TABLE %s (\n"
            + "    c1 bigint,\n"
            + "    c2 bigint,\n"
            + "    c3 bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ")\n"
            + "PARTITION BY HASH(c1)\n"
            + "PARTITIONS 4\n"
            + ";";
        String primaryTableName1 = "testPartitioningTable1";
        String primaryTableName2 = "testPartitioningTable2";
        String createTableSql = String.format(pattern, primaryTableName1);
        try (Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(connection, DB_NAME);
            Statement stmt = connection.createStatement();
            stmt.executeUpdate(createTableSql);
            stmt.executeUpdate(String.format("create table %s like %s", primaryTableName2, primaryTableName1));
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @FP_CATCHUP_TASK_SUSPEND = '1000'");
        Set<String> jobIds = scheduleMovePartition(primaryTableName1);
        for (String jobId : jobIds) {
            try {
                Thread.sleep(2000);
            } catch (Exception e) {
            }
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.executeUpdate(String.format("pause ddl %s", jobId));
                try {
                    Thread.sleep(2000);
                } catch (Exception e) {
                }

                do {
                    ResultSet rs = stmt.executeQuery("show ddl " + jobId);
                    if (rs.next()) {
                        String state = rs.getString("STATE");
                        if (state.equalsIgnoreCase("ROLLBACK_TO_READY")) {
                            try {
                                Thread.sleep(1000);
                                continue;
                            } catch (Exception e) {
                            }
                        }
                    }
                    try {
                        stmt.executeUpdate(String.format("continue ddl %s", jobId));
                        break;
                    } catch (SQLException ex) {
                        if (!ex.getMessage().contains("ROLLBACK_TO_READY")) {
                            Assert.fail(ex.getMessage());
                        }
                    }
                } while (true);
            } catch (SQLException e) {
                if (!e.getMessage().contains("The ddl job does not exist")) {
                    Assert.fail(e.getMessage());
                }
            }
        }
    }

    private Set<String> scheduleMovePartition(String tableName) throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        Map<String, List<String>> storageAndPartitions = showTopologyByStorage(tddlConnection, tableName);
        Set<String> jobIds = new HashSet<>();
        if (storageAndPartitions.size() <= 1) {
            return jobIds;
        }
        List<String> storages = new ArrayList<>();
        List<List<String>> partitions = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : storageAndPartitions.entrySet()) {
            String storage = entry.getKey();
            storages.add(storage);
            partitions.add(entry.getValue());
        }
        String movePgSql = String.format(
            "alter tablegroup by table " + tableName + " move partitions %s to '%s' async=true",
            String.join(",", partitions.get(0)), storages.get(1));
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeUpdate(movePgSql);
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        long startTime = System.currentTimeMillis();
        List<Map<String, String>> fullDDL;
        do {
            jobIds.clear();
            fullDDL = showFullDDL();
            if (fullDDL.size() == 0) {
                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                }
            } else {
                for (Map<String, String> map : fullDDL) {
                    jobIds.add(map.get("JOB_ID"));
                }
                break;
            }
        } while (System.currentTimeMillis() - startTime < 600 * 1000);
        if (jobIds.size() == 0) {
            Assert.fail("DDL job is not scheduled after 600 seconds");
        }
        return jobIds;
    }
}
