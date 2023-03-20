package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageMovePartitionTest extends BaseTestCase {
    private static String testDataBase1 = "fileStoragePartitionTest";

    private static String testDataBase2 = "fileStoragePartitionTest2";

    private static Engine engine = PropertiesUtil.engine();

    private String innoDataBase;

    private String ossDataBase;
    String innodbTableName;
    String ossTableName;

    @Parameterized.Parameters(name = "{index}:innoDB={0}.{1},ossDB={2}.{3}")
    public static List<String[]> prepare() {
        return Arrays.asList(
            new String[][] {
                {testDataBase1, "move_partition1", testDataBase1, "oss_move_partition1"},
                {testDataBase1, "move_partition2", testDataBase2, "oss_move_partition2"}
            });
    }

    public FileStorageMovePartitionTest(String innoDataBase, String innodbTableName, String ossDataBase,
                                        String ossTableName) {
        this.innoDataBase = innoDataBase;
        this.innodbTableName = innodbTableName;
        this.ossDataBase = ossDataBase;
        this.ossTableName = ossTableName;
    }

    @BeforeClass
    static public void initTestDatabase() {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase1));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase1));
            statement.execute(String.format("drop database if exists %s ", testDataBase2));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase2));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private void checkTotalCount(Connection innoConn, Connection ossConn,
                                 long countTotal, String innodbTestTableName, String ossTestTableName)
        throws SQLException {
        assertWithMessage(ossTestTableName + " 丢数据").that(FullTypeTestUtil.count(ossConn, ossTestTableName))
            .isEqualTo(countTotal - FullTypeTestUtil.count(innoConn, innodbTestTableName));

    }

    @Test
    public void testMovePartition() {

        String rbTable = "rebalance table %s";
        String rbTableGroup = "rebalance tablegroup %s";
        String rbDatabase = "rebalance database";
        String expire = "alter table %s expire local partition %s";
        String showTp = "show topology from %s";
        int lpCount = 0;

        ResultSet rs = null;
        try (Connection innoConn = getPolardbxConnection(innoDataBase);
            Connection ossConn = getPolardbxConnection(ossDataBase)) {
            // prepare table
            FullTypeTestUtil.prepareInnoTable(innoConn, innodbTableName, 20, 20);
            FullTypeTestUtil.prepareTTLTable(ossConn, ossDataBase, ossTableName, innoDataBase, innodbTableName, engine);

            // get local partition
            List<String> localPartitions = new ArrayList<>();
            rs = JdbcUtil.executeQuery(String.format(
                "select LOCAL_PARTITION_NAME from information_schema.local_partitions where table_schema=\"%s\" and table_name=\"%s\"",
                innoDataBase, innodbTableName), innoConn);
            while (rs.next()) {
                localPartitions.add(rs.getString("LOCAL_PARTITION_NAME"));
            }
            rs.close();

            // get table group name
            String innoTg = getTgName(innoConn, innodbTableName);
            String ossTg = getTgName(ossConn, ossTableName);

            // get partition to dn map
            List<String> innoPart = new ArrayList<>();
            List<String> ossPart = new ArrayList<>();
            Set<String> dns = new TreeSet<>();
            rs = JdbcUtil.executeQuery(String.format(showTp, innodbTableName), innoConn);
            while (rs.next()) {
                String part = rs.getString("PARTITION_NAME");
                String dn = rs.getString("DN_ID");
                innoPart.add(part);
                dns.add(dn);
            }
            rs.close();
            rs = JdbcUtil.executeQuery(String.format(showTp, ossTableName), ossConn);
            while (rs.next()) {
                String part = rs.getString("PARTITION_NAME");
                String dn = rs.getString("DN_ID");
                ossPart.add(part);
                dns.add(dn);
            }
            rs.close();

            // move partition oss table without data
            movePartition(ossConn, ossTg, ossPart, dns);
            // check count
            assertWithMessage(ossTableName + " 非空").that(FullTypeTestUtil.count(ossConn, ossTableName))
                .isEqualTo(0);
            long countTotal = FullTypeTestUtil.count(innoConn, innodbTableName);

            // expire local partition
            JdbcUtil.executeUpdate(innoConn, String.format(expire, innodbTableName, localPartitions.get(lpCount++)));

            // check count
            long countBeforeMove = FullTypeTestUtil.count(ossConn, ossTableName);
            assertWithMessage(ossTableName + " 为空").that(countBeforeMove).isGreaterThan(0);
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);

            // move partition oss table with data
            movePartition(ossConn, ossTg, ossPart, dns);
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);

            // move partition innodb table
            movePartition(innoConn, innoTg, innoPart, dns);
            JdbcUtil.executeUpdate(innoConn, String.format(expire, innodbTableName, localPartitions.get(lpCount++)));
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);

            // rebalance table
            executeAndWaitFinish(ossConn, String.format(rbTable, ossTableName));
            executeAndWaitFinish(innoConn, String.format(rbTable, innodbTableName));
            JdbcUtil.executeUpdate(innoConn, String.format(expire, innodbTableName, localPartitions.get(lpCount++)));
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);

            // rebalance tableGroup
            executeAndWaitFinish(ossConn, String.format(rbTableGroup, ossTg));
            executeAndWaitFinish(innoConn, String.format(rbTableGroup, innoTg));
            JdbcUtil.executeUpdate(innoConn, String.format(expire, innodbTableName, localPartitions.get(lpCount++)));
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);

            // rebalance database
            executeAndWaitFinish(ossConn, rbDatabase);
            executeAndWaitFinish(innoConn, rbDatabase);
            JdbcUtil.executeUpdate(innoConn, String.format(expire, innodbTableName, localPartitions.get(lpCount)));
            checkTotalCount(innoConn, ossConn, countTotal, innodbTableName, ossTableName);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    private void movePartition(Connection conn, String tg, List<String> partitionList, Set<String> dnList) {
        String movePart = "alter tablegroup %s move partitions %s to \"%s\"";
        Map<String, List<String>> dnToPart = new TreeMap<>();
        dnList.forEach(x -> dnToPart.put(x, new ArrayList<>()));
        List<String> dns = new ArrayList<>(dnList);
        for (int i = 0; i < partitionList.size(); i++) {
            dnToPart.get(dns.get(i % dns.size())).add(partitionList.get(i));
        }
        for (Map.Entry<String, List<String>> entry : dnToPart.entrySet()) {
            JdbcUtil.executeUpdate(conn,
                String.format(movePart, tg, String.join(",", entry.getValue()), entry.getKey()));
        }
    }

    private void executeAndWaitFinish(Connection conn, String sql) throws SQLException, InterruptedException {
        String jobId;
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            rs.next();
            jobId = rs.getString("job_id");
        }
        long[] ttl_pause = new long[] {1, 5, 10, 20};
        for (long pause : ttl_pause) {
            // sleep to wait for the schedule job finish
            Thread.sleep(pause * 1000L);
            ResultSet rs = JdbcUtil.executeQuery("show ddl " + jobId, conn);
            if (!rs.next()) {
                // ddl finished
                rs.close();
                return;
            }
            rs.close();
        }
        Assert.fail(sql + " takes too long!");
    }

    private String getTgName(Connection conn, String table) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(
            String.format("show full tablegroup where tables like \"%%%s%%\"", table), conn)) {
            rs.next();
            return rs.getString("TABLE_GROUP_NAME");
        }
    }
}
