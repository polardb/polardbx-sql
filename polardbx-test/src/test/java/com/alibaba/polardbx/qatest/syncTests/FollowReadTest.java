package com.alibaba.polardbx.qatest.syncTests;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class FollowReadTest extends BaseTestCase {

    private static int accountIds = 100;
    private static String database = "FollowReadTest";
    private static String tableName = "transfer_test";
    private static String dropSQL = "DROP TABLE IF EXISTS %s";
    private static String createSQL1 =
        "CREATE TABLE IF NOT EXISTS %s (a int NOT NULL,b int NOT NULL, c int NOT NULL, PRIMARY KEY (`a`)) ";
    private static String createSQL2 =
        "CREATE TABLE IF NOT EXISTS %s (a int NOT NULL,b int NOT NULL, c int NOT NULL, PRIMARY KEY (`a`)) single";

    @Parameterized.Parameters(name = "{index}:single={0},read={1}, write={2}")
    public static List<String[]> prepare() {
        String[][] object = {
            //single table
            {"true", "XA", "XA", "false"},
            {"true", "XA", "TSO", "false"},
            {"true", "TSO", "TSO", "false"},

            //part table
            {"false", "XA", "XA", "false"},
            {"false", "XA", "TSO", "false"},
            {"false", "TSO", "TSO", "false"},

            //single table + partitionHint
            {"true", "TSO", "TSO", "true"},

            //part table + partitionHint
            {"false", "TSO", "TSO", "true"}
        };
        return Arrays.asList(object);
    }

    private boolean isSingleTable;
    private String readTrxPolicy;
    private String writeTrxPolicy;
    private boolean finish = false;
    private Exception exceptionReference = null;
    private int parallelism = 8;
    private int partitionCount = 0;
    private boolean enablePartitionHint = false;

    public FollowReadTest(String isSingleTable, String readTrxPolicy, String writeTrxPolicy, String enable) {
        this.isSingleTable = Boolean.valueOf(isSingleTable);
        this.readTrxPolicy = readTrxPolicy;
        this.writeTrxPolicy = writeTrxPolicy;
        this.enablePartitionHint = Boolean.valueOf(enable);
    }

    @Before
    public void before() throws SQLException {

        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.executeUpdate(connection, "set global ENABLE_FOLLOWER_READ=true");
        }

        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.createPartDatabase(connection, database);
        }
        //init data
        initData();
    }

    @After
    public void after() throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.executeUpdate(connection, "set global ENABLE_FOLLOWER_READ=false");
        }
        finish = true;
        try (Connection connection = getPolardbxConnection()) {
            JdbcUtil.dropDatabase(connection, database);
        }
    }

    @Test
    public void testFollowRead() throws Exception {
        //write
        Thread[] threads = new Thread[parallelism];
        for (int i = 0; i < parallelism; i++) {
            threads[i] = new TransferThread();
            threads[i].setName("TransferThread-" + i);
        }

        for (int i = 0; i < parallelism; i++) {
            threads[i].start();
        }

        //read
        Thread[] queryThreads = new Thread[parallelism];
        for (int i = 0; i < parallelism; i++) {
            queryThreads[i] = new QueryThread();
            queryThreads[i].setName("QueryThread-" + i);
        }
        for (int i = 0; i < parallelism; i++) {
            queryThreads[i].start();
        }

        for (int i = 0; i < parallelism; i++) {
            queryThreads[i].join();
        }
    }

    public Map<Integer, int[]> getQueryResult(Connection connection, String hint) throws SQLException {
        Map<Integer, int[]> mapRet = new HashMap<>();
        if (enablePartitionHint) {
            for (int i = 0; i < partitionCount; i++) {

                try (Statement stmt = connection.createStatement()) {
                    stmt.executeUpdate(String.format("set partition_hint = p%d", i + 1));
                }
                try (Statement stmt = connection.createStatement()) {
                    ResultSet resultSet =
                        stmt.executeQuery(
                            String.format("%s select /* %s */ * from %s", hint, Thread.currentThread().getName(),
                                tableName));

                    while (resultSet.next()) {

                        int a = resultSet.getInt(1);
                        int b = resultSet.getInt(2);
                        int c = resultSet.getInt(3);
                        int[] ret = new int[] {a, b, c};
                        mapRet.put(a, ret);
                    }
                }
            }
        } else {
            try (Statement stmt = connection.createStatement()) {
                ResultSet resultSet =
                    stmt.executeQuery(String.format("%s select * from %s", hint, tableName));

                while (resultSet.next()) {

                    int a = resultSet.getInt(1);
                    int b = resultSet.getInt(2);
                    int c = resultSet.getInt(3);
                    int[] ret = new int[] {a, b, c};
                    mapRet.put(a, ret);
                }
            }
        }

        return mapRet;
    }

    public void initData() throws SQLException {
        Connection conn = null;
        try {
            conn = getPolardbxConnection();

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(String.format(dropSQL, tableName));
            }

            try (Statement stmt = conn.createStatement()) {
                if (isSingleTable) {
                    stmt.executeUpdate(String.format(createSQL2, tableName));
                } else {
                    stmt.executeUpdate(String.format(createSQL1, tableName));
                }
            }

            try (Statement stmt = conn.createStatement()) {
                ResultSet resultSet = stmt.executeQuery(String.format("show topology from %s", tableName));
                while (resultSet.next()) {
                    partitionCount++;
                }
            }

            String insertSQL = "insert into %s (a, b, c) values (? , ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(String.format(insertSQL, tableName))) {
                for (int i = 1; i <= accountIds; i++) {
                    stmt.setInt(1, i);
                    stmt.setInt(2, 1);
                    stmt.setInt(3, 0);
                    stmt.executeUpdate();
                }
            }
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    public class QueryThread extends Thread {
        @Override
        public void run() {
            int iterNum = 1000;
            Connection conn = getPolardbxConnection();
            try {

                while (iterNum-- > 0) {
                    if (exceptionReference != null) {
                        break;
                    }
                    if (readTrxPolicy.equalsIgnoreCase("TSO")) {

                        boolean startTrxForSingle = false;
                        if (!isSingleTable || new Random().nextDouble() < 0.5) {
                            try (PreparedStatement stmt = conn.prepareStatement("start transaction read only;")) {
                                stmt.executeUpdate();
                            }
                            startTrxForSingle = true;
                        }

                        try (PreparedStatement stmt = conn.prepareStatement("set session TRANSACTION_POLICY='TSO'")) {
                            stmt.executeUpdate();
                        }
                        Map<Integer, int[]> slaveRet = getQueryResult(conn, "/*+TDDL:slave()*/");
                        Map<Integer, int[]> masterRet = getQueryResult(conn, "/*+TDDL:master()*/");
                        int sum = 0;
                        for (Map.Entry<Integer, int[]> entry : slaveRet.entrySet()) {
                            Integer key = entry.getKey();
                            int[] value = entry.getValue();
                            sum += value[1];
                            if (startTrxForSingle) {
                                int[] targetVal = masterRet.get(key);
                                if (value[1] != targetVal[1]) {
                                    throw new RuntimeException(
                                        " Inconsistent data！slave " + value[1] + " master " + targetVal[1]);
                                }
                                if (value[2] != targetVal[2]) {
                                    throw new RuntimeException(
                                        " Inconsistent data！version " + value[1] + " version " + targetVal[1]);
                                }
                            }
                        }
                        if (sum != accountIds) {
                            throw new RuntimeException(" Inconsistent data！current " + sum);
                        }
                        System.out.println("sum " + sum);

                        try (PreparedStatement stmt = conn.prepareStatement("commit")) {
                            stmt.executeUpdate();
                        }
                    } else if (readTrxPolicy.equalsIgnoreCase("XA")) {
                        if (enablePartitionHint) {
                            try (PreparedStatement stmt = conn.prepareStatement("begin")) {
                                stmt.executeUpdate();
                            }
                        }
                        try (PreparedStatement stmt = conn.prepareStatement("set session TRANSACTION_POLICY='XA'")) {
                            stmt.executeUpdate();
                        }
                        Map<Integer, int[]> mapRet = getQueryResult(conn, "/*+TDDL:slave()*/");
                        int sum = 0;
                        for (Map.Entry<Integer, int[]> entry : mapRet.entrySet()) {
                            sum += entry.getValue()[1];
                        }
                        if (sum != accountIds) {
                            throw new RuntimeException(" Inconsistent data！current  " + sum);
                        }
                        System.out.println("sum " + sum);
                        if (enablePartitionHint) {
                            try (PreparedStatement stmt = conn.prepareStatement("commit")) {
                                stmt.executeUpdate();
                            }
                        }
                    } else {
                        throw new UnsupportedOperationException("unsupport transaction policy!");
                    }
                }
            } catch (Exception t) {
                t.printStackTrace();
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        //ignore
                    }
                    conn = null;
                }
            } finally {
                finish = true;
            }
        }
    }

    public class TransferThread extends Thread {
        @Override
        public void run() {
            Random random = new Random();
            Connection conn = null;
            try {
                conn = getPolardbxConnection();
                while (!finish) {
                    int account1 = random.nextInt(100) + 1;
                    int account2 = random.nextInt(100) + 1;
                    if (account1 != account2) {
                        String sql1 = "begin";
                        try (PreparedStatement stmt = conn.prepareStatement(sql1)) {
                            stmt.executeUpdate();
                        }
                        try (PreparedStatement stmt = conn.prepareStatement(
                            String.format("set session TRANSACTION_POLICY='%s'", writeTrxPolicy))) {
                            stmt.executeUpdate();
                        }

                        String sql2 =
                            String.format("select * from %s where a in (%d, %d) for update", tableName, account1,
                                account2);
                        try (PreparedStatement stmt = conn.prepareStatement(sql2)) {
                            stmt.executeQuery();
                        }
                        String sql3 =
                            String.format("update %s set b=b-1,c=c+1 where a=%d", tableName, account1);
                        try (PreparedStatement stmt = conn.prepareStatement(sql3)) {
                            stmt.executeUpdate();
                        }
                        String sql4 =
                            String.format("update %s set b=b+1,c=c+1 where a=%d", tableName, account2);
                        try (PreparedStatement stmt = conn.prepareStatement(sql4)) {
                            stmt.executeUpdate();
                        }
                        String sql5 = "commit";
                        try (PreparedStatement stmt = conn.prepareStatement(sql5)) {
                            stmt.executeUpdate();
                        }

                    }
                }
            } catch (Exception t) {
                t.printStackTrace();
                if (exceptionReference == null) {
                    exceptionReference = t;
                }
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (Exception e) {
                        //ignore
                    }
                }
            }
        }
    }
}
