package com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils;

import com.alibaba.polardbx.qatest.ConnectionWrap;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.UniformDistributionDataGenerator;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataCheckUtil.checkData;
import static com.alibaba.polardbx.qatest.util.JdbcUtil.useDb;

public class DataManipulateUtil {

    final static Log log = LogFactory.getLog(DdlStateCheckUtil.class);

    public enum TABLE_TYPE {
        SECONDARY_PARTITION_TABLE,
        PARTITION_TABLE,
        SINGLE_TABLE,
        BROADCAST_TABLE
    }

    public static synchronized Connection getPolardbxConnection(String db) {
        try {
            Connection connection = ConnectionManager.getInstance().getDruidPolardbxConnection();
            ConnectionWrap connectionWrap = new ConnectionWrap(connection);
            useDb(connectionWrap, db);
//            setSqlMode(ConnectionManager.getInstance().getPolardbxMode(), connectionWrap);
            return connectionWrap;
        } catch (SQLException t) {
            log.error("get PolardbxConnection error!", t);
            throw new RuntimeException(t);
        }
    }

    public static void batchInsert(String dbName, String tableName, int tableRows, int threadNum)
        throws InterruptedException, SQLException {
        Long start = new Date().getTime();
        List<Thread> threads = new ArrayList<>();
        List<Connection> connections = new ArrayList<>();
        Boolean fastMode = true;
        for (int i = 0; i < threadNum; i++) {
            Connection tddlConnection = getPolardbxConnection(dbName);
            connections.add(tddlConnection);
            Thread thread = new Thread(() -> {
                DataLoader dataLoader =
                    DataLoader.create(tddlConnection, tableName, new UniformDistributionDataGenerator());
                dataLoader.batchInsert(tableRows, fastMode);
            });
            threads.add(thread);
            thread.start();
        }
//        int splitPartitionSize = (int) (initialTableDetail.dataLength / 8);

        for (int i = 0; i < threads.size(); i++) {
            threads.get(i).join();
        }
        for (Connection connection : connections) {
            connection.close();
        }
        Long end = new Date().getTime();
        log.info("data loader cost " + (end - start) + " ms");
    }

    public static void prepareData(Connection tddlConnection, String schemaName, String mytable) throws SQLException {
        prepareData(tddlConnection, schemaName, mytable, 2_000_000, TABLE_TYPE.PARTITION_TABLE);
    }

    public static void prepareData(Connection tddlConnection, String schemaName, String mytable, int eachPartRows)
        throws SQLException {
        prepareData(tddlConnection, schemaName, mytable, eachPartRows, TABLE_TYPE.PARTITION_TABLE);
    }

    public static void prepareData(Connection tddlConnection, String schemaName, String mytable, int eachPartRows,
                                   TABLE_TYPE tableType)
        throws SQLException {
        int partNum = 16;
        log.info("start to prepare data...");
        Long now = System.currentTimeMillis();
        String sql = String.format("create database if not exists %s mode = auto", schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("use %s", schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        sql = String.format(createTableStmt, mytable, partNum);
        if (tableType == TABLE_TYPE.SECONDARY_PARTITION_TABLE) {
            int subPartNum = 4;
            int primaryPartNum = partNum / subPartNum;
            createTableStmt =
                "create table if not exists "
                    + " %s(a int NOT NULL AUTO_INCREMENT,b int,z int, c varchar(32), PRIMARY KEY(a)"
                    + ") PARTITION BY HASH(a) PARTITIONS %d SUBPARTITION BY HASH(z) SUBPARTITIONS %d";
            sql = String.format(createTableStmt, mytable, primaryPartNum, subPartNum);
        } else if (tableType == TABLE_TYPE.SINGLE_TABLE) {
            partNum = 1;
            createTableStmt =
                "create table if not exists "
                    + " %s(a int NOT NULL AUTO_INCREMENT,b int,z int, c varchar(32), PRIMARY KEY(a)"
                    + ") single";
            sql = String.format(createTableStmt, mytable);
        }
        String setTableGroupStmt = "alter table `%s` set tablegroup = ''";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(setTableGroupStmt, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        int totalRows = partNum * eachPartRows;
        String deleteMoreThanSql = "delete from %s where a > %d";
        sql = String.format(deleteMoreThanSql, mytable, totalRows);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("select count(1) from %s", mytable);
        int currRows =
            Integer.valueOf(JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection)).get(0).get(0).toString());
        int residueRows = totalRows - currRows;
        int threadNum = 8;
        int tableRows = residueRows / threadNum;
        if (residueRows > 0) {
            try {
                batchInsert(schemaName, mytable, tableRows, threadNum);
            } catch (Exception e) {
                log.info(e.getMessage());
                throw new RuntimeException("fail to insert data into logical table");
            }
        }
        Long cost = System.currentTimeMillis() - now;
        log.info(String.format("finish prepare data in %d ms ", cost));
    }

    public static void prepareDataForDrds(Connection tddlConnection, String schemaName, String mytable)
        throws SQLException {
        Long now = System.currentTimeMillis();
        int partNum = 8;
        int groupNum = 16;
        String sql = String.format("create database if not exists %s mode = drds", schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("use %s", schemaName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") DBPARTITION BY HASH(a) TBPARTITION BY HASH(a) TBPARTITIONS %d";
        sql = String.format(createTableStmt, mytable, partNum);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("select count(1) from %s", mytable);
        int currRows =
            Integer.valueOf(JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection)).get(0).get(0).toString());
        int totalRows = partNum * 500_000 * groupNum;
        int residueRows = totalRows - currRows;
        int threadNum = 8;
        int tableRows = residueRows / threadNum;
        if (residueRows > 0) {
            try {
                batchInsert(schemaName, mytable, tableRows, threadNum);
            } catch (Exception e) {
                log.info(e.getMessage());
                throw new RuntimeException("fail to insert data into logical table");
            }
        }
        Long cost = System.currentTimeMillis() - now;
        log.info(String.format("finish prepare data in %d ms ", cost));
    }

    public static void randomOperationOnTable(Connection conn, String tableName, ChangeRowsState changeRowsState,
                                              int threadId,
                                              AtomicInteger stopFlag)
        throws SQLException {
        // we will only care about primary key values between [startPoint, endPoint)
        // if success:
        //    update startPoint and endPoint, change values in rows.
        // if failed:
        //    update startPoint and endPoint, change values in rows back.
        // if primarySet too large:
        //    update primary Set as subList of primarySet,
        //    filter all the value in rows.
        int insertTotalCount = 128;
        int updateTotalCount = 32;
        int deleteTotalCount = 32;
        Map<Integer, Integer> updateSet = new HashMap<>();
        int round = 0;
        log.info(String.format("thread %d start to operate on table!", threadId));
        while (stopFlag.get() != 1) {
            int insertCount = 0;
            int updateCount = 0;
            int deleteCount = 0;
            String insertSql = "INSERT INTO " + tableName + " (a, b) VALUES (?, ?)";
            String updateSql = "UPDATE " + tableName + " SET b = ? WHERE a = ?";
            String deleteSql = "DELETE FROM " + tableName + " WHERE a in (%s) ";
            List<Exception> exceptions = new ArrayList<>();
            PreparedStatement insertStmt = null, updateStmt = null, deleteStmt = null;
            try {
                conn.setAutoCommit(false);
                insertStmt = conn.prepareStatement(insertSql);
                for (insertCount = 0; insertCount < insertTotalCount; insertCount++) {
                    int key = changeRowsState.nextVal;
                    int value = changeRowsState.nextRand();
                    insertStmt.setInt(1, key);
                    insertStmt.setInt(2, value); // 生成新的主键值，这个方法需要确保不会产生重复的主键
                    insertStmt.addBatch();
                    changeRowsState.updateByInsert(key, value);
                }
                insertStmt.executeBatch();

                updateStmt = conn.prepareStatement(updateSql);
                List<Integer> updatePrimaryKeys = changeRowsState.fetchKeysForUpdate(updateTotalCount);
                for (updateCount = 0; updateCount < updateTotalCount; updateCount++) {
                    int key = updatePrimaryKeys.get(updateCount);
                    int value = changeRowsState.nextRand();
                    updateStmt.setInt(1, value);
                    updateStmt.setInt(2, key);
                    updateStmt.addBatch();
                    updateSet.put(key, changeRowsState.rows.get(key));
                    changeRowsState.updateByUpdate(key, value);
                }
                updateStmt.executeBatch();

                List<Integer> deletePrimaryKeys = changeRowsState.fetchKeysForDelete(deleteTotalCount);
                List<String> deleteKeyStrs = new ArrayList<>();
                for (deleteCount = 0; deleteCount < deleteTotalCount; deleteCount++) {
                    int key = deletePrimaryKeys.get(deleteCount);
                    deleteKeyStrs.add(String.valueOf(key));
                    changeRowsState.updateByDelete(key);
                }
                deleteSql = String.format(deleteSql, StringUtils.join(deleteKeyStrs, ","));
                deleteStmt = conn.prepareStatement(deleteSql);
                deleteStmt.execute();

                conn.commit();
            } catch (SQLException e) {
                if (conn != null) {
                    try {
                        conn.rollback();
                        System.out.println("Transaction rolled back due to error.");
                    } catch (SQLException ex) {
                        System.out.println("Error rolling back transaction: " + ex.getMessage());
                    }
                }
                changeRowsState.rollbackUpdate(insertCount, deleteCount, updateSet);
            } finally {
                try {
                    if (insertStmt != null) {
                        insertStmt.close();
                    }
                    if (updateStmt != null) {
                        updateStmt.close();
                    }
                    if (deleteStmt != null) {
                        deleteStmt.close();
                    }
                    if (conn != null) {
                        // 恢复自动提交模式
                        conn.setAutoCommit(true);
                    }
                } catch (SQLException ex) {
                    System.out.println("Error closing resources: " + ex.getMessage());
                }
            }
            changeRowsState.autoGc(threadId);
            round++;
            if (round % 50 == 0) {
                log.info(String.format("thread %d has update data round %d", threadId, round));
            }
        }
    }

    public static Map<Integer, Integer> sampleRows(Connection tddlConnection, String schemaName, String mytable) {
        String countSql = String.format(
            "select table_rows from information_schema.tables where table_name = \"%s\" and table_schema = \"%s\"",
            mytable, schemaName);
        int currRows =
            Integer.valueOf(
                JdbcUtil.getAllResult(JdbcUtil.executeQuery(countSql, tddlConnection)).get(0).get(0).toString());
        // expected 3200_0000
        int sampleRows = 500_000;
        double randValue = sampleRows / (double) currRows;
//        String sampleSql = String.format("select a, b from %s where rand() < %f", mytable, randValue);
        String sampleSql = String.format("select a, b from %s limit %d", mytable, sampleRows);
        Map<Integer, Integer> beforeRows = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sampleSql, tddlConnection)).
            stream().collect(
                Collectors.toMap(o -> Integer.valueOf(o.get(0).toString()), o -> Integer.valueOf(o.get(1).toString())));
        return beforeRows;
    }

    public static void updateTable(Connection tddlConnection, String schemaName, String mytable,
                                   Map<Integer, Integer> beforeRows,
                                   AtomicInteger finishDdlFlag, AtomicReference<List<Integer>> checkSum)
        throws SQLException, InterruptedException {
        log.info("start to collect data before update data.");
        int threadNum = 4;
        String getNextValSql = String.format("select max(a) from %s", mytable);
        int nextVal =
            Integer.valueOf(JdbcUtil.getAllResult(JdbcUtil.executeQuery(getNextValSql, tddlConnection)).get(0).get(0)
                .toString());
        List<Map<Integer, Integer>> afterRowsList = new ArrayList<>();
        List<Map<Integer, Integer>> beforeRowsList = new ArrayList<>();
        List<ChangeRowsState> changeRowsStateList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            afterRowsList.add(new HashMap<>());
            beforeRowsList.add(new HashMap<>());
        }
        int i = 0;
        for (Integer key : beforeRows.keySet()) {
            int index = i % threadNum;
            afterRowsList.get(index).put(key, beforeRows.get(key));
            beforeRowsList.get(index).put(key, beforeRows.get(key));
            i++;
        }
        int stepOnTask = 20_000_000;
        for (int j = 0; j < threadNum; j++) {
            ChangeRowsState changeRowsState = new ChangeRowsState(afterRowsList.get(j), nextVal + 1 + stepOnTask * j);
            changeRowsStateList.add(changeRowsState);
        }

        log.info("finish to collect data before update data.");
        List<Thread> threads = new ArrayList<>();
        for (int j = 0; j < threadNum; j++) {
            ChangeRowsState changeRowsState = changeRowsStateList.get(j);
            final int threadId = j;
            Thread updateTableData = new Thread(
                () -> {
                    try (Connection connection = getPolardbxConnection(schemaName)) {
                        randomOperationOnTable(connection, mytable, changeRowsState, threadId, finishDdlFlag);
                    } catch (SQLException ignored) {
                    }
                });
            updateTableData.start();
            threads.add(updateTableData);
        }
        for (int j = 0; j < threadNum; j++) {
            threads.get(j).join();
        }
        log.info("finish update data, calculate checksum");
        for (int j = 0; j < threadNum; j++) {
            ChangeRowsState changeRowsState = changeRowsStateList.get(j);
            checkSum.set(changeRowsState.updateCheckSum(beforeRowsList.get(j), checkSum.get()));
        }
        log.info("finish to calculate checksum");
        finishDdlFlag.set(2);
    }

    public static Boolean checkOnlineLogApply(Connection tddlConnection, Long jobId, String mytable,
                                              AtomicReference<List<Date>> applyFlag
    ) {
        String logInfo = String.format("there are %d moment that apply rowlog: %s", applyFlag.get().size(),
            applyFlag.get().toString());
        log.info(logInfo);
//        return !applyFlag.get().isEmpty();
        return true;
    }

    public static void recordOnlineLogApply(Connection tddlConnection, String schemaName, String mytable,
                                            AtomicInteger finishDdlFlag,
                                            AtomicReference<List<Date>> applyFlag
    ) throws InterruptedException {
        String sql = "show physical ddl";
        while (finishDdlFlag.get() != 1) {
            List<List<Object>> results = JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection));
            if (!results.isEmpty()) {
                String state = results.get(0).get(3).toString();
                if (state.equalsIgnoreCase("REACHED_BARRIER_RUNNING")) {
                    log.info("apply row log moment: " + new Date());
                    applyFlag.get().add(new Date());
                }
            }
            Thread.sleep(30L);
        }
//        Boolean compareResult = true;
//        for (int i = 0; i < checkSum.size(); i++) {
//            if (!checkSum.get(i).equals(checkSum2.get(i))) {
//                compareResult = false;
//            }
//        }
//        return compareResult;
//        return true;

    }

    public static Boolean checkTableDataForRebuildTable(Connection tddlConnection, Long jobId, String schemaName,
                                                        String mytable,
                                                        List<Integer> checkSum
    ) throws SQLException {
        List<Integer> checkSum2 = DataCheckUtil.checkData(tddlConnection, schemaName, mytable);
        Boolean compareResult = true;
        for (int i = 0; i < checkSum.size(); i++) {
            if (!checkSum.get(i).equals(checkSum2.get(i))) {
                compareResult = false;
            }
        }
        return compareResult;
    }

    public static Boolean checkTableDataForNonRebuildTable(Connection tddlConnection, Long jobId, String schemaName,
                                                           String mytable, String indexName,
                                                           List<Integer> checkSum
    ) throws SQLException {
        List<Integer> checkSum2 = DataCheckUtil.checkData(tddlConnection, schemaName, mytable, indexName);
        Boolean compareResult = true;
        for (int i = 0; i < checkSum.size(); i++) {
            if (!checkSum.get(i).equals(checkSum2.get(i))) {
                compareResult = false;
            }
        }
        return compareResult;
    }

    public static Boolean waitTillFlagSet(AtomicInteger flag, int target) throws InterruptedException {
        Boolean waitDone = false;
        int i = 0;
        while (i < 1000) {
            if (flag.get() == target) {
                waitDone = true;
                break;
            }
            Thread.sleep(1 * 1000);
            i++;
        }
        return waitDone;

    }
}
