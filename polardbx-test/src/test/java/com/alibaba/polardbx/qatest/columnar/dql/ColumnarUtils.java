package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ColumnarUtils {
    private static final String SHOW_COLUMNAR_OFFSET = "show columnar offset";

    private static final int RETRY_COUNT = 100;

    private static final int RETRY_INTERVAL = 500;

    public static void createColumnarIndex(Connection tddlConnection, String indexName, String tableName,
                                           String sortKey, String partKey, int partCount) {
        final String createColumnarIdx = "create clustered columnar index %s on %s(%s) "
            + " engine='EXTERNAL_DISK' partition by hash(%s) partitions %s";
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(createColumnarIdx, indexName, tableName, sortKey, partKey, partCount));
    }

    public static void createColumnarIndexWithDictionary(Connection tddlConnection, String indexName, String tableName,
                                                         String sortKey, String partKey, int partCount,
                                                         String dicColumns) {
        Assert.assertTrue(dicColumns != null);
        final String createColumnarIdx = "create clustered columnar index %s on %s(%s) "
            + " engine='EXTERNAL_DISK' partition by hash(%s) partitions %s dictionary_columns = '%s'";
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(createColumnarIdx, indexName, tableName, sortKey, partKey, partCount, dicColumns));
    }

    public static void dropColumnarIndex(Connection tddlConnection, String indexName, String tableName) {
        final String dropColumnarIndex = "alter table %s drop index %s";
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(dropColumnarIndex, tableName, indexName));
    }

    public static String getExplainResult(Connection tddlConnection, String sql) {
        waitColumnarOffset(tddlConnection);
        return JdbcUtil.getExplainResult(tddlConnection, sql);
    }

    public static ResultSet executeQuery(String sql, Connection tddlConnection) {
        waitColumnarOffset(tddlConnection);
        return JdbcUtil.executeQuery(sql, tddlConnection);
    }

    public static void waitColumnarOffset(Connection tddlConnection) {
        Pair<Long, Long> offsets = getInnodbAndColumnarOffset(tddlConnection);
        long innodbOffset = offsets.getKey(), columnarOffset = offsets.getValue();

        for (int retry = 0; retry <= RETRY_COUNT && columnarOffset < innodbOffset; retry++) {
            try {
                Thread.sleep(RETRY_INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            columnarOffset = getColumnarOffset(tddlConnection);
        }

        if (columnarOffset < innodbOffset) {
            throw new RuntimeException(
                String.format(
                    "wait columnar offset failed, retry time is %s, retry interval is %s, innodb offset is %s, columnar offset is %s",
                    RETRY_COUNT, RETRY_INTERVAL, innodbOffset, columnarOffset));
        }
    }

    public static long getColumnarOffset(Connection tddlConnection) {
        try (ResultSet rs = JdbcUtil.executeQuery(SHOW_COLUMNAR_OFFSET, tddlConnection)) {
            while (rs.next()) {
                String type = rs.getString("type");
                long position = rs.getLong("position");
                if (type.equalsIgnoreCase("cn_min_latency")) {
                    return position;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("get columnar offset failed");
    }

    public static long getColumnarTso(Connection tddlConnection) {
        try (ResultSet rs = JdbcUtil.executeQuery(SHOW_COLUMNAR_OFFSET, tddlConnection)) {
            while (rs.next()) {
                String type = rs.getString("type");
                long tso = rs.getLong("tso");
                if (type.equalsIgnoreCase("COLUMNAR_LATENCY")) {
                    return tso;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        throw new RuntimeException("get columnar offset failed");
    }

    static public long columnarFlushAndGetTso(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("call polardbx.columnar_flush()");
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            return -1;
        }
    }

    static public long columnarFlushAndGetTso(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            return columnarFlushAndGetTso(statement);
        }
    }

    public static Pair<Long, Long> getInnodbAndColumnarOffset(Connection tddlConnection) {
        long innodbOffset = 0, columnarOffset = -1;
        try (ResultSet rs = JdbcUtil.executeQuery(SHOW_COLUMNAR_OFFSET, tddlConnection)) {
            while (rs.next()) {
                String type = rs.getString("type");
                long position = rs.getLong("position");
                if (type.equalsIgnoreCase("cdc")) {
                    innodbOffset = position;
                } else if (type.equalsIgnoreCase("cn_min_latency")) {
                    columnarOffset = position;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Pair.of(innodbOffset, columnarOffset);
    }
}
