package com.alibaba.polardbx.qatest.ddl.auto.instantrepartition;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class PartitionTestUtils extends BaseTestCase {

    private static final String CREATE_TABLENAME = "CREATE TABLE %s (\n" +
            "    `a1` bigint NOT NULL AUTO_INCREMENT,\n" +
            "    `a2` bigint NOT NULL,\n" +
            "    `a3` int NOT NULL DEFAULT '0',\n" +
            "    `a4` bigint DEFAULT '0',\n" +
            "    `a5` tinyint NOT NULL,\n" +
            "    `a6` int NOT NULL DEFAULT '0',\n" +
            "    `a7` bigint NOT NULL,\n" +
            "    `a8` tinyint NOT NULL,\n" +
            "    `a9` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a10` tinyint NOT NULL,\n" +
            "    `a11` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a12` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a13` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a14` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a15` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a16` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a17` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a18` decimal(16, 8) NOT NULL DEFAULT '0.00000000',\n" +
            "    `a19` tinyint NOT NULL,\n" +
            "    `a20` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a21` tinyint(1) NOT NULL,\n" +
            "    `a22` tinyint NOT NULL,\n" +
            "    `a23` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a24` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a25` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a26` tinyint NOT NULL,\n" +
            "    `a27` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a28` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a29` varchar(64) DEFAULT NULL,\n" +
            "    `a30` varchar(32) DEFAULT NULL,\n" +
            "    `a31` bigint NOT NULL DEFAULT '0',\n" +
            "    `a32` int DEFAULT NULL,\n" +
            "    `a33` bigint DEFAULT '0',\n" +
            "    `a34` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    `a35` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    `a36` bigint DEFAULT '0',\n" +
            "    `a37` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a38` decimal(16, 4) NOT NULL DEFAULT '1.0000',\n" +
            "    `a39` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a40` decimal(16, 8) NOT NULL DEFAULT '0.00000000',\n" +
            "    `a41` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a42` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a43` varchar(64) DEFAULT NULL,\n" +
            "    `a44` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a45` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    `a46` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a47` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    `a48` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    `a49` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a50` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a51` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a52` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a53` varchar(128) DEFAULT '',\n" +
            "    `a54` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a55` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a56` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a57` bigint DEFAULT '0',\n" +
            "    `a58` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a59` bigint DEFAULT '0',\n" +
            "    `a60` varchar(64) DEFAULT NULL,\n" +
            "    `a61` varchar(64) DEFAULT NULL,\n" +
            "    `a62` int NOT NULL DEFAULT '0',\n" +
            "    `a63` tinyint NOT NULL DEFAULT '0',\n" +
            "    `a64` varchar(64) DEFAULT NULL,\n" +
            "    `a65` decimal(16, 8) NOT NULL DEFAULT '0.00000000',\n" +
            "    `a66` decimal(32, 16) NOT NULL DEFAULT '0.0000000000000000',\n" +
            "    `a67` text,\n" +
            "    `a68` bigint NOT NULL DEFAULT '0',\n" +
            "    `a69` int NOT NULL DEFAULT '0',\n" +
            "    `a70` json DEFAULT NULL,\n" +
            "    `a71` json DEFAULT NULL,\n" +
            "    `a72` int DEFAULT NULL,\n" +
            "    `a73` datetime(3) NOT NULL,\n" +
            "    `a74` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
            "    `a75` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),\n" +
            "    `a76` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
            "    `a77` timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),\n" +
            "    `a78` decimal(32, 16) DEFAULT '0.0000000000000000',\n" +
            "    PRIMARY KEY (`a1`),\n" +
            "    KEY `indx2` (`a2`, `a77`, `a3`, `a7`, `a6`, `a8`, `a10`, `a20`, `a29`, `a76`),\n" +
            "    KEY `indx3` (`a2`, `a6`, `a1`, `a3`, `a7`, `a8`, `a10`, `a20`, `a29`, `a76`, `a77`),\n" +
            "    KEY `idx_modified_at` (`a77`),\n" +
            "    KEY `indx1` (`a2`, `a1`, `a3`, `a7`, `a6`, `a8`, `a10`, `a20`, `a29`, `a26`, `a76`, `a77`, `a73`)\n" +
            ") ENGINE=InnoDB  %s";

    private static final String TAR_TB_PART = "PARTITION BY KEY(`a2`)\n" +
            "PARTITIONS 2\n" +
            "SUBPARTITION BY RANGE(TO_DAYS(`a73`))\n" +
            "(SUBPARTITION `p20241001` VALUES LESS THAN (739526),\n" +
            " SUBPARTITION `p20241002` VALUES LESS THAN (739527),\n" +
            " SUBPARTITION `p20241003` VALUES LESS THAN (739528),\n" +
            " SUBPARTITION `p20241004` VALUES LESS THAN (739529),\n" +
            " SUBPARTITION `p20241005` VALUES LESS THAN (739530),\n" +
            " SUBPARTITION `p20241006` VALUES LESS THAN (739531),\n" +
            " SUBPARTITION `p20241007` VALUES LESS THAN (739532),\n" +
            " SUBPARTITION `p20241008` VALUES LESS THAN (739533),\n" +
            " SUBPARTITION `p20241009` VALUES LESS THAN (739534),\n" +
            " SUBPARTITION `p20241010` VALUES LESS THAN (739535),\n" +
            " SUBPARTITION `p20241011` VALUES LESS THAN (739536),\n" +
            " SUBPARTITION `p20241012` VALUES LESS THAN (739537),\n" +
            " SUBPARTITION `p20241013` VALUES LESS THAN (739538),\n" +
            " SUBPARTITION `p20241014` VALUES LESS THAN (739539),\n" +
            " SUBPARTITION `p20241015` VALUES LESS THAN (739540),\n" +
            " SUBPARTITION `p20241016` VALUES LESS THAN (739541),\n" +
            " SUBPARTITION `p20241017` VALUES LESS THAN (739542),\n" +
            " SUBPARTITION `p20241018` VALUES LESS THAN (739543),\n" +
            " SUBPARTITION `p20241019` VALUES LESS THAN (739544),\n" +
            " SUBPARTITION `p20241020` VALUES LESS THAN (739545),\n" +
            " SUBPARTITION `p20241021` VALUES LESS THAN (739546),\n" +
            " SUBPARTITION `p20241022` VALUES LESS THAN (739547),\n" +
            " SUBPARTITION `p20241023` VALUES LESS THAN (739548),\n" +
            " SUBPARTITION `p20241024` VALUES LESS THAN (739549),\n" +
            " SUBPARTITION `p20241025` VALUES LESS THAN (739550),\n" +
            " SUBPARTITION `p20241026` VALUES LESS THAN (739551),\n" +
            " SUBPARTITION `p20241027` VALUES LESS THAN (739552),\n" +
            " SUBPARTITION `p20241028` VALUES LESS THAN (739553),\n" +
            " SUBPARTITION `p20241029` VALUES LESS THAN (739554),\n" +
            " SUBPARTITION `p20241030` VALUES LESS THAN (739555),\n" +
            " SUBPARTITION `p20241031` VALUES LESS THAN (739556));";

    private static final String SRC_TB_PART = "PARTITION BY KEY(`a2`)\n" +
            "PARTITIONS 2\n" +
            "SUBPARTITION BY RANGE(TO_DAYS(`a73`))\n" +
            "(SUBPARTITION `p202310` VALUES LESS THAN (739190),\n" +
            " SUBPARTITION `p202311` VALUES LESS THAN (739220),\n" +
            " SUBPARTITION `p202312` VALUES LESS THAN (739251),\n" +
            " SUBPARTITION `p202401` VALUES LESS THAN (739282),\n" +
            " SUBPARTITION `p202402` VALUES LESS THAN (739311),\n" +
            " SUBPARTITION `p202403` VALUES LESS THAN (739342),\n" +
            " SUBPARTITION `p202404` VALUES LESS THAN (739372),\n" +
            " SUBPARTITION `p202405` VALUES LESS THAN (739403),\n" +
            " SUBPARTITION `p202406` VALUES LESS THAN (739433),\n" +
            " SUBPARTITION `p202407` VALUES LESS THAN (739464),\n" +
            " SUBPARTITION `p202408` VALUES LESS THAN (739495),\n" +
            " SUBPARTITION `p202409` VALUES LESS THAN (739525),\n" +
            " SUBPARTITION `p202410` VALUES LESS THAN (739556),\n" +
            " SUBPARTITION `p202411` VALUES LESS THAN (739586),\n" +
            " SUBPARTITION `p202412` VALUES LESS THAN (739617));";

    private static final String TAR_TB_NAME = "c";
    private static final String SRC_TB_NAME = "f";


    public static void prepareData(Connection conn, boolean isSrc, String startDt, String endDt, int rowCount) throws SQLException {
        PreparedStatement pstmt = null;

        try {
            conn.setAutoCommit(false);
            // 准备 SQL 语句
            String sql = "INSERT IGNORE INTO " + (isSrc ? SRC_TB_NAME : TAR_TB_NAME) + " ("
                    + "a2, a3, a4, a5, a6, a7, a8, a9, a10, "
                    + "a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, "
                    + "a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, "
                    + "a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, "
                    + "a41, a42, a43, a44, a45, a46, a47, a48, a49, a50, "
                    + "a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, "
                    + "a61, a62, a63, a64, a65, a66, a67, a68, a69, a70, "
                    + "a71, a72, a73, a74, a75, a76, a77, a78"
                    + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                    " ?, ?)";

            pstmt = conn.prepareStatement(sql);

            // 设置每条记录的值
            for (int i = 1; i <= rowCount; i++) {
                setValues(pstmt, new Random().nextLong() % (rowCount * 5), new Random(), startDt, endDt);
                pstmt.addBatch();
                if (i % 1000 == 0) {
                    pstmt.executeBatch(); // 每1000条记录执行一次批量插入
                    conn.commit(); // 提交事务
                    System.out.println("Inserted " + i + " rows.");
                }
            }

            // 处理剩余不足1000条的记录
            pstmt.executeBatch();
            conn.commit();

            System.out.println("Total inserted rows: " + rowCount);

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void createTable(Connection conn, boolean isSrc) throws SQLException {
        String createTbl;
        if (isSrc) {
            createTbl = String.format(CREATE_TABLENAME, SRC_TB_NAME, SRC_TB_PART);
        } else {
            createTbl = String.format(CREATE_TABLENAME, TAR_TB_NAME, TAR_TB_PART);
        }
        JdbcUtil.executeUpdateSuccess(conn, createTbl);
    }

    private static void setValues(PreparedStatement pstmt, long randomLong, Random random, String startDt, String endDt) throws SQLException {
        // 使用占位符 ? 来设置每个字段的值
        //pstmt.setLong(1, null);                  // a1: 随机 bigint
        int i = 1;
        pstmt.setLong(i++, random.nextLong());           // a2: 随机 bigint
        pstmt.setInt(i++, random.nextInt(100));          // a3: 随机 int
        pstmt.setLong(i++, random.nextLong());           // a4: 随机 bigint
        pstmt.setByte(i++, (byte) random.nextInt(10));   // a5: 随机 tinyint
        pstmt.setInt(i++, random.nextInt(100));          // a6: 随机 int
        pstmt.setLong(i++, random.nextLong());           // a7: 随机 bigint
        pstmt.setByte(i++, (byte) random.nextInt(10));   // a8: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));   // a9: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a10: 随机 tinyint

        // 设置 decimal 类型的字段
        DecimalFormat df = new DecimalFormat("0.0000000000000000");
        pstmt.setString(i++, df.format(random.nextDouble())); // a11: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a12: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a13: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a14: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a15: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a16: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a17: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a18: 随机 decimal

        // 设置其他类型的字段
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a19: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a20: 随机 tinyint
        pstmt.setBoolean(i++, random.nextBoolean());    // a21: 随机 tinyint(1)
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a22: 随机 tinyint
        pstmt.setString(i++, df.format(random.nextDouble())); // a23: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a24: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a25: 随机 decimal
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a26: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a27: 随机 tinyint
        pstmt.setString(i++, df.format(random.nextDouble())); // a28: 随机 decimal

        // 设置字符串类型字段
        pstmt.setString(i++, generateRandomString(64)); // a29: 随机 varchar
        pstmt.setString(i++, generateRandomString(32)); // a30: 随机 varchar
        pstmt.setLong(i++, random.nextLong());          // a31: 随机 bigint
        pstmt.setInt(i++, random.nextInt(1000));        // a32: 随机 int
        pstmt.setLong(i++, random.nextLong());          // a33: 随机 bigint
        pstmt.setString(i++, df.format(random.nextDouble())); // a34: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a35: 随机 decimal
        pstmt.setLong(i++, random.nextLong());          // a36: 随机 bigint
        pstmt.setString(i++, df.format(random.nextDouble())); // a37: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a38: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a39: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a40: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a41: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a42: 随机 decimal
        pstmt.setString(i++, generateRandomString(64)); // a43: 随机 varchar
        pstmt.setString(i++, df.format(random.nextDouble())); // a44: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a45: 随机 decimal
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a46: 随机 tinyint
        pstmt.setString(i++, df.format(random.nextDouble())); // a47: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a48: 随机 decimal
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a49: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a50: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a51: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a52: 随机 tinyint
        pstmt.setString(i++, generateRandomString(128)); // a53: 随机 varchar
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a54: 随机 tinyint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a55: 随机 tinyint
        pstmt.setString(i++, df.format(random.nextDouble())); // a56: 随机 decimal
        pstmt.setLong(i++, random.nextLong());          // a57: 随机 bigint
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a58: 随机 tinyint
        pstmt.setLong(i++, random.nextLong());          // a59: 随机 bigint
        pstmt.setString(i++, generateRandomString(64)); // a60: 随机 varchar
        pstmt.setString(i++, generateRandomString(64)); // a61: 随机 varchar
        pstmt.setInt(i++, random.nextInt(1000));        // a62: 随机 int
        pstmt.setByte(i++, (byte) random.nextInt(10));  // a63: 随机 tinyint
        pstmt.setString(i++, generateRandomString(64)); // a64: 随机 varchar
        pstmt.setString(i++, df.format(random.nextDouble())); // a65: 随机 decimal
        pstmt.setString(i++, df.format(random.nextDouble())); // a66: 随机 decimal
        pstmt.setString(i++, generateRandomText());     // a67: 随机 text
        pstmt.setLong(i++, random.nextLong());          // a68: 随机 bigint
        pstmt.setInt(i++, random.nextInt(1000));        // a69: 随机 int
        pstmt.setString(i++, "{\"key\":\"value\"}");     // a70: 随机 json
        pstmt.setString(i++, "{\"key\":\"value\"}");     // a71: 随机 json
        pstmt.setInt(i++, random.nextInt(1000));        // a72: 随机 int
        pstmt.setString(i++, generateRandomDate(startDt, endDt));     // a73: 随机时间
        pstmt.setTimestamp(i++, getCurrentTimestamp()); // a74: 当前时间戳
        pstmt.setTimestamp(i++, getCurrentTimestamp()); // a75: 当前时间戳
        pstmt.setTimestamp(i++, getCurrentTimestamp()); // a76: 当前时间戳
        pstmt.setTimestamp(i++, getCurrentTimestamp()); // a77: 当前时间戳
        pstmt.setString(i++, df.format(random.nextDouble())); // a78: 随机 decimal
    }

    private static String generateRandomDate(String startDate, String endDate) {

        // 设置 a73 为 2024 年 10 月 1 日 到 2024 年 11 月 30 日之间的随机日期时间
        LocalDateTime start = parseDateTime(startDate);
        LocalDateTime end = parseDateTime(endDate);

        long startTime = start.toEpochSecond(ZoneOffset.UTC);
        long endTime = end.toEpochSecond(ZoneOffset.UTC);

        long randomTime = ThreadLocalRandom.current().nextLong(startTime, endTime);
        LocalDateTime randomDateTime = LocalDateTime.ofEpochSecond(randomTime, 0, ZoneOffset.UTC);

        return randomDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

    }

    private static String generateRandomString(int length) {
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            sb.append(characters.charAt(index));
        }
        return sb.toString();
    }

    private static String generateRandomText() {
        return generateRandomString(100); // 返回长度为100的随机文本
    }

    private static String getCurrentDateTime() {
        return java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    private static java.sql.Timestamp getCurrentTimestamp() {
        return new java.sql.Timestamp(System.currentTimeMillis());
    }

    public static Map<Long, Long> getPkList(Connection conn, int month) throws SQLException {
        String begin = "2024-" + month + "-01";
        String end = "2024-" + month + "-31";
        //String sql = String.format("select a1,count(1) from %s where a73 >= '%s' and a73<='%s' group by a1", SRC_TB_NAME, begin, end);
        String sql = String.format("select a1,count(1) from %s group by a1", SRC_TB_NAME);
        Map<Long, Long> pkList = new HashMap<>();
        try (ResultSet rs = JdbcUtil.executeQuery(sql, conn)) {
            while (rs.next()) {
                pkList.put(rs.getLong(1), rs.getLong(2));
            }
        }
        return pkList;
    }

    public static String genInsertIntoTarForSrcSQL(Long pk, int month) {

        String sql = "INSERT IGNORE INTO " + TAR_TB_NAME + " ("
                + "a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, "
                + "a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, "
                + "a21, a22, a23, a24, a25, a26, a27, a28, a29, a30, "
                + "a31, a32, a33, a34, a35, a36, a37, a38, a39, a40, "
                + "a41, a42, a43, a44, a45, a46, a47, a48, a49, a50, "
                + "a51, a52, a53, a54, a55, a56, a57, a58, a59, a60, "
                + "a61, a62, a63, a64, a65, a66, a67, a68, a69, a70, "
                + "a71, a72, a73, a74, a75, a76, a77, a78"
                + ") select * from " + SRC_TB_NAME + " FORCE INDEX (`PRIMARY`) " +
                "where a1 in (" + pk + ") ";
        //"where a1 in (" + pk + ") and a73 >= '"+begin+"' and a73 <= '" + end + "'";
        return sql;
    }

    public static boolean checkPkExistence(Connection conn, Long pk) {
        String sql = "select count(1) from " + TAR_TB_NAME + " where a1=" + pk;
        try {
            PreparedStatement preparedStatement = conn.prepareStatement(sql);
            ResultSet resultSet = preparedStatement.executeQuery();
            resultSet.next();
            return resultSet.getLong(1) > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static String genDeleteFromTarSQL(Long pk) {

        String sql = "delete from " + TAR_TB_NAME +
                " where a1 in (" + pk + ") ";
        //"where a1 in (" + pk + ") and a73 >= '"+begin+"' and a73 <= '" + end + "'";
        return sql;
    }

    public static String genSelectForUpdateFromTarSQL(Long pk) {

        String sql = "select * from " + TAR_TB_NAME +
                " where a1 in (" + pk + ") for update";
        //"where a1 in (" + pk + ") and a73 >= '"+begin+"' and a73 <= '" + end + "'";
        return sql;
    }

    public static LocalDate parseDate(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try {
            return LocalDate.parse(dateString, formatter);
        } catch (DateTimeParseException e) {
            System.err.println("Failed to parse date: " + dateString + ". Please use the format yyyy-MM-dd.");
            return null;
        }
    }

    public static LocalDate parseDateFromPartName(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        try {
            return LocalDate.parse(dateString.substring(1), formatter);
        } catch (DateTimeParseException e) {
            System.err.println("Failed to parse date: " + dateString + ". Please use the format yyyy-MM-dd.");
            return null;
        }
    }

    public static LocalDateTime parseDateTime(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try {
            return LocalDateTime.parse(dateString, formatter);
        } catch (DateTimeParseException e) {
            System.err.println("Failed to parse date: " + dateString + ". Please use the format yyyy-MM-dd.");
            return null;
        }
    }

    public static String getPartNameFromDate(LocalDate localDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYYMMdd");
        return "p" + localDate.format(formatter);
    }

    public static String dateToString(LocalDate localDate) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("YYYY-MM-dd");
        return localDate.format(formatter);
    }

    public static Pair<List<List<String>>, List<List<String>>> preparePartitionNames(String curNewestPartitionDate, String curOldestPartitionDate, int count) {
        List<List<String>> addPartitionNamesList = new ArrayList<>();
        List<List<String>> dropPartitionNamesList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            List<String> addPartitionNames = new ArrayList<>();
            List<String> dropPartitionNames = new ArrayList<>();
            if (i % 2 == 0 && RandomUtils.getBoolean() && (i + 2 < count)) {
                String curNewestPart = getPartNameFromDate(parseDate(curNewestPartitionDate).plusDays(i + 1));
                String curOldestPart = getPartNameFromDate(parseDate(curOldestPartitionDate).plusDays(i + 1));
                addPartitionNames.add(curNewestPart);
                dropPartitionNames.add(curOldestPart);
                i++;
            }
            String curNewestPart = getPartNameFromDate(parseDate(curNewestPartitionDate).plusDays(i + 1));
            String curOldestPart = getPartNameFromDate(parseDate(curOldestPartitionDate).plusDays(i + 1));
            addPartitionNames.add(curNewestPart);
            dropPartitionNames.add(curOldestPart);
            addPartitionNamesList.add(addPartitionNames);
            dropPartitionNamesList.add(dropPartitionNames);
        }
        return Pair.of(addPartitionNamesList, dropPartitionNamesList);
    }

    public enum ErrorType {
        UNEXPECTED_Double_Write,
        UNEXPECTED_KILL,
        UNEXPECTED_ROUTE_ERROR
    }
}