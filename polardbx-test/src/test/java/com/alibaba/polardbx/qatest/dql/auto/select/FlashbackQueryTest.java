package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class FlashbackQueryTest extends AutoReadBaseTestCase {
    final String dropTableSql = "DROP TABLE IF EXISTS FlashbackQueryTest";
    final String createTableSql = "CREATE TABLE FlashbackQueryTest (\n"
        + "`a` int(11) NOT NULL,\n"
        + "`b` int(11) NOT NULL,\n"
        + "`c` int(11) NOT NULL,\n"
        + "`d` int(11) NOT NULL,\n"
        + "GLOBAL INDEX `idxb` (`b`) COVERING (`a`) PARTITION BY RANGE COLUMNS (`b`) (\n"
        + "PARTITION `p1` VALUES LESS THAN (100)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p2` VALUES LESS THAN (200)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p3` VALUES LESS THAN (300)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p4` VALUES LESS THAN MAXVALUE\n"
        + "STORAGE ENGINE InnoDB\n"
        + "),\n"
        + "CLUSTERED INDEX `idxc` (`c`) PARTITION BY RANGE COLUMNS (`c`) (\n"
        + "PARTITION `p1` VALUES LESS THAN (100)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p2` VALUES LESS THAN (200)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p3` VALUES LESS THAN (300)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p4` VALUES LESS THAN MAXVALUE\n"
        + "STORAGE ENGINE InnoDB\n"
        + "),\n"
        + "GLOBAL INDEX `idxd` (`d`) COVERING (`a`) PARTITION BY RANGE COLUMNS (`d`) (\n"
        + "PARTITION `p1` VALUES LESS THAN (100)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p2` VALUES LESS THAN (200)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p3` VALUES LESS THAN (300)\n"
        + "STORAGE ENGINE InnoDB,\n"
        + "PARTITION `p4` VALUES LESS THAN MAXVALUE\n"
        + "STORAGE ENGINE InnoDB\n"
        + ")\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
        + "PARTITION BY RANGE COLUMNS(`a`)\n"
        + "(\n"
        + "PARTITION `p1` VALUES LESS THAN (10) ENGINE = InnoDB,\n"
        + "PARTITION `p2` VALUES LESS THAN (20) ENGINE = InnoDB,\n"
        + "PARTITION `p3` VALUES LESS THAN (30) ENGINE = InnoDB,\n"
        + "PARTITION `p4` VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB);";

    final String tableName = "FlashbackQueryTest";
    final String tableFormat = "CREATE TABLE FlashbackQueryTest ("
        + "`a` int(11) NOT NULL AUTO_INCREMENT,\n"
        + "`b` int(11) NOT NULL,\n"
        + "`c` int(11) NOT NULL,\n"
        + "`d` int(11) NOT NULL,\n"
        + "PRIMARY KEY (`a`) \n"
        + ") {0} ";

    final String[] partitionStrings = new String[] {
        "single",
        "broadcast",
        "partition by key(a)"
    };

    @Before
    public void before() {
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);
    }

    @After
    public void after() {
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Test
    public void testPartition() throws SQLException {
        String currentTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        final List<String> testSqlList = ImmutableList.of(
            "/*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest partition(p1) as of timestamp '"
                + currentTime + "'",
            "/*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest tt partition(p1) as of timestamp '"
                + currentTime + "'",
            "/*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of timestamp '"
                + currentTime + "' as tt partition(p1)"
        );
        final List<String> explainResult = ImmutableList.of(
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF TIMESTAMP ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n",
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF TIMESTAMP ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n",
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF TIMESTAMP ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n"
        );
        final List<String> explainResult80 = ImmutableList.of(
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF GCN ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n",
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF GCN ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n",
            "Gather(concurrent=true)\n"
                + "  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF GCN ? AS `FlashbackQueryTest`\")\n"
                + "HitCache:false\n"
                + "Source:null\n"
                + "TemplateId: NULL\n"
        );

        for (int i = 0; i < testSqlList.size(); i++) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + testSqlList.get(i));
            StringBuilder sb = new StringBuilder();
            while (rs.next()) {
                sb.append(rs.getString(1)).append("\n");
            }
            System.out.println(sb);
            if (isMySQL80()) {
                Assert.assertEquals(explainResult80.get(i), sb.toString());
            } else {
                Assert.assertEquals(explainResult.get(i), sb.toString());
            }
            JdbcUtil.executeSuccess(tddlConnection, testSqlList.get(i));
        }
    }

    @Test
    public void testTimezone() {
        Date date = new Date();

        // Convert specified timezone to default timezone.
        TimeZone sourceTimeZone = TimeZone.getDefault();
        TimeZone targetTimeZone = TimeZone.getTimeZone("GMT+2");
        long sourceOffset = sourceTimeZone.getRawOffset();
        long targetOffset = targetTimeZone.getRawOffset();
        long offset = targetOffset - sourceOffset;
        date.setTime(date.getTime() + offset);

        String targetTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
        String sql = "select * from FlashbackQueryTest partition(p1) as of timestamp '"
            + targetTimestamp + "'";

        try {
            System.out.println(sql);
            JdbcUtil.executeSuccess(tddlConnection, "set time_zone = '+2:00'");
            JdbcUtil.executeSuccess(tddlConnection, sql);
        } finally {
            JdbcUtil.executeSuccess(tddlConnection, "set time_zone = 'SYSTEM'");
        }
    }

    @Test
    public void test57Tso() {
        if (isMySQL80()) {
            return;
        }
        String sql =
            "/*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest tt partition(p1) as of tso 10000";
        String expect =
            "Gather(concurrent=true)  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF TSO ? AS `FlashbackQueryTest`\")HitCache:falseSource:nullTemplateId: NULL";
        String explain = JdbcUtil.getExplainResult(tddlConnection, sql);
        Assert.assertEquals(expect, explain);
    }

    @Test
    public void test80Tso() {
        if (!isMySQL80()) {
            return;
        }
        String sql =
            "/*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest tt partition(p1) as of tso 10000";
        String expect =
            "Gather(concurrent=true)  LogicalView(tables=\"FlashbackQueryTest[p1]\", sql=\"SELECT `a`, `b`, `c`, `d` FROM `FlashbackQueryTest` AS OF GCN ? AS `FlashbackQueryTest`\")HitCache:falseSource:nullTemplateId: NULL";
        String explain = JdbcUtil.getExplainResult(tddlConnection, sql);
        Assert.assertEquals(expect, explain);
    }

    @Test
    public void testTraceFunction() throws Exception {
        for (String partition : partitionStrings) {
            String createTable = MessageFormat.format(tableFormat, partition);

            JdbcUtil.dropTable(tddlConnection, tableName);
            JdbcUtil.executeSuccess(tddlConnection, createTable);

            //等待5s，避免时间转tso：(UNIX_TIMESTAMP(now()) << 22) * 1000，报错：The definition of the table required by the flashback query has changed
            Thread.sleep(5000);

            String sql =
                "set @tso=tso_timestamp(); trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso @tso";
            JdbcUtil.executeSuccess(tddlConnection, sql);
            List<List<String>> result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("@tso".toUpperCase()));

            sql =
                "set @tso=tso_timestamp(); trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso @tso + 10 - 10";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("@tso".toUpperCase()));

            sql =
                "trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso tso_timestamp()";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("tso_timestamp".toUpperCase()));

            sql =
                "trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso (UNIX_TIMESTAMP(now()) << 22) * 1000";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("now()".toUpperCase()));

            sql =
                "trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso tso_timestamp() + round(rand())";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("rand()".toUpperCase()));

            sql =
                "trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso (UNIX_TIMESTAMP(CURRENT_TIMESTAMP()) << 22) * 1000";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("CURRENT_TIMESTAMP()".toUpperCase()));

            //点查
            sql =
                "trace /*+TDDL:plancache=false enable_mpp=false*/ select * from FlashbackQueryTest as of tso tso_timestamp() + round(rand()) where a = 1";
            JdbcUtil.executeSuccess(tddlConnection, sql);

            result = getTrace(tddlConnection);
            System.out.println(result.get(0).get(11));
            Assert.assertFalse(result.get(0).get(11).toUpperCase().contains("rand()".toUpperCase()));
        }
    }
}