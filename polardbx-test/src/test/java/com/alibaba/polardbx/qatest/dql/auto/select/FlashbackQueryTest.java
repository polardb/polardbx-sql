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
            "/*+TDDL:plancache=false*/ select * from FlashbackQueryTest partition(p1) as of timestamp '"
                + currentTime + "'",
            "/*+TDDL:plancache=false*/ select * from FlashbackQueryTest tt partition(p1) as of timestamp '"
                + currentTime + "'",
            "/*+TDDL:plancache=false*/ select * from FlashbackQueryTest as of timestamp '"
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

}