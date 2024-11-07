package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;

/**
 * 针对分区表的DDL打标测试
 * <p>
 * created by ziyang.lb
 **/
public class CdcLocalPartitionTableDdlRecordTest extends CdcBaseTest {
    private int startYear = Calendar.getInstance().get(Calendar.YEAR) - 1;
    private String T_ORDER = String.format("CREATE TABLE `t_order` (\n"
        + "\t`id` bigint(20) DEFAULT NULL,\n"
        + "\t`gmt_modified` datetime NOT NULL,\n"
        + "\tPRIMARY KEY (`gmt_modified`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4\n"
        + "PARTITION BY KEY(`gmt_modified`)\n"
        + "PARTITIONS 8\n"
        + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
        + "STARTWITH '%s-01-01'\n"
        + "INTERVAL 1 MONTH\n"
        + "EXPIRE AFTER 12\n"
        + "PRE ALLOCATE 6\n"
        + "PIVOTDATE NOW()\n", startYear);

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        String sql;
        String tokenHints;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery("select database()");

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database if exists cdc_ddl_local_partition";
            stmt.execute(sql);
            Thread.sleep(2000);
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());

            tokenHints = buildTokenHints();
            sql = tokenHints + "create database cdc_ddl_local_partition partition_mode = 'partitioning'";
            stmt.execute(sql);
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());

            sql = "use cdc_ddl_local_partition";
            stmt.execute(sql);

            doDDl(stmt);

            tokenHints = buildTokenHints();
            sql = tokenHints + "drop database cdc_ddl_local_partition";
            stmt.execute(sql);
            stmt.execute("use __cdc__");
            Assert.assertEquals(sql, getDdlRecordInfoListByToken(tokenHints).get(0).getDdlSql());
        }
    }

    private void doDDl(Statement stmt)
        throws SQLException {
        String sql;
        String tokenHints;

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + T_ORDER;
        stmt.execute(sql);
        List<DdlRecordInfo> ddlRecordInfoList = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertTrue(StringUtils.startsWith(ddlRecordInfoList.get(0).getDdlSql(), tokenHints));
        Assert.assertEquals(1, ddlRecordInfoList.size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + String.format("ALTER TABLE t_order EXPIRE LOCAL PARTITION p%s%s01",
            startYear,
            StringUtils.leftPad(String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1), 2, "0"));
        stmt.execute(sql);
        Assert.assertEquals(1, getDdlRecordInfoListByToken(tokenHints).size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "ALTER TABLE t_order EXPIRE LOCAL PARTITION";
        stmt.execute(sql);
        Assert.assertEquals(1, getDdlRecordInfoListByToken(tokenHints).size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "CREATE TABLE t1 (\n"
            + "    id bigint,\n"
            + "    gmt_modified DATETIME PRIMARY KEY NOT NULL\n"
            + ") single;";
        stmt.execute(sql);
        ddlRecordInfoList = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertTrue(StringUtils.startsWith(ddlRecordInfoList.get(0).getDdlSql(), tokenHints));
        Assert.assertEquals(1, ddlRecordInfoList.size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "alter table t1\n"
            + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
            + "STARTWITH " + String.format("'%s-01-01'", startYear) + "\n"
            + "INTERVAL 1 MONTH\n"
            + "EXPIRE AFTER 12\n"
            + "PRE ALLOCATE 6\n"
            + "PIVOTDATE NOW()";
        stmt.execute(sql);
        Assert.assertEquals(1, getDdlRecordInfoListByToken(tokenHints).size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "alter table t1 remove local partitioning";
        stmt.execute(sql);
        Assert.assertEquals(0, getDdlRecordInfoListByToken(tokenHints).size());

        // Test Step
        tokenHints = buildTokenHints();
        sql = tokenHints + "check table t_order with local partition";
        stmt.execute(sql);
        Assert.assertEquals(0, getDdlRecordInfoListByToken(tokenHints).size());
    }
}
