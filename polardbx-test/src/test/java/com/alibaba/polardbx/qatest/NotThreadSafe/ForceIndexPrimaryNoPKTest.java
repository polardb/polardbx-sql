package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@NotThreadSafe
public class ForceIndexPrimaryNoPKTest extends ReadBaseTestCase {
    final String tablePrefix = "ForceIndexPrimaryNoPKTest";
    final String singleTable = tablePrefix + "_singe";
    final String shardingTable = tablePrefix + "_sharding";
    final String createShardingTableSql = "create table if not exists " + shardingTable
        + "(id int primary key, "
        + "a int,"
        + "index idx_ForceIndexPrimaryNoPKTest(a)"
        + ") dbpartition by hash(a)";
    final String createSingleTableSql = "create table if not exists " + singleTable
        + "(id int primary key, "
        + "a int,"
        + "index idx_ForceIndexPrimaryNoPKTest(a)"
        + ")";
    final String dropPkSql = "alter table %s drop primary key";

    @Before
    public void PrepareData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + singleTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + shardingTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createShardingTableSql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSingleTableSql);

        final StringBuilder insertTemp = new StringBuilder("insert into %s values");
        final int batchSize = 100;
        for (int i = 0; i < batchSize; i++) {
            insertTemp.append(String.format(" (%s, %s)", i, i));
            if (i < batchSize - 1) {
                insertTemp.append(",");
            }
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertTemp.toString(), shardingTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(insertTemp.toString(), singleTable));

        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropPkSql, shardingTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(dropPkSql, singleTable));

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set PLAN_CACHE=true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set ENABLE_FORCE_PRIMARY_FOR_TSO=true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set transaction_policy=\"TSO\"");
    }

    @After
    public void clearData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + singleTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop table if exists " + shardingTable);
    }

    @Test
    public void testAggWithNoPK() throws SQLException {
        final String[] testSqls = {
            "select count(*) from %s",
            // Sum secondary index.
            "select sum(a) from %s"};
        final String[] tableNames = {
            singleTable, shardingTable
        };

        for (String testSql : testSqls) {
            for (String table : tableNames) {
                String sql = String.format(testSql, table);
                System.out.println(sql);
                try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql)) {
                    boolean found = false;
                    while (rs.next()) {
                        final String explainResult = rs.getString(1);
                        System.out.println(explainResult);
                        found |= StringUtils.containsIgnoreCase(explainResult, "force index(primary)");
                    }
                    Assert.assertTrue(!found, "Found unexpected force index in explain result.");
                }

                JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            }
        }
    }
}
