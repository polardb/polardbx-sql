package com.alibaba.polardbx.qatest.dql.sharding.agg;

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
public class ForceIndexPrimaryAsOfTest extends ReadBaseTestCase {
    final String tablePrefix = "ForceIndexPrimaryAsOfTest";
    final String singleTable = tablePrefix + "_singe";
    final String shardingTable = tablePrefix + "_sharding";
    final String createShardingTableSql = "create table if not exists " + shardingTable
        + "(id int primary key, "
        + "a int,"
        + "global index g_idx_ForceIndexPrimaryAsOfTest(a) dbpartition by hash(a),"
        + "index idx_ForceIndexPrimaryAsOfTest(a)"
        + ") dbpartition by hash(id)";
    final String createSingleTableSql = "create table if not exists " + singleTable
        + "(id int primary key, "
        + "a int,"
        + "index idx_ForceIndexPrimaryAsOfTest(a)"
        + ")";

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
    public void testAggWithAsOf() throws SQLException {
        final String[] testSqls = {
            "select count(*) from %s as of timestamp %s",
            // Sum secondary index.
            "select sum(a) from %s as of timestamp %s"};
        final String[] tableNames = {
            singleTable, shardingTable
        };

        for (String testSql : testSqls) {
            for (String table : tableNames) {
                String sql = String.format(testSql, table, "'2022-01-01 00:00:00'");
                System.out.println(sql);
                try (final ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "explain " + sql)) {
                    boolean found = false;
                    while (rs.next()) {
                        final String explainResult = rs.getString(1);
                        System.out.println(explainResult);
                        found |= StringUtils.containsIgnoreCase(explainResult, "force index(primary)");
                    }
                    Assert.assertTrue(found, "not found force index in explain result.");
                }

                JdbcUtil.executeQueryFaied(tddlConnection, sql,
                    new String[] {
                        "The definition of the table required by the flashback query has changed",
                        "Snapshot too old"});
            }
        }
    }
}
