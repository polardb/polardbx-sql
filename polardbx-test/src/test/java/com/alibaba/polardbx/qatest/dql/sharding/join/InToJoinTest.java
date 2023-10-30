package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

public class InToJoinTest extends ReadBaseTestCase {
    static final String TABLE_NAME = "test_in_to_join";

    @Test
    public void testInToJoinWithDataChange() {
        String sql =
            "/*+TDDL: IN_SUB_QUERY_THRESHOLD=8 */ select * from %s where c1 in (1,1,1,1,1,1,1,1,1,1) and c2 in (3,4);";
        checkPlanUseMaterializedJoin(String.format(sql, TABLE_NAME));

        insertData();
        JdbcUtil.executeQuerySuccess(tddlConnection, "analyze table " + TABLE_NAME);
        checkPlanUseMaterializedJoin(String.format(sql, TABLE_NAME));
    }

    private void checkPlanUseMaterializedJoin(String sql) {
        String explainResult =
            getExplainResult(tddlConnection, sql).toLowerCase();
        Assert.assertTrue(explainResult.contains("materializedsemijoin"),
            "plan should be materialized semi join");
    }

    private void checkPlanUseBkaJoinWithAgg(String sql) {
        String explainResult =
            getExplainResult(tddlConnection, sql).toLowerCase();
        Assert.assertTrue(explainResult.contains("BKAJoin(condition=\"ROW_VALUE = c1\", type=\"inner\")".toLowerCase())
                && explainResult.contains("HashAgg(group=\"ROW_VALUE\")".toLowerCase()),
            "plan should be bka join with agg");
    }

    @Before
    public void prepareTable() {
        JdbcUtil.executeSuccess(tddlConnection, "drop table if exists " + TABLE_NAME);
        String createTable = "CREATE TABLE %s (\n"
            + "`c1` int(11) DEFAULT NULL,\n"
            + "\t`c2` int(11) DEFAULT NULL,\n"
            + "\tKEY `auto_shard_key_c1` USING BTREE (`c1`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`c1`)";
        JdbcUtil.executeSuccess(tddlConnection, String.format(createTable, TABLE_NAME));
    }

    private void insertData() {
        String insertData =
            "insert into %s values (1,1), (1,2), (1,3);";
        JdbcUtil.executeSuccess(tddlConnection, String.format(insertData, TABLE_NAME));
    }
}
