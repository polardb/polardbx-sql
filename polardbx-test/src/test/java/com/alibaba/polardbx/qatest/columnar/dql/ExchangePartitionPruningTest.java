package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExchangePartitionPruningTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_1 = "test_exchange_prune_1";
    private static final String TABLE_2 = "test_exchange_prune_2";

    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";

    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";

    @Before
    public void prepare() {
        dropTable();
        prepareData();
    }

    @Test
    public void testExchangePrune() throws SQLException {
        // test partition join with two pairwise join
        String sql =
            String.format("select count(*) from %s t1 join %s t2 on t1.c1 = t2.c1", TABLE_1, TABLE_2);
        checkResult(sql);
    }

    private void checkResult(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            long result = rs.getLong(1);
            Assert.assertTrue(result == 5, "count result should be 5, but was " + result);
        } else {
            Assert.fail("get empty result");
        }
        rs.close();
    }

    @After
    public void dropTable() {
        JdbcUtil.dropTable(tddlConnection, TABLE_1);
        JdbcUtil.dropTable(tddlConnection, TABLE_2);
    }

    private void prepareData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_2));
        // insert data
        for (int i = 0; i < 5; ++i) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, i, i));
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_2, i, i));
        }

        // insert extra data which will be discard when exchange
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, 7, 7));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, 8, 8));

        // create columnar index
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_1, TABLE_1,
            "c2", "c2", 10);
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_2, TABLE_2,
            "c1", "c1", 10);
    }
}
