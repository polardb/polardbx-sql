package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PairWiseJoinTest extends ColumnarReadBaseTestCase {
    private static final String TABLE_1 = "test_broadcast_under_pairwise_1";
    private static final String TABLE_2 = "test_broadcast_under_pairwise_2";
    private static final String TABLE_3 = "test_broadcast_under_pairwise_3";

    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";

    @Before
    public void prepare() {
        dropTable();
        prepareData();
    }

    @Test
    public void testThreeTablePairWise() throws SQLException {
        // test partition join with two pairwise join
        String sql =
            String.format("select count(*) from %s t1 join %s t2 on t1.c1 = t2.c1 join %s t3 on t2.c1 = t3.c1", TABLE_1,
                TABLE_2, TABLE_3);
        checkResult(sql);

        // adjust join order
        injectStatistics();
        checkResult(sql);
    }

    private void checkResult(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);
        if (rs.next()) {
            Assert.assertTrue(rs.getLong(1) == 3, "count result should be 3, but was " + rs.getLong(1));
        } else {
            Assert.fail("get empty result");
        }
        rs.close();
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_2));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_3));
    }

    private void prepareData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_2));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_3));
        // insert data
        for (int i = 0; i < 3; ++i) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_1, i, i));
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_2, i, i));
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_3, i, i));
        }
        // create columnar index
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_1, TABLE_1,
            "c2", "c2", 4);
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_2, TABLE_2,
            "c1", "c1", 4);
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_3, TABLE_3,
            "c1", "c1", 4);
    }

    private void injectStatistics() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery("select database()", tddlConnection);
        String db = null;
        if (rs.next()) {
            db = rs.getString(1);
        } else {
            Assert.fail("get empty result");
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format("set STATISTIC_CORRECTIONS = 'STATISTIC TRACE INFO:\n"
                + "MULTI[15]\n"
                + "Catalog:%s,%s\n"
                + "Action:getRowCount\n"
                + "StatisticValue:30000\n"
                + "CACHE_LINE, modify by 2023-10-26 14:33:17\n"
                + "\n"
                + "MULTI[15]\n"
                + "Catalog:%s,%s,c1\n"
                + "Action:getCardinality\n"
                + "StatisticValue:3000\n"
                + "HLL_SKETCH, modify by 2023-10-26 14:33:17\n"
                + "\n"
                + "MULTI[15]\n"
                + "Catalog:%s,%s\n"
                + "Action:getRowCount\n"
                + "StatisticValue:30000\n"
                + "CACHE_LINE, modify by 2023-10-26 14:33:19\n"
                + "\n"
                + "MULTI[11]\n"
                + "Catalog:%s,%s,c1\n"
                + "Action:getCardinality\n"
                + "StatisticValue:3000\n"
                + "HLL_SKETCH, modify by 2023-10-26 14:33:19\n"
                + "\n"
                + "MULTI[10]\n"
                + "Catalog:%s,%s,c1\n"
                + "Action:getCardinality\n"
                + "StatisticValue:3\n"
                + "HLL_SKETCH, modify by 2023-10-26 14:33:15\n"
                + "\n"
                + "MULTI[10]\n"
                + "Catalog:%s,%s\n"
                + "Action:getRowCount\n"
                + "StatisticValue:3\n"
                + "CACHE_LINE, modify by 2023-10-26 14:33:15'"
            , db, TABLE_2, db, TABLE_2
            , db, TABLE_3, db, TABLE_3
            , db, TABLE_1, db, TABLE_1
        ));
    }
}
