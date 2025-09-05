package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PrunerColumnarTracerTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "test_columnar_tracer";
    private static final String COLUMNAR_INDEX_NAME = "col_" + TABLE_NAME;
    private static final String CREATE_TABLE = "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";
    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";
    private static final String DROP_TABLE = "drop table if exists %s";
    @Before
    public void prepare() {
        dropTable();
        prepareData();
    }

    @Test
    public void testGlobalShowPruneTracer() throws SQLException {

        String sqlSelect =
            "trace select * from " + TABLE_NAME + " force index (" + COLUMNAR_INDEX_NAME + ") where (c1 = 1)";

        String sqlShow = "show prune tracer";

        //execute select sql
        ResultSet rs = JdbcUtil.executeQuery(sqlSelect, tddlConnection);
        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();
        Assert.assertEquals(1, count);

        //execute show prune tracer
        ResultSet rsShow = JdbcUtil.executeQuery(sqlShow, tddlConnection);
        int showCount = 0;
        while (rsShow.next()) {
            showCount++;
        }
        rsShow.close();
        Assert.assertEquals(1, showCount);
    }
    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }
    private void prepareData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME,  1, 1));
        // create columnar index
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE_NAME, TABLE_NAME, "c1", "c1", 4);
    }

}
