package com.alibaba.polardbx.qatest.columnar.dql;

import com.alibaba.polardbx.qatest.ColumnarIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Preventing columnar table from being dropped by closing cold data feature
 */
@ColumnarIgnore
public class ColumnarWithColdDataTest extends ColumnarReadBaseTestCase {
    private static final String TABLE = "test_columnar_with_cold_data";

    private static final String CREATE_TABLE =
        "create table %s (c1 int primary key, c2 varchar(255)) partition by hash(c1)";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String INSERT_DATA = "insert into %s values (%s, '%s')";

    @Before
    public void prepare() throws SQLException {
        dropTable();
        prepareData();
        prepareParam();
        clearColdDataStatus();
    }

    @After
    public void dropTable() throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE));
        clearColdDataStatus();
    }

    private void prepareData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE));
        // insert data
        for (int i = 0; i < 3; ++i) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE, i, i));
        }
        // create columnar index
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + TABLE, TABLE,
            "c2", "c2", 4);
    }

    private void prepareParam() {
        JdbcUtil.executeSuccess(tddlConnection, "set global workload_type=ap");
    }

    @Test
    public void testTurnOffAndOnColdData() throws SQLException {
        checkResult();
        turnOffColdData();
        checkResult();
        turnOnColdData();
        checkResult();
    }

    @Test
    public void testTurnOnAndOffColdData() throws SQLException {
        checkResult();
        turnOnColdData();
        checkResult();
        turnOffColdData();
        checkResult();
    }

    private void checkResult() {
        String columnarSql = "select * from " + TABLE + " force index (" + "col_" + TABLE + ")";
        String primarySql = "select * from " + TABLE + " force index (primary)";
        DataValidator.selectContentSameAssertWithDiffSql(columnarSql, primarySql, null, tddlConnection, tddlConnection,
            false, false, false);
    }
}
