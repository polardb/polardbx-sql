package com.alibaba.polardbx.qatest.NotThreadSafe.udf.sql;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class UdfMaxDepthTest extends BaseTestCase {
    protected Connection tddlConnection;
    private static final String BASE_FUNCTION_NAME = "F1";
    private static final String[] UPPER_FUNCTION_NAMES = new String[] {"F2", "F3", "F4"};
    private static final String DROP_FUNCTION = "drop function if exists %s";
    private static final String BASE_FUNCTION = "create function %s() returns int return 1 + 1;";
    private static final String UPPER_FUNCTION = "create function %s() returns int return %s()";
    private static final String SET_MAX_PL_DEPTH = "set %s MAX_PL_DEPTH = %s";

    @Before
    public void prepareData() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, BASE_FUNCTION_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(BASE_FUNCTION, BASE_FUNCTION_NAME));
        for (int i = 0; i < UPPER_FUNCTION_NAMES.length; ++i) {
            String name = UPPER_FUNCTION_NAMES[i];
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, name));
            JdbcUtil.executeSuccess(tddlConnection,
                String.format(UPPER_FUNCTION, name, i == 0 ? "F1" : UPPER_FUNCTION_NAMES[i - 1]));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }

    @Test
    public void testMaxDepth() throws InterruptedException {
        // test set session
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "session", 1));
        JdbcUtil.executeSuccess(tddlConnection, "select F2()");
        JdbcUtil.executeFailed(tddlConnection, "select f3()", "reached max");

        // test set global
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "global", 2));
        JdbcUtil.executeSuccess(tddlConnection, "select f3()");
        JdbcUtil.executeFailed(tddlConnection, "select f4()", "reached max");

        // wait for sync
        Thread.sleep(2_000);
        // use another connection to test
        Connection checkConn = getPolardbxConnection();
        JdbcUtil.executeSuccess(checkConn, "select f3()");
        JdbcUtil.executeFailed(checkConn, "select f4()", "reached max");
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, BASE_FUNCTION_NAME));
        for (String name : UPPER_FUNCTION_NAMES) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, name));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }
}
