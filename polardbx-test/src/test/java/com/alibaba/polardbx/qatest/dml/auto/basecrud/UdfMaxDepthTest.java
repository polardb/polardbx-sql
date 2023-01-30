package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.FileStoreIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

@FileStoreIgnore
public class UdfMaxDepthTest extends BaseTestCase {
    protected Connection tddlConnection;
    private String BASE_function_NAME = "F1";
    private String[] UPPER_function_NAMES = new String[] {"F2", "F3", "F4"};
    private String DROP_function = "drop function if exists %s";
    private String BASE_function = "create function %s() returns int return 1 + 1;";
    private String UPPER_function = "create function %s() returns int return %s()";
    private String SET_MAX_PL_DEPTH = "set %s MAX_PL_DEPTH = %s";

    @Before
    public void prepareData() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_function, BASE_function_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(BASE_function, BASE_function_NAME));
        for (int i = 0; i < UPPER_function_NAMES.length; ++i) {
            String name = UPPER_function_NAMES[i];
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_function, name));
            JdbcUtil.executeSuccess(tddlConnection,
                String.format(UPPER_function, name, i == 0 ? "F1" : UPPER_function_NAMES[i - 1]));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }

    @Test
    public void testMaxDepth() throws InterruptedException {
        // test set session
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "session", 1));
        JdbcUtil.executeSuccess(tddlConnection, "select F2()");
        JdbcUtil.executeFaied(tddlConnection, "select f3()", "reached max");

        // test set global
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "global", 2));
        JdbcUtil.executeSuccess(tddlConnection, "select f3()");
        JdbcUtil.executeFaied(tddlConnection, "select f4()", "reached max");

        // wait for sync
        Thread.sleep(2_000);
        // use another connection to test
        Connection checkConn = getPolardbxConnection();
        JdbcUtil.executeSuccess(checkConn, "select f3()");
        JdbcUtil.executeFaied(checkConn, "select f4()", "reached max");
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_function, BASE_function_NAME));
        for (String name : UPPER_function_NAMES) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_function, name));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }
}
