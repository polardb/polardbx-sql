package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class ProcedureMaxDepthTest extends BaseTestCase {
    protected Connection tddlConnection;
    private String BASE_PROCEDURE_NAME = "P1";
    private String[] UPPER_PROCEDURE_NAMES = new String[] {"P2", "P3", "P4"};
    private String DROP_PROCEDURE = "drop procedure if exists %s";
    private String BASE_PROCEDURE = "create procedure %s() SELECT 1;";
    private String UPPER_PROCEDURE = "create procedure %s() call %s";
    private String SET_MAX_PL_DEPTH = "set %s MAX_PL_DEPTH = %s";

    @Before
    public void prepareData() throws SQLException {
        this.tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, BASE_PROCEDURE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(BASE_PROCEDURE, BASE_PROCEDURE_NAME));
        for (int i = 0; i < UPPER_PROCEDURE_NAMES.length; ++i) {
            String name = UPPER_PROCEDURE_NAMES[i];
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, name));
            JdbcUtil.executeSuccess(tddlConnection,
                String.format(UPPER_PROCEDURE, name, i == 0 ? "P1" : UPPER_PROCEDURE_NAMES[i - 1]));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }

    @Test
    public void testMaxDepth() throws InterruptedException {
        // test set session
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "session", 1));
        JdbcUtil.executeSuccess(tddlConnection, "call p2");
        JdbcUtil.executeFailed(tddlConnection, "call p3", "reached max");

        // test set global
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "global", 2));
        JdbcUtil.executeSuccess(tddlConnection, "call p3");
        JdbcUtil.executeFailed(tddlConnection, "call p4", "reached max");

        // wait for sync
        Thread.sleep(2_000);
        // use another connection to test
        Connection checkConn = getPolardbxConnection();
        JdbcUtil.executeSuccess(checkConn, "call p3");
        JdbcUtil.executeFailed(checkConn, "call p4", "reached max");
    }

    @After
    public void dropData() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, BASE_PROCEDURE_NAME));
        for (String name : UPPER_PROCEDURE_NAMES) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_PROCEDURE, name));
        }
        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_MAX_PL_DEPTH, "GLOBAL", 4096));
    }
}
