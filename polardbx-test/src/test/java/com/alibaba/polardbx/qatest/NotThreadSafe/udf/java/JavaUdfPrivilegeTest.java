package com.alibaba.polardbx.qatest.NotThreadSafe.udf.java;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class JavaUdfPrivilegeTest extends BaseTestCase {
    public static String SIMPLE_USER_NAME = "normal_user";

    public static String SIMPLE_USER_PASSWD = "x123456";

    private static Connection rootConnection;

    private static Connection simpleConnection;

    private static String FUNC_NAME = "test_udf_privilege";

    @BeforeClass
    public static void init() throws SQLException {
        rootConnection = ConnectionManager.getInstance().newPolarDBXConnection();
        JdbcUtils.execute(rootConnection, "use " + PropertiesUtil.polardbXDBName1(true));
        JdbcUtils.execute(rootConnection, String.format("drop user if exists '%s'@'%%'", SIMPLE_USER_NAME));
        JdbcUtils.execute(rootConnection,
            String.format("create user '%s'@'%%' identified by '%s'", SIMPLE_USER_NAME, SIMPLE_USER_PASSWD));
        JdbcUtils.execute(rootConnection,
            String.format("grant all on %s.* to '%s'@'%%'", PropertiesUtil.polardbXDBName1(true), SIMPLE_USER_NAME));
        JdbcUtils.execute(rootConnection, String.format("drop java function if exists %s", FUNC_NAME));
        simpleConnection = ConnectionManager.getInstance().newPolarDBXConnection(SIMPLE_USER_NAME, SIMPLE_USER_PASSWD);
        JdbcUtils.execute(simpleConnection, "use " + PropertiesUtil.polardbXDBName1(true));
    }

    @Test
    public void testCreateJavaUdf() {
        String sql = String.format("create java function %s RETURN_TYPE BIGINT "
            + "CODE "
            + "public class %s extends UserDefinedJavaFunction "
            + "{             "
            + "     public Object compute(Object[] args) {       "
            + "         return 55;     "
            + "     }"
            + "} end_code", FUNC_NAME, StringUtils.funcNameToClassName(FUNC_NAME));
        JdbcUtil.executeFailed(simpleConnection, sql, "ERR_CHECK_PRIVILEGE_FAILED");
        JdbcUtil.executeSuccess(rootConnection, sql);
    }

    @Test
    public void testDropJavaUdf() {
        String sql = "drop java function if exists " + FUNC_NAME;
        JdbcUtil.executeFailed(simpleConnection, sql, "ERR_CHECK_PRIVILEGE_FAILED");
        JdbcUtil.executeSuccess(rootConnection, sql);
    }

    @Test
    public void testShowJavaUdf() {
        String sql = "select * from information_schema.java_functions";
        JdbcUtil.executeFailed(simpleConnection, sql, "ERR_CHECK_PRIVILEGE_FAILED");
        JdbcUtil.executeSuccess(rootConnection, sql);
    }

    @AfterClass
    public static void clear() throws SQLException {
        JdbcUtils.execute(rootConnection, String.format("drop user if exists '%s'@'%%'", SIMPLE_USER_NAME));
        JdbcUtils.execute(rootConnection, String.format("drop java function if exists %s", FUNC_NAME));
    }
}
