package com.alibaba.polardbx.qatest.NotThreadSafe.udf.java;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class JavaUdfMaxNumTest extends BaseTestCase {

    private static Connection connection;

    private static String FUNC_NAME = "test_udf_create_";

    @Before
    public void init() throws SQLException {
        connection = getPolardbxConnection();
    }

    @Test
    public void testSetSessionShouldFail() {
        JdbcUtil.executeFailed(connection, "set max_java_udf_num = 5", "should be set with global scope");
    }

    @Test
    public void testCreateJavaUdfSuccess() throws SQLException {
        JdbcUtils.execute(connection, "set global max_java_udf_num = 5");
        for (int i = 0; i < 6; ++i) {
            String funName = FUNC_NAME + i;
            JdbcUtils.execute(connection, String.format("drop java function if exists %s", funName));
            String sql = String.format("create java function %s RETURN_TYPE BIGINT "
                + "CODE "
                + "public class %s extends UserDefinedJavaFunction "
                + "{             "
                + "     public Object compute(Object[] args) {       "
                + "         return 55;     "
                + "     }"
                + "} end_code", funName, StringUtils.funcNameToClassName(funName));
            Statement statement = JdbcUtil.createStatement(connection);
            try {
                statement.execute(sql);
            } catch (SQLException e) {
                if (e.getMessage().toLowerCase().contains("number of java udf reached")) {
                    return;
                }
            } finally {
                JdbcUtil.close(statement);
            }
        }
        Assert.fail("should reach max number of java udf");
    }

    @After
    public void clear() throws SQLException {
        JdbcUtils.execute(connection, "set global max_java_udf_num = default");
        for (int i = 0; i < 6; ++i) {
            String funName = FUNC_NAME + i;
            JdbcUtils.execute(connection, String.format("drop java function if exists %s", funName));
        }
    }
}

