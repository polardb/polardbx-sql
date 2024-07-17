package com.alibaba.polardbx.qatest.NotThreadSafe.udf.java;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.utils.StringUtils;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JavaUdfConcurrentTest extends BaseTestCase {
    private static final int RUN_TIME = 10_000;

    private static final String JAVA_UDF_NAME = "my_concat";

    public class QueryTask implements Runnable {
        private final String query;
        private final Object result;

        public Exception ex;

        public QueryTask(String query, Object result) {
            this.query = query;
            this.result = result;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();
            try (Connection conn = JavaUdfConcurrentTest.this.getPolardbxConnection()) {
                Statement statement = conn.createStatement();
                while (System.currentTimeMillis() - startTime < RUN_TIME * 1000) {
                    String select = RandomUtils.getBoolean() ? "/* +TDDL: cmd_extra()*/" + query : query;
                    ResultSet rs = statement.executeQuery(select);
                    if (!rs.next() || !rs.getObject(1).equals(result)) {
                        throw new RuntimeException("execute result not matched");
                    }
                    rs.close();
                    Thread.sleep(10);
                }
                statement.close();
            } catch (InterruptedException | SQLException e) {
                ex = e;
                throw new RuntimeException("execute failed" + e.getMessage());
            }
        }
    }

    @Test
    public void testConcurrentTasks() throws InterruptedException {
        QueryTask normalFuncTask = new QueryTask("select concat('test', 12)", "test12");
        Thread queryNormalFunction = new Thread(normalFuncTask);
        QueryTask udfTask = new QueryTask(String.format("select %s('test', 12)", JAVA_UDF_NAME), "test_polarx_12");
        Thread queryUdf = new Thread(udfTask);
        Thread functionThread = new Thread(() -> {
            try (Connection conn = getPolardbxConnection()) {
                Statement statement = conn.createStatement();
                while (true) {
                    // 删除用户自定义函数
                    statement.execute("DROP JAVA FUNCTION IF EXISTS my_function");
                    Thread.sleep(50);
                    // 创建用户自定义函数
                    statement.execute("CREATE JAVA FUNCTION my_function no state RETURN_TYPE BIGINT "
                        + "CODE "
                        + "public class My_function extends UserDefinedJavaFunction {             "
                        + "public Object compute(Object[] args) {          "
                        + " return 31;     "
                        + " } "
                        + "} "
                        + "END_CODE");
                    Thread.sleep(50);
                    statement.execute("select my_function()");
                }
            } catch (SQLException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        queryNormalFunction.start();
        queryUdf.start();
        functionThread.start();

        Thread.sleep(RUN_TIME);
        Assert.assertTrue(normalFuncTask.ex == null);
        Assert.assertTrue(udfTask.ex == null);
    }

    @Before
    public void initFunction() {
        try {
            JdbcUtils.execute(getPolardbxConnection(), "drop java function if exists " + JAVA_UDF_NAME);
            JdbcUtils.execute(getPolardbxConnection(),
                String.format("CREATE JAVA FUNCTION `%s` return_type varchar(255) input_types varchar(255), int\n"
                    + "code\n"
                    + "public class %s extends UserDefinedJavaFunction {\n"
                    + "public Object compute(Object[] args) {\n"
                    + "Integer a = (Integer) args[1];\n"
                    + "String b = (String) args[0];\n"
                    + "return  b + \"_polarx_\" + a;\n"
                    + "}\n"
                    + "};\n"
                    + "end_code;", JAVA_UDF_NAME, StringUtils.funcNameToClassName(JAVA_UDF_NAME)));
        } catch (SQLException ex) {
        }
    }
}
