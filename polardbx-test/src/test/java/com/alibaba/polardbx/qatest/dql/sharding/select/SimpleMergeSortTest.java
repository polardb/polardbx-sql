package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.google.common.truth.Truth.assert_;

public class SimpleMergeSortTest extends ReadBaseTestCase {

    protected ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SimpleMergeSortTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    @Test
    public void testFixedOrderResult() {
        String sql = "/*+TDDL:PREFETCH_EXECUTE_POLICY=1*/select pk from " + baseOneTableName + " limit 10 ";
        ResultSet tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        List<List<Object>> firstResults = JdbcUtil.getAllResult(tddlRs, false);

        for (int i = 0; i < 9; i++) {
            tddlRs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
            List<List<Object>> testResults = JdbcUtil.getAllResult(tddlRs, false);
            assert_().that(testResults).containsExactlyElementsIn(firstResults).inOrder();
        }
    }

    @Test
    public void testVerifyTraceNum() {
        String sql = "/*+TDDL:PREFETCH_EXECUTE_POLICY=1*/select * from " + baseOneTableName + " limit 1 ";
        Assert.assertTrue(getTraceCount(sql) <= 2);
    }

    private int getTraceCount(String sql) {
        JdbcUtil.executeQuery(String.format("trace " + sql, baseOneTableName),
            tddlConnection);
        int traceCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuery("show trace", tddlConnection)) {
            while (rs.next()) {
                traceCount++;
            }
        } catch (SQLException e) {
            Assert.fail(e.getMessage());
        }
        return traceCount;
    }
}
