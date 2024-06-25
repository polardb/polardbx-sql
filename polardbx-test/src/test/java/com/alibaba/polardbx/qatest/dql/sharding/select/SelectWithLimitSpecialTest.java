package com.alibaba.polardbx.qatest.dql.sharding.select;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.truth.Truth;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class SelectWithLimitSpecialTest extends ReadBaseTestCase {

    @Parameters(name = "{index}:table={0}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneTable());
    }

    public SelectWithLimitSpecialTest(String baseOneTableName) {
        this.baseOneTableName = baseOneTableName;
    }

    BigInteger[] bigIntegers = {
        new BigInteger("-18446744073709551615"), new BigInteger("18446744073709551615"),
        new BigInteger("0"), new BigInteger("-1"), new BigInteger("10")};

    @Test
    public void singleTest() throws Exception {
        if (!useXproto(tddlConnection)) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + baseOneTableName);
        String sqlTemp = "select * from " + baseOneTableName + " order by pk limit %s";
        for (BigInteger bigInteger : bigIntegers) {
            String sql = String.format(sqlTemp, bigInteger.toString());
            if (bigInteger.signum() < 0) {
                if (multi(baseOneTableName)) {
                    JdbcUtil.executeFailed(tddlConnection, sql, "optimize error by get rex");
                } else {
                    JdbcUtil.executeFailed(tddlConnection, sql, "You have an error in your SQL syntax");
                }
            }
            if (bigInteger.signum() == 0) {
                if (multi(baseOneTableName)) {
                    JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                    emptyTrace();
                } else {
                    JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                    traceParameter("0");
                }
            }
            if (bigInteger.signum() > 0) {
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    @Test
    public void doubleTest() throws Exception {
        if (!useXproto(tddlConnection)) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "analyze table " + baseOneTableName);
        String sqlTemp = "select * from " + baseOneTableName + " order by pk limit %s,%s";
        for (BigInteger bigInteger1 : bigIntegers) {
            for (BigInteger bigInteger2 : bigIntegers) {
                String sql = String.format(sqlTemp, bigInteger1.toString(), bigInteger2.toString());
                if (bigInteger1.signum() < 0 || bigInteger2.signum() < 0) {
                    if (multi(baseOneTableName)) {
                        JdbcUtil.executeFailed(tddlConnection, sql, "optimize error by get rex");
                    } else {
                        JdbcUtil.executeFailed(tddlConnection, sql, "You have an error in your SQL syntax");
                    }
                    continue;
                }
                if (bigInteger2.signum() == 0) {
                    if (multi(baseOneTableName)) {
                        JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                        emptyTrace();
                    } else {
                        JdbcUtil.executeQuery("trace " + sql, tddlConnection);
                        traceParameter("0");
                    }
                    continue;
                }
                selectContentSameAssert(sql, null, mysqlConnection, tddlConnection, true);
            }
        }
    }

    private void traceParameter(String para) throws SQLException {
        String sql = "show trace";
        try (ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection)) {
            if (rs.next()) {
                String params = rs.getString("PARAMS");
                Truth.assertThat(params).contains(para);
                return;
            }
        }
        assertWithMessage("show trace fails to find " + para).fail();
    }

    private void emptyTrace() {
        String sql = "show trace";
        Truth.assertThat(JdbcUtil.getAllResult(JdbcUtil.executeQuery(sql, tddlConnection))).isEmpty();
    }

    private boolean multi(String tableName) {
        return tableName.contains("multi");
    }

    private boolean broadcast(String tableName) {
        return tableName.contains("broadcast");
    }
}
