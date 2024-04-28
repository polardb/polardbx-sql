package com.alibaba.polardbx.qatest.NotThreadSafe.udf.sql;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Purpose: test other dml/dql case work correct concurrently under modification of udf
 */
public class RandomUdfTest extends CrudBasedLockTestCase {
    private static final int ROUND = 1000;
    private static final String DROP_FUNCTION = "drop function if exists %s";
    private static final String CREATE_FUNCTION =
        "create function %s(data char(255)) returns char(255) %s return data;";
    private static final String SELECT_RESULT = "select %s('%s')";

    @Test
    public void testRandomFunc() throws SQLException {
        for (int i = 0; i < ROUND; ++i) {
            String functionName = RandomUtils.getStringWithPrefix(24, "random_udf_test_");
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, functionName));
            JdbcUtil.executeSuccess(tddlConnection,
                String.format(CREATE_FUNCTION, functionName, RandomUtils.getBoolean() ? "" : "no sql"));
            try (ResultSet rs = JdbcUtil.executeQuery(String.format(SELECT_RESULT, functionName, functionName),
                tddlConnection);) {
                if (!rs.next()) {
                    Assert.fail("select result should not be empty");
                }
                if (!functionName.equals(rs.getString(1))) {
                    Assert.fail(
                        String.format("select result not expect, expect %s, but %s", functionName, rs.getString(1)));
                }
            }
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_FUNCTION, functionName));
        }
    }
}
