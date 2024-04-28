package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JoinWithCollationTest extends PartitionTestBase {
    private static final String
        MPP_HINT =
        "ENABLE_BKA_JOIN=FALSE ENABLE_BROADCAST_JOIN=FALSE ENABLE_MPP=TRUE ENABLE_MASTER_MPP=TRUE WORKLOAD_TYPE=AP MPP_NODE_SIZE=2 ENABLE_PUSH_JOIN=FALSE ENABLE_CBO_PUSH_JOIN=FALSE";

    private static final String TABLE_NAME_1 = "test_join_collation_1";

    private static final String TABLE_NAME_2 = "test_join_collation_2";

    private static final String CREATE_TABLE = "create table %s (c1 char(255), c2 char(255) ) partition by hash(c1)";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String INSERT_DATA = "insert into %s values ('%s', '%s')";

    @Before
    public void prepare() {
        dropTable();
        createTable();
        insertData();
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_2));
    }

    private void createTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME_1));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME_2));
    }

    private void insertData() {
        // insert lower case letter into table 1
        for (char c = 'a'; c <= 'z'; c++) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME_1, c, c));
        }
        // insert upper case letter into table 2
        for (char c = 'A'; c <= 'Z'; c++) {
            JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME_2, c, c));
        }
    }

    @Test
    public void testJoinWithCollation() throws SQLException {
        String hint = String.format("/*+TDDL: %s */", MPP_HINT);
        String sql =
            String.format("select count(*) from %s t1 join %s t2 where t1.c1=t2.c1", TABLE_NAME_1, TABLE_NAME_2);
        try (ResultSet rs = JdbcUtil.executeQuery(hint + sql, tddlConnection)) {
            if (rs.next()) {
                long matched = rs.getLong(1);
                Assert.assertTrue(matched == ('z' - 'a' + 1), "matched rows not correct, matched is " + matched);
            } else {
                org.junit.Assert.fail("get empty result, unexpected");
            }
        }
    }
}
