package com.alibaba.polardbx.qatest.dql.auto.join;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JoinWithDynamicValueTest extends BaseTestCase {

    private static Connection connection;

    private static final String TABLE_NAME = "test_dynamic_value";

    private static final String SQL =
        "/*+TDDL: cmd_extra(IN_SUB_QUERY_THRESHOLD=5)*/ select * from %s where c1 in (%s) and c2 = 5;";

    @Before
    public void init() throws SQLException {
        connection = getPolardbxConnection();
        JdbcUtils.execute(connection, String.format("drop table if exists %s", TABLE_NAME));
        String createTable = String.format("create table %s " + "(\n"
            + "  `c1` int(11) DEFAULT NULL,\n"
            + "  `c2` int(11) DEFAULT NULL,\n"
            + "  KEY `auto_shard_key_c1` USING BTREE (`c1`)\n"
            + "  ) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 partition by hash(`c1`);", TABLE_NAME);
        JdbcUtils.execute(connection, createTable);
    }

    @Test
    public void testLittleValue() {
        String explainResult =
            getExplainResult(connection, String.format(SQL, TABLE_NAME,
                IntStream.range(0, 8).boxed().map(Object::toString).collect(Collectors.joining(",")))).toLowerCase();
        Assert.assertTrue(explainResult.contains("materializedsemijoin"),
            "plan should be materialized semi join");
    }

    @Test
    public void testMuchValue() {
        String explainResult =
            getExplainResult(connection, String.format(SQL, TABLE_NAME,
                IntStream.range(0, 80000).boxed().map(Object::toString)
                    .collect(Collectors.joining(",")))).toLowerCase();
        Assert.assertTrue(explainResult.contains("semihashjoin"),
            "plan should be semi hash join");
    }

    @After
    public void clear() throws SQLException {
        JdbcUtils.execute(connection, String.format("drop table if exists %s", TABLE_NAME));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}


