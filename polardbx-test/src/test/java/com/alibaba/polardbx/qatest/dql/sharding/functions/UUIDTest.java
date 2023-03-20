package com.alibaba.polardbx.qatest.dql.sharding.functions;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class UUIDTest extends ReadBaseTestCase {
    String SINGLE_TABLE_NAME = "test_uuid";
    String SHARD_TABLE_NAME = "test_uuid_shard";
    String DROP_TABLE_IF_EXISTS = "drop table if exists %s";
    String CREATE_TABLE = "create table %s (id int, data varchar(255))";
    String PARTITION_POLICY = " dbpartition by hash(id)";
    String INSERT_DATA = "insert into %s values (1, 'x'), (2, 'y'), (3, 'z')";
    String SELECT_FUNC_RESULT = "select id, uuid(), uuid_short() from %s";

    @Before
    public void initData() {
        prepareData(tddlConnection, SINGLE_TABLE_NAME, "");
        prepareData(tddlConnection, SHARD_TABLE_NAME, PARTITION_POLICY);
    }

    private void prepareData(Connection connection, String tableName, String partitionPolicy) {
        JdbcUtil.executeSuccess(connection, String.format(DROP_TABLE_IF_EXISTS, tableName));
        JdbcUtil.executeSuccess(connection, String.format(CREATE_TABLE + partitionPolicy, tableName));
        JdbcUtil.executeSuccess(connection, String.format(INSERT_DATA, tableName));
    }

    public void testProject(String tableName) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SELECT_FUNC_RESULT, tableName), tddlConnection)) {
            List<Pair<String, String>> uuidAndUuidShort = getUuidResult(rs);
            if (uuidAndUuidShort.size() != 3) {
                Assert.fail("select result size not expected");
            }
            Set<String> uuids = uuidAndUuidShort.stream().map(t -> t.getKey()).collect(Collectors.toSet());
            if (uuids.size() != 3) {
                Assert.fail("uuid should be unique, but not");
            }
            Set<String> uuidShorts = uuidAndUuidShort.stream().map(t -> t.getValue()).collect(Collectors.toSet());
            if (uuidShorts.size() != 3) {
                Assert.fail("uuid_short should be unique, but not");
            }
        }
    }

    private List<Pair<String, String>> getUuidResult(ResultSet rs) throws SQLException {
        List<Pair<String, String>> res = new ArrayList<>();
        while (rs.next()) {
            res.add(Pair.of(rs.getString("uuid()"), rs.getString("uuid_short()")));
        }
        return res;
    }

    @Test
    public void testShardTable() throws SQLException {
        testProject(SHARD_TABLE_NAME);
    }

    @Test
    public void testSingleTable() throws SQLException {
        testProject(SINGLE_TABLE_NAME);
    }
}
