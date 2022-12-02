/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

public class IndexLookupTest extends ReadBaseTestCase {

    enum Sharding {
        DB_ONLY("index_lookup_test_a",
            "dbpartition by HASH(a)"),
        DB_TB_SAME_COLUMN("index_lookup_test_a_a",
            "dbpartition by HASH(a) tbpartition by HASH(a) tbpartitions 2"),
        DB_TB_DIFF_COLUMN("index_lookup_test_a_b",
            "dbpartition by HASH(a) tbpartition by HASH(b) tbpartitions 2"),
        // Uncomment the following code once this aone is fixed
        // https://work.aone.alibaba-inc.com/issue/41325609
//        DB_RANGE_HASH("index_lookup_test_ab",
//            "dbpartition by RANGE_HASH(a, b, 1)"),
//        DB_TB_SAME_RANGE_HASH("index_lookup_test_ab_ab",
//            "dbpartition by RANGE_HASH(a, b, 1) tbpartition by RANGE_HASH(a, b, 1) tbpartitions 2"),
        TB_ONLY("index_lookup_test_x_a",
            "tbpartition by HASH(a) tbpartitions 2");

        final String table;
        final String topology;

        Sharding(String table, String topology) {
            this.table = table;
            this.topology = topology;
        }

        boolean isRangeHash() {
            return topology.contains("RANGE_HASH");
        }
    }

    enum WhereCond {
        ALL("true"),
        POINT("pk = '123'"),
        POINT_NULL("pk = '003'"),
        RANGE("pk between 200 and 277");

        final String condition;

        WhereCond(String condition) {
            this.condition = condition;
        }
    }

    private static final String CREATE_TABLE = "create table %s ( \n"
        + "    pk char(10) not null,\n"
        + "    a int,\n"
        + "    b int,\n"
        + "    c int,\n"
        + "    primary key(pk),\n"
        + "    global index idx_on_%s (a, b) dbpartition by HASH(a)"
        + ") %s";

    private static final String CREATE_TABLE_MYSQL = "create table %s ( \n"
        + "    pk char(10) not null,\n"
        + "    a int,\n"
        + "    b int,\n"
        + "    c int,\n"
        + "    primary key(pk),\n"
        + "    index idx_on_%s (a, b)"
        + ")";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String QUERY = "/*+TDDL:LOOKUP_IN_VALUE_LIMIT=10 */ "
        + "select * from ${table_name} force index ( idx_on_${table_name} ) where ${where_cond}";

    private final Sharding sharding;
    private final WhereCond whereCond;
    private final boolean inTrans;

    public IndexLookupTest(Sharding sharding, WhereCond whereCond, boolean inTrans) {
        this.sharding = sharding;
        this.whereCond = whereCond;
        this.inTrans = inTrans;
    }

    @Parameters(name = "{index}:{0},{1},{2}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(Sharding.values(),
            WhereCond.values(),
            new Object[] {true, false});
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Create tables on DRDS
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            try (Statement stmt = tddlConnection.createStatement()) {
                for (Sharding sharding : Sharding.values()) {
                    stmt.executeUpdate(String.format(CREATE_TABLE, sharding.table, sharding.table, sharding.topology));
                    stmt.executeUpdate(buildInsertTable(sharding.table));
                }
            }
        }

        // Create tables on MySQL
        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            try (Statement stmt = mysqlConnection.createStatement()) {
                for (Sharding sharding : Sharding.values()) {
                    stmt.executeUpdate(String.format(CREATE_TABLE_MYSQL, sharding.table, sharding.table));
                    stmt.executeUpdate(buildInsertTable(sharding.table));
                }
            }
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Drop tables on DRDS
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            try (Statement stmt = tddlConnection.createStatement()) {
                for (Sharding sharding : Sharding.values()) {
                    stmt.executeUpdate(String.format(DROP_TABLE, sharding.table));
                }
            }
        }

        // Drop tables on MySQL
        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            try (Statement stmt = mysqlConnection.createStatement()) {
                for (Sharding sharding : Sharding.values()) {
                    stmt.executeUpdate(String.format(DROP_TABLE, sharding.table));
                }
            }
        }
    }

    @Test
    public void testLookupJoin() throws SQLException {
        if (usingNewPartDb()) {
            return;
        }
        String query = buildQuery();

//        System.out.println(query); // DEBUG

        if (inTrans) {
            tddlConnection.setAutoCommit(false);
        }

        selectContentSameAssertWithDiffSql("trace " + query, query, null,
            mysqlConnection, tddlConnection, true, false, false);

        int countLookup = 0;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show trace")) {
            while (rs.next()) {
                if (rs.getString("STATEMENT").contains("idx_on_index_lookup_test")) {
                    continue; // Skip table scan on index table
                }
                countLookup++;
                int rows = rs.getInt("ROWS");
                String statement = rs.getString("STATEMENT");
                Assert.assertFalse("bka_magic should be replaced", statement.contains("bka_magic"));
                if (!(statement.contains("NULL")
                    || sharding.isRangeHash())) {
                    Assert.assertTrue("should not see empty lookup results after pruning", rows != 0);
                }
            }
        }

        if (inTrans) {
            tddlConnection.commit();
            tddlConnection.setAutoCommit(true);
        }
    }

    private static List<Object[]> cartesianProduct(Object[]... arrays) {
        List[] lists = Arrays.stream(arrays)
            .map(Arrays::asList)
            .toArray(List[]::new);
        List<List<Object>> result = Lists.cartesianProduct(lists);
        return result.stream()
            .map(List::toArray)
            .collect(Collectors.toList());
    }

    private static String buildInsertTable(String tableName) {
        return "insert into " + tableName + "(a, b, c, pk) values " + LookupJoinTest.buildInsertValues(true, false);
    }

    private String buildQuery() {
        Map<String, String> variables = ImmutableMap.<String, String>builder()
            .put("where_cond", whereCond.condition)
            .put("table_name", sharding.table)
            .build();
        String text = QUERY;
        for (Map.Entry<String, String> e : variables.entrySet()) {
            text = DateUtils.replaceAll(text, "${" + e.getKey() + "}", e.getValue());
        }
        return text;
    }
}
