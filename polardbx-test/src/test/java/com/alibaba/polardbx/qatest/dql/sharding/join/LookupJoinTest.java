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
import net.jcip.annotations.NotThreadSafe;
import org.apache.calcite.util.Util;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

import static com.alibaba.polardbx.qatest.dql.sharding.join.JoinUtils.cartesianProduct;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

@NotThreadSafe
public class LookupJoinTest extends ReadBaseTestCase {

    enum JoinEquiPred {
        ONE_COLUMN("F.a = D.a", "a", "D.b = 1 and D.c = 1"),
        TWO_COLUMNS("F.a = D.a AND F.b = D.b", "a, b", "D.c = 1"),
        THREE_COLUMNS("F.a = D.a AND F.b = D.b AND F.c = D.c", "a, b, c", "true");

        final String condition;
        final String columns;
        final String maxOneRowCondition;

        JoinEquiPred(String condition, String columns, String maxOneRowCondition) {
            this.condition = condition;
            this.columns = columns;
            this.maxOneRowCondition = maxOneRowCondition;
        }

        public boolean isMultiColumn() {
            return this != ONE_COLUMN;
        }
    }

    enum Sharding {
        DB_ONLY("dim_table_lj",
            "dbpartition by HASH(a)"),
        DB_TB_SAME_COLUMN("dim_table_lj_a",
            "dbpartition by HASH(a) tbpartition by HASH(a) tbpartitions 2"),
        DB_TB_DIFF_COLUMN("dim_table_lj_b",
            "dbpartition by HASH(A) tbpartition by HASH(B) tbpartitions 2"),
        // Uncomment the following code once this aone is fixed
        // https://work.aone.alibaba-inc.com/issue/41325609
//        DB_RANGE_HASH("dim_table_lj_ab",
//            "dbpartition by RANGE_HASH(a, b, 1)"),
//        DB_TB_SAME_RANGE_HASH("dim_table_lj_ab_ab",
//            "dbpartition by RANGE_HASH(A, b, 1) tbpartition by RANGE_HASH(a, b, 1) tbpartitions 2"),
        TB_ONLY("dim_table_lj_x_a",
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

    enum JoinType {
        INNER_JOIN(
            "select * from ${fact_table} F inner join ${dim_table} D on ${join_cond} where ${where_cond}",
            "BKA_JOIN(${fact_table}, ${dim_table})"),
        LEFT_JOIN(
            "select * from ${fact_table} F left join ${dim_table} D on ${join_cond} where ${where_cond}",
            "BKA_JOIN(${fact_table}, ${dim_table})"),
        RIGHT_JOIN(
            "select * from ${dim_table} D right join ${fact_table} F on ${join_cond} where ${where_cond}",
            "BKA_JOIN(${dim_table}, ${fact_table})"),
        MATERIALIZED_SEMI_JOIN(
            "select * from ${dim_table} where (${join_keys}) in (select ${join_keys} from ${fact_table} where ${where_cond})",
            "MATERIALIZED_SEMI_JOIN(${dim_table}, ${fact_table})"),
        MATERIALIZED_ANTI_JOIN(
            "select * from ${dim_table} where (${join_keys}) not in (select ${join_keys} from ${fact_table} where ${where_cond})",
            "MATERIALIZED_SEMI_JOIN(${dim_table}, ${fact_table})"),
        SEMI_BKA_JOIN(
            "select * from ${fact_table} where (${join_keys}) in (select ${join_keys} from ${dim_table}) and ${where_cond}",
            "SEMI_BKA_JOIN(${fact_table}, ${dim_table})"),
        LEFT_SINGLE_JOIN(
            "select *, (select val from ${dim_table} D where ${join_cond} and ${max_one_row_cond}) val from ${fact_table} F where ${where_cond}",
            "SEMI_BKA_JOIN(${fact_table}, ${dim_table})");

        final String query;
        final String joinHint;

        JoinType(String query, String joinHint) {
            this.query = query;
            this.joinHint = joinHint;
        }

        public boolean isAntiJoin() {
            return this == MATERIALIZED_ANTI_JOIN;
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

    private static final String FACT_TABLE_NAME = "fact_table_lj";

    private static final String CREATE_FACT_TABLE = "create table if not exists " + FACT_TABLE_NAME + " ( \n"
        + "    PK char(10) not null,\n"
        + "    A int,\n"
        + "    B int,\n"
        + "    C int,\n"
        + "    primary key(PK)\n"
        + ")";

    private static final String CREATE_DIM_TABLE = "create table if not exists %s (\n"
        + "    A int not null,\n"
        + "    B int not null,\n"
        + "    C int not null,\n"
        + "    VAL char(10),\n"
        + "    primary key(A,B,C)\n"
        + ") %s";

    private static final String DROP_TABLE = "drop table if exists %s";

    private static final String HINT_TEMPLATE = "/*+TDDL:%s LOOKUP_IN_VALUE_LIMIT=10 ENABLE_JOIN_CLUSTERING=false */";

    private final JoinEquiPred joinEquiPred;
    private final Sharding sharding;
    private final JoinType joinType;
    private final WhereCond whereCond;
    private final boolean inTrans;

    public LookupJoinTest(JoinEquiPred joinEquiPred, Sharding sharding, JoinType joinType, WhereCond whereCond,
                          boolean inTrans) {
        this.joinEquiPred = joinEquiPred;
        this.sharding = sharding;
        this.joinType = joinType;
        this.whereCond = whereCond;
        this.inTrans = inTrans;
    }

    @Parameters(name = "{index}:{0},{1},{2},{3},{4}")
    public static List<Object[]> getParameters() {
        return cartesianProduct(JoinEquiPred.values(),
            Sharding.values(),
            JoinType.values(),
            WhereCond.values(),
            new Object[] {true, false});
    }

    @BeforeClass
    public static void beforeClass() throws Exception {
        // Create tables on DRDS
        try (Connection tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.addBatch(CREATE_FACT_TABLE);
                for (Sharding sharding : Sharding.values()) {
                    stmt.addBatch(String.format(CREATE_DIM_TABLE, sharding.table, sharding.topology));
                }
                stmt.executeBatch();
            }

            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.addBatch(buildInsertFactTable());
                for (Sharding sharding : Sharding.values()) {
                    stmt.addBatch(buildInsertDimTable(sharding.table, sharding.isRangeHash()));
                }
                stmt.executeBatch();
            }
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            // Create tables on MySQL
            try (Statement stmt = mysqlConnection.createStatement()) {
                stmt.addBatch(CREATE_FACT_TABLE);
                stmt.addBatch(buildInsertFactTable());
                for (Sharding sharding : Sharding.values()) {
                    stmt.addBatch(String.format(CREATE_DIM_TABLE, sharding.table, ""));
                    stmt.addBatch(buildInsertDimTable(sharding.table, sharding.isRangeHash()));
                }
                stmt.executeBatch();
            }
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        // Drop tables on DRDS
        try (Connection tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.addBatch(String.format(DROP_TABLE, FACT_TABLE_NAME));
                for (Sharding sharding : Sharding.values()) {
                    stmt.addBatch(String.format(DROP_TABLE, sharding.table));
                }
                stmt.executeBatch();
            }
        }

        // Drop tables on MySQL
        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            try (Statement stmt = mysqlConnection.createStatement()) {
                stmt.addBatch(String.format(DROP_TABLE, FACT_TABLE_NAME));
                for (Sharding sharding : Sharding.values()) {
                    stmt.addBatch(String.format(DROP_TABLE, sharding.table));
                }
                stmt.executeBatch();
            }
        }

    }

    @Test
    public void testLookupJoin() throws SQLException {
        if (usingNewPartDb()) {
            return;
        }
        if (joinEquiPred.isMultiColumn() && joinType.isAntiJoin()) {
            return;
        }
        String query = buildQuery();

//        System.out.println(query); // DEBUG

        if (inTrans) {
            tddlConnection.setAutoCommit(false);
        }

        selectContentSameAssertWithDiffSql("trace " + query, query, null,
            mysqlConnection, tddlConnection, true, false, false);

        int countLookup;
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show trace")) {
            countLookup = -1; // minus the query to fact table
            while (rs.next()) {
                countLookup++;
                int rows = rs.getInt("ROWS");
                String statement = rs.getString("STATEMENT");
                if (joinType != JoinType.MATERIALIZED_ANTI_JOIN) {
                    Assert.assertFalse("bka_magic should be replaced", statement.contains("bka_magic"));
                }
                if (!(statement.contains("NULL")
                    || joinType == JoinType.MATERIALIZED_ANTI_JOIN
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

    private static String buildInsertFactTable() {
        return "insert into " + FACT_TABLE_NAME + "(a, b, c, pk) values " + buildInsertValues(true, false);
    }

    private static String buildInsertDimTable(String table, boolean rangeHash) {
        return "insert into " + table + "(a, b, c, val) values " + buildInsertValues(false, rangeHash);
    }

    private static final int MAX_NUMBER = 5;

    public static String buildInsertValues(boolean withNull, boolean rangeHash) {
        List<Integer> numbers = new ArrayList<>();
        for (int n = 1; n <= MAX_NUMBER; n++) {
            numbers.add(n);
        }
        if (withNull) {
            numbers.add(null);
        } else {
            numbers.add(0);
        }

        StringJoiner joiner = new StringJoiner(", ");
        if (!rangeHash) {
            for (Integer a : numbers) {
                for (Integer b : numbers) {
                    for (Integer c : numbers) {
                        String name = String.format("%s%s%s", Util.first(a, 0), Util.first(b, 0), Util.first(c, 0));
                        joiner.add(String.format("(%s,%s,%s,'%s')", a, b, c, name));
                    }
                }
            }
        } else {
            for (Integer ab : numbers) {
                for (Integer c : numbers) {
                    String name = String.format("%s%s%s", Util.first(ab, 0), Util.first(ab, 0), Util.first(c, 0));
                    joiner.add(String.format("(%s,%s,%s,'%s')", ab, ab, c, name));
                }
            }
        }
        return joiner.toString();
    }

    private String buildQuery() {
        String text = String.format(HINT_TEMPLATE, joinType.joinHint) + joinType.query;
        Map<String, String> variables = ImmutableMap.<String, String>builder()
            .put("fact_table", FACT_TABLE_NAME)
            .put("dim_table", sharding.table)
            .put("join_cond", joinEquiPred.condition)
            .put("where_cond", whereCond.condition)
            .put("join_keys", joinEquiPred.columns)
            .put("max_one_row_cond", joinEquiPred.maxOneRowCondition)
            .build();
        for (Map.Entry<String, String> e : variables.entrySet()) {
            text = DateUtils.replaceAll(text, "${" + e.getKey() + "}", e.getValue());
        }
        return text;
    }
}
