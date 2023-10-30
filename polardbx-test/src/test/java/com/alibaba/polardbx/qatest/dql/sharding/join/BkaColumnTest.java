package com.alibaba.polardbx.qatest.dql.sharding.join;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.mysqlDBName1;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssertWithDiffSql;

public class BkaColumnTest extends ReadBaseTestCase {
    protected static final String CREATE_TABLE = "create table %s (id int not null, %s)";
    protected static final String PART_DEF = " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2";
    protected static final String DROP_TABLE = "drop table if exists %s";
    protected static String ADD_INDEX = "alter table %s add global index %s(%s) covering(%s) dbpartition by hash(%s)";
    protected static String INSERT_DATA = "insert into %s values %s";

    protected static final String FORBID_OTHER_SEMI_HINT =
        "ENABLE_MATERIALIZED_SEMI_JOIN= false ENABLE_SEMI_SORT_MERGE_JOIN=false ENABLE_SEMI_HASH_JOIN=false ENABLE_SEMI_NL_JOIN=false";
    protected static final String ANTI_BKA_HINT =
        "/*+TDDL: cmd_extra(ENABLE_CBO=true) " + FORBID_OTHER_SEMI_HINT + " SEMI_BKA_JOIN( %s, %s)*/";

    private static final String[] TYPES = new String[] {
        "tinyint", "int", "bigint", "float"
    };

    private static final String TABLE_NAME_1 = "test_bka_trace_1";
    private static final String TABLE_NAME_2 = "test_bka_trace_2";

    @Test
    public void innerTableWithPredicate() {
        String hint = String.format(ANTI_BKA_HINT, TABLE_NAME_1, TABLE_NAME_2);
        String tddlSql = String.format(
            "select c1 from %s where (c1) in (select c1 from %s force index(%s) where c2 >= 10 ) and id = 1",
            TABLE_NAME_1, TABLE_NAME_2, TABLE_NAME_2 + "_g_c1");
        String mysqlSql = String.format(
            "select c1 from %s where (c1) in (select c1 from %s where c2 >= 10 ) and id = 1",
            TABLE_NAME_1, TABLE_NAME_2);
        selectContentSameAssertWithDiffSql("trace " + hint + tddlSql, mysqlSql, null,
            mysqlConnection, tddlConnection, true, false, false);
        String explain = getExplainResult(tddlConnection, hint + tddlSql);
        Assert.assertTrue(explain.toLowerCase().contains("test_bka_trace_2_g_c1"),
            "plan should contain access to index table");
        Assert.assertTrue(explain.replaceAll("test_bka_trace_2_g_c1", "").toLowerCase().contains("test_bka_trace_2"),
            "plan should contain access to primary table");
    }

    @Ignore
    public void floatTypeNotAppearInLookupCol() throws SQLException {
        String hint = String.format(ANTI_BKA_HINT, TABLE_NAME_1, TABLE_NAME_2);
        String sql = String.format(
            "select c1 from %s where (c1, c4) in (select c1, c4 from %s where c2 >= 10 ) and id = 1",
            TABLE_NAME_1, TABLE_NAME_2);
        selectContentSameAssertWithDiffSql("trace " + hint + sql, sql, null,
            mysqlConnection, tddlConnection, true, false, false);
        List<String> physicalSqls = getPhysicalSqls(tddlConnection);
        Assert.assertTrue(physicalSqls.stream().filter(phySql -> phySql.toLowerCase().contains("test_bka_trace_2"))
                .allMatch(phySql -> phySql.toLowerCase().contains("`c1` in (")),
            "all physical sql should not contain float type column");
    }

    @BeforeClass
    public static void init() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            createTable(tddlConnection, true);
            addGlobalIndex(TABLE_NAME_2, TABLE_NAME_2 + "_g_c1", "c1", "id", "c1", tddlConnection);
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            createTable(mysqlConnection, false);
        }

        List<String> data = Arrays.asList("3, -1, 2, 30000, 100", "2, -1, 2, 30000, 100", "1, -99, 34, 38000, 100");
        insertData(TABLE_NAME_1, data);
        data = Arrays.asList("1, -99, 34, 38000, 100");
        insertData(TABLE_NAME_2, data);
    }

    private static void createTable(Connection connection, boolean isTddl) {
        JdbcUtil.executeSuccess(connection, String.format(DROP_TABLE, TABLE_NAME_1));
        JdbcUtil.executeSuccess(connection, String.format(DROP_TABLE, TABLE_NAME_2));

        String columnDefs = IntStream.range(0, TYPES.length).boxed()
            .map((i) -> String.format("c%s %s ", i + 1, TYPES[i])).collect(Collectors.joining(","));
        JdbcUtil.executeSuccess(connection,
            String.format(isTddl ? CREATE_TABLE + PART_DEF : CREATE_TABLE, TABLE_NAME_1, columnDefs));
        JdbcUtil.executeSuccess(connection,
            String.format(isTddl ? CREATE_TABLE + PART_DEF : CREATE_TABLE, TABLE_NAME_2, columnDefs));
    }

    private static void addGlobalIndex(String tableName, String indexName, String indexColumn, String coveringColumn,
                                       String partitionColumn, Connection tddlConnection) {
        JdbcUtil.executeSuccess(tddlConnection,
            String.format(ADD_INDEX, tableName, indexName, indexColumn, coveringColumn, partitionColumn));
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_1));
            JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME_2));
        }
    }

    public static void insertData(String tableName, List<String> records) throws SQLException {
        try (Connection tddlConnection = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(tddlConnection, polardbXShardingDBName1());
            JdbcUtil.executeSuccess(tddlConnection,
                String.format(INSERT_DATA, tableName, StringUtils.join(records.stream().map(t -> "(" + t + ")").collect(
                    Collectors.toList()), ",")));
        }

        try (Connection mysqlConnection = ConnectionManager.getInstance().getDruidMysqlConnection()) {
            JdbcUtil.useDb(mysqlConnection, mysqlDBName1());
            JdbcUtil.executeSuccess(mysqlConnection,
                String.format(INSERT_DATA, tableName, StringUtils.join(records.stream().map(t -> "(" + t + ")").collect(
                    Collectors.toList()), ",")));
        }
    }

    public static List<String> getPhysicalSqls(Connection tddlConnection) throws SQLException {
        List<String> physicalSqls = new ArrayList<>();
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("show trace")) {
            while (rs.next()) {
                physicalSqls.add(rs.getString("STATEMENT"));
            }
        }
        return physicalSqls;
    }
}
