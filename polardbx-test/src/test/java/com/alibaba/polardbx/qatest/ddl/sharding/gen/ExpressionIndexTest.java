package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ExpressionIndexTest extends DDLBaseNewDBTestCase {
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    private static final String ENABLE_CREATE_EXPRESSION_INDEX = "ENABLE_CREATE_EXPRESSION_INDEX=TRUE";
    private static final String ENABLE_UNIQUE_KEY_ON_GEN_COL = "ENABLE_UNIQUE_KEY_ON_GEN_COL=TRUE";

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Test
    public void testCreateFailed() {
        String tableName = "expr_create_fail_tbl";

        String create = String.format(
            "create table %s (a int primary key, b int, c int, d varchar(64), e varchar(64)) dbpartition by hash(a)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String hint = buildCmdExtra(ENABLE_CREATE_EXPRESSION_INDEX);

        // Do not enable expression index by default
        String indexName = tableName + "_idx_1";
        String indexColumns = "(a+1)";
        List<String> ddl = generatedDdl(tableName, "local", indexName, indexColumns, "");

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, ddl.get(0)).contains("is not enabled"));

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection, ddl.get(1)).contains("is not enabled"));

        // Do not support global expression index
        indexName = tableName + "_idx_2";
        indexColumns = "(a+1),b";
        ddl = generatedDdl(tableName, "global", indexName, indexColumns, "dbpartition by hash(b)");

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, hint + ddl.get(0)).contains("not support yet"));

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, hint + ddl.get(1)).contains("not support yet"));

        // Do not enable unique expression index by default
        indexName = tableName + "_idx_3";
        indexColumns = "(a+1)";
        ddl = generatedDdl(tableName, "local unique", indexName, indexColumns, "");

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, hint + ddl.get(0)).contains("is not enabled"));

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        Assert.assertTrue(
            JdbcUtil.executeUpdateFailedReturn(tddlConnection, hint + ddl.get(1)).contains("is not enabled"));
    }

    @Test
    public void testCreateSucc() {
        String tableName = "expr_index_create_tbl";
        String indexName = tableName + "_idx";
        String create =
            String.format("create table %s (a int primary key, b int, c int, d varchar(64), e varchar(64))", tableName);

        String[] partDefs = new String[] {"single", "broadcast", "dbpartition by hash(a)"};
        String[] indexColumns =
            new String[] {
                "substr(d,-2)",
                "concat(d,e)",
                "10+10",
                "(a+1) desc,b,c-1,substr(d,-2) asc,a+b+c*2",
                "a+1",
                "(a+1) desc",
                "(a+1),b",
                "d(20)"};
        String[][] columnInfos = new String[][] {
            new String[] {"$0", "substr(`d`,-(2))"},
            new String[] {"$0", "concat(`d`,`e`)"},
            new String[] {"$0", "(10 + 10)"},
            new String[] {
                "$0", "(`a` + 1)", "$2", "(`c` - 1)", "$3", "substr(`d`,-(2))", "$4", "((`a` + `b`) + (`c` * 2))"},
            new String[] {"$0", "(`a` + 1)"},
            new String[] {"$0", "(`a` + 1)"},
            new String[] {"$0", "(`a` + 1)"},
            new String[] {},
        };

        for (String partDef : partDefs) {
            for (int i = 0; i < indexColumns.length; i++) {
                testCreateExprIndexInternal(tableName, indexName, create, partDef, indexColumns[i], columnInfos[i],
                    false);
                testCreateExprIndexInternal(tableName, indexName, create, partDef, indexColumns[i], columnInfos[i],
                    true);
            }
        }
    }

    @Test
    public void testSpecialIndexName() {
        String tableName = "expr_special_index_tbl";
        String create =
            String.format("create table %s (a int primary key, b int, c int, d varchar(64), e varchar(64))", tableName);

        String indexColumns = "(a+1) desc,b,c-1,substr(d,-2) asc,a+b+c*2";
        String[] columnInfos = new String[] {
            "$0", "(`a` + 1)", "$2", "(`c` - 1)", "$3", "substr(`d`,-(2))", "$4", "((`a` + `b`) + (`c` * 2))"};
        String[] indexNames = new String[] {
            "c", "`c`", "```c```", "`c```", "`3`", "`\"f\"`",
            "this_is_a_index_with_a_very_looooooooooooooooooooong_index_name",};

        for (String indexName : indexNames) {
            testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns,
                columnInfos, false);
            testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns,
                columnInfos, true);
        }
    }

    @Test
    public void testSpecialColumnName() {
        String tableName = "expr_special_column_tbl";
        String indexName = tableName + "_idx";
        String create = String.format("create table %s (a int primary key, abs varchar(64))", tableName);

        String indexColumns = "abs(10)";
        String[] columnInfos = new String[] {};

        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            false);
        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            true);

        create = String.format("create table %s (a int primary key, b varchar(64))", tableName);
        columnInfos = new String[] {"$0", "abs(10)"};
        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            false);
        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            true);
    }

    @Test
    public void testMultiColumnIndex() {
        String tableName = "expr_multi_column_tbl";
        String indexName = tableName + "_idx";
        String create =
            String.format("create table %s (a int primary key, b int, c int, d varchar(64), e varchar(64))", tableName);

        String indexColumns = "(a+1) desc,b,c-1,substr(d,-2) asc,a+b+c*2";
        String[] columnInfos = new String[] {
            "$0", "(`a` + 1)", "$2", "(`c` - 1)", "$3", "substr(`d`,-(2))", "$4", "((`a` + `b`) + (`c` * 2))"};

        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            false);
        testCreateExprIndexInternal(tableName, indexName, create, "dbpartition by hash(a)", indexColumns, columnInfos,
            true);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show index from " + tableName);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);

        String[] columnInIndex =
            new String[] {indexName + "$0", "b", indexName + "$2", indexName + "$3", indexName + "$4"};
        for (List<Object> object : objects) {
            if (object.get(2).toString().equalsIgnoreCase(indexName)) {
                int seq = Integer.parseInt(object.get(3).toString()) - 1;
                Assert.assertTrue(columnInIndex[seq].equalsIgnoreCase(object.get(4).toString()));
            }
        }

        System.out.println(objects);
    }

    private void testCreateExprIndexInternal(String tableName, String indexName, String create, String partDef,
                                             String indexColumns, String[] columnInfos, boolean unique) {
        System.out.println(tableName + " " + partDef + " " + indexColumns);
        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);

        String hint = buildCmdExtra(ENABLE_CREATE_EXPRESSION_INDEX, ENABLE_UNIQUE_KEY_ON_GEN_COL);
        List<String> ddl = generatedDdl(tableName, "local " + (unique ? "unique" : ""), indexName, indexColumns, "");
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + ddl.get(0));
        checkGenCol(tableName, columnInfos);

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create + partDef);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + ddl.get(1));
        checkGenCol(tableName, columnInfos);
    }

    private void checkGenCol(String tableName, String[] columnInfos) {
        String query = String.format(
            "select column_name,extra,generation_expression from information_schema.columns where table_schema='%s' and table_name='%s'",
            tddlDatabase1, tableName);
        ResultSet rs = JdbcUtil.executeQuery(query, tddlConnection);
        List<List<Object>> objects = JdbcUtil.getAllResult(rs);

        Set<Integer> columnFound = new HashSet<>();
        System.out.println(objects);
        for (List<Object> object : objects) {
            for (int j = 0; j < columnInfos.length; j += 2) {
                if (object.get(0).toString().contains(columnInfos[j])) {
                    Assert.assertTrue(object.get(1).toString().equals("VIRTUAL GENERATED"));
                    Assert.assertTrue(object.get(2).toString().equals(columnInfos[j + 1]));
                    columnFound.add(j);
                    break;
                }
            }
        }

        int virtualColumnCount = 0;
        for (List<Object> object : objects) {
            if (object.get(1).toString().equals("VIRTUAL GENERATED")) {
                virtualColumnCount++;
            }
        }

        Assert.assertTrue(columnFound.size() == columnInfos.length / 2);
        Assert.assertTrue(columnFound.size() == virtualColumnCount);
    }

    private List<String> generatedDdl(String tableName, String indexDef, String indexName, String indexColumns,
                                      String partDef) {
        List<String> ddl = new ArrayList<>();
        ddl.add(String.format("alter table %s add %s index %s(%s) %s", tableName, indexDef, indexName,
            String.join(",", indexColumns), partDef));
        ddl.add(String.format("create %s index %s on %s(%s) %s", indexDef, indexName, tableName,
            String.join(",", indexColumns), partDef));
        return ddl;
    }

    @Test
    public void testMultipleAlterTable() {
        String tableName = "expr_index_multi_alter_tbl";
        String create = String.format(
            "create table %s (a int primary key, b int, c varchar(20), d varchar(20)) dbpartition by hash(a)",
            tableName);

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);

        String hint = buildCmdExtra(ENABLE_CREATE_EXPRESSION_INDEX, ENABLE_UNIQUE_KEY_ON_GEN_COL);
        String alter = String.format(
            "alter table %s add local index i1(a+1), add local index i2(substr(c,2)), modify column d varchar(40)",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + alter);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show create table " + tableName);
        String showCreateTable = JdbcUtil.getAllStringResult(rs, false, null).get(0).get(1);
        String trueCreateTable = isRDS80 ? "CREATE TABLE `expr_index_multi_alter_tbl` (\n"
            + "\t`a` int(11) NOT NULL,\n"
            + "\t`b` int(11) DEFAULT NULL,\n"
            + "\t`c` varchar(20) COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
            + "\t`d` varchar(40) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,\n"
            + "\t`i1$0` bigint(20) GENERATED ALWAYS AS (`a` + 1) VIRTUAL,\n"
            + "\t`i2$0` varchar(20) COLLATE utf8mb4_general_ci GENERATED ALWAYS AS (substr(`c`, 2)) VIRTUAL,\n"
            + "\tPRIMARY KEY (`a`),\n"
            + "\tKEY `i1` (`i1$0`),\n"
            + "\tKEY `i2` (`i2$0`)\n"
            + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci  dbpartition by hash(`a`)" :
            "CREATE TABLE `expr_index_multi_alter_tbl` (\n"
                + "\t`a` int(11) NOT NULL,\n"
                + "\t`b` int(11) DEFAULT NULL,\n"
                + "\t`c` varchar(20) DEFAULT NULL,\n"
                + "\t`d` varchar(40) DEFAULT NULL,\n"
                + "\t`i1$0` bigint(20) GENERATED ALWAYS AS (`a` + 1) VIRTUAL,\n"
                + "\t`i2$0` varchar(20) GENERATED ALWAYS AS (substr(`c`, 2)) VIRTUAL,\n"
                + "\tPRIMARY KEY (`a`),\n"
                + "\tKEY `i1` (`i1$0`),\n"
                + "\tKEY `i2` (`i2$0`)\n"
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`a`)";
        Assert.assertTrue(showCreateTable.equalsIgnoreCase(trueCreateTable));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show index from " + tableName);
        List<List<String>> objects = JdbcUtil.getAllStringResult(rs, false, null);
        for (List<String> object : objects) {
            if (object.get(2).equalsIgnoreCase("i1")) {
                Assert.assertTrue(object.get(4).equalsIgnoreCase("i1$0"));
            } else if (object.get(2).equalsIgnoreCase("i2")) {
                Assert.assertTrue(object.get(4).equalsIgnoreCase("i2$0"));
            }
        }

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show columns from " + tableName);
        objects = JdbcUtil.getAllStringResult(rs, false, null);
        for (List<String> object : objects) {
            if (object.get(0).equalsIgnoreCase("i1$0")) {
                Assert.assertTrue(object.get(5).equalsIgnoreCase("VIRTUAL GENERATED"));
            } else if (object.get(2).equalsIgnoreCase("i2$0")) {
                Assert.assertTrue(object.get(5).equalsIgnoreCase("VIRTUAL GENERATED"));
            }
        }
    }

    @Test
    public void testAlterTableAddColumnAndIndex() {
        String tableName = "com_index_multi_alter_tbl";
        String create = String.format(
            "create table %s (a int primary key, b int, c varchar(20), d varchar(20)) dbpartition by hash(a)",
            tableName);

        dropTableIfExists(tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, create);
        String hint = buildCmdExtra(ENABLE_CREATE_EXPRESSION_INDEX, ENABLE_UNIQUE_KEY_ON_GEN_COL);
        String alter = String.format(
            "alter table %s add column e varchar(40), add index i1(e(20)), add index i2(c, e(20))",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + alter);

        alter = String.format(
            "alter table %s change column e ee varchar(40), add index i3(ee(20)), add index i4(c, ee(20))",
            tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, hint + alter);
    }
}
