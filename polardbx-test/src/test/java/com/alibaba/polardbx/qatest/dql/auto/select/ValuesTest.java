package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ColumnDataRandomGenerateRule;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.calcite.sql.SqlValuesTableSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import com.alibaba.polardbx.qatest.validator.DataValidator.*;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.polardbXShardingDBName1;
import static com.alibaba.polardbx.qatest.validator.DataValidator.resultSetContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.google.common.truth.Truth.assertWithMessage;

public class ValuesTest extends BaseTestCase {

    private static ColumnDataRandomGenerateRule rule = new ColumnDataRandomGenerateRule();

    private static final String DROP_TABLE = "drop table if exists %s";

    private static String[] colTypes = new String[] {"varchar", "integer"};

    private static int varcharMaxLength = 255;

    private static String colNamePrefix = "c";

    private static String tableName1 = "ValuesStatementTest_values_table1";

    private static String tableName2 = "ValuesStatementTest_values_table2";
    private static String tableNameAlias = "ValuesStatementTest_values_table_alias";

    private static String colDef = "";
    private static String insertValues1 = "";

    private static String valuesStatement1 = "";

    private static String insertValues2 = "";

    private static String valuesStatement2 = "";

    private static String selectStatement = "select * from %s";

    private static int rowNum = 100;

    @BeforeClass
    public static void init() {
        initColDef();
        String createTable = "create table %s (" + colDef + ")";
        initValueStatement();
        String insertTable = "insert into %s %s";

        try (Connection conn = getPolardbxConnection0()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format(DROP_TABLE, tableName1));
            statement.execute(String.format(DROP_TABLE, tableName2));
            statement.execute(String.format(createTable, tableName1));
            statement.execute(String.format(createTable, tableName2));
            statement.execute(String.format(insertTable, tableName1, insertValues1));
            statement.execute(String.format(insertTable, tableName2, insertValues2));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @AfterClass
    public static void dropTable() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format(DROP_TABLE, tableName1));
            statement.execute(String.format(DROP_TABLE, tableName2));
        }
    }

    @Test
    public void valuesSelectTest() {
        try (Connection conn = getPolardbxConnection()) {
            ResultSet rs1 = JdbcUtil.executeQuery(String.format(selectStatement, tableName1), conn);
            ResultSet rs2 = JdbcUtil.executeQuery(valuesStatement1, conn);
            resultSetContentSameAssertIgnoringMetaInfo(rs1, rs2, false);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void valuesAliasTest() {
        String valueAliasSql = "select * from (" + valuesStatement1 + ") as " + createAliasAndColumns();
        try (Connection conn = getPolardbxConnection()) {
            ResultSet rs1 = JdbcUtil.executeQuery(String.format(selectStatement, tableName1), conn);
            ResultSet rs2 = JdbcUtil.executeQuery(valueAliasSql, conn);
            resultSetContentSameAssert(rs1, rs2, false);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void valuesOrderByAndLimitTest() {
        try (Connection conn = getPolardbxConnection()) {
            for (int i = 0; i < colTypes.length; i++) {
                StringJoiner sj1 = new StringJoiner(",");
                StringJoiner sj2 = new StringJoiner(",");
                for (int j = 0; j < colTypes.length; j++) {
                    int colIndex = (i + j) % colTypes.length;
                    sj1.add(createColName(colIndex));
                    sj2.add("column_" + colIndex);
                }
                String orderByColName = sj1.toString();
                String orderByDefaultName = sj2.toString();

                int offset = rowNum / 2;
                int fetch = rowNum - offset;
                String orderByColNameAndLimit = " order by " + orderByColName + " limit " + offset + "," + fetch;
                String orderByDefaultNameAndLimit =
                    " order by " + orderByDefaultName + " limit " + offset + "," + fetch;

                String selectSql = "select * from " + tableName1 + orderByColNameAndLimit;
                String valuesSql = valuesStatement1 + orderByDefaultNameAndLimit;
                String selectValuesSql1 = "select * from (" + valuesSql + ") as " + createAliasAndColumns();
                String selectValuesSql2 =
                    "select * from (" + valuesStatement1 + ") as " + createAliasAndColumns() + orderByColNameAndLimit;

                ResultSet rs1 = JdbcUtil.executeQuery(selectSql, conn);
                ResultSet rs2 = JdbcUtil.executeQuery(valuesSql, conn);
                ResultSet rs3 = JdbcUtil.executeQuery(selectValuesSql1, conn);
                ResultSet rs4 = JdbcUtil.executeQuery(selectValuesSql2, conn);

                resultSetContentSameAssertIgnoringMetaInfo(rs3, rs2, false);
                resultSetContentSameAssert(rs1, rs4, false);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void valuesUnPushedJoinTest() {
        StringJoiner sj = new StringJoiner(",");
        for (int j = 0; j < colTypes.length; j++) {
            sj.add(tableName1 + "." + createColName(j));
        }
        String orderBy = " order by " + sj.toString();

        try (Connection conn = getPolardbxConnection()) {
            for (int i = 0; i < colTypes.length; i++) {
                String joinColName = createColName(i);
                String selectJoinSql = "select * from " + tableName1 + " join " + tableName2 +
                    createJoinCondition(tableName1, tableName2, joinColName, joinColName) + orderBy;
                String valuesJoinSql = "/*+TDDL:ENABLE_VALUES_PUSHDOWN=false*/select * from " + tableName1 + " join " +
                    " (" + valuesStatement2 + ") as " + createAliasAndColumns() +
                    createJoinCondition(tableName1, tableNameAlias, joinColName, joinColName) + orderBy;

                ResultSet rs1 = JdbcUtil.executeQuery(selectJoinSql, conn);
                ResultSet rs2 = JdbcUtil.executeQuery(valuesJoinSql, conn);
                resultSetContentSameAssert(rs1, rs2, false);
                rs1.close();
                rs2.close();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    /**
     * values pushed join暂时不支持表别名
     */
    @Test
    public void valuesPushedJoinTest() {
        StringJoiner sj = new StringJoiner(",");
        for (int j = 0; j < colTypes.length; j++) {
            sj.add(tableName1 + "." + createColName(j));
        }
        String orderBy = " order by " + sj.toString();

        try (Connection conn = getPolardbxConnection()) {
            for (int i = 0; i < colTypes.length; i++) {
                String joinColName = createColName(i);
                String selectJoinSql = "select * from " + tableName1 + " join " + tableName2 +
                    createJoinCondition(tableName1, tableName2, joinColName, joinColName) + orderBy;
                String valuesJoinSql = "select * from " + tableName1 + " join " +
                    " (" + valuesStatement2 + ") as " + tableNameAlias +
                    createJoinCondition(tableName1, tableNameAlias, joinColName,
                        SqlValuesTableSource.COLUMN_NAME_PREFIX + i) + orderBy;

                ResultSet rs1 = JdbcUtil.executeQuery(selectJoinSql, conn);
                ResultSet rs2 = JdbcUtil.executeQuery(valuesJoinSql, conn);
                resultSetContentSameAssertIgnoringMetaInfo(rs1, rs2, false);
                rs1.close();
                rs2.close();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }

    @Test
    public void valuesUnionTest() {
        String[] unions = new String[] {"union", "intersect", "except"};
        for (String union : unions) {
            String selectUnionSql = "select " + createColumns() + " from " + tableName1 + " " + union + " " +
                "select " + createColumns() + " from " + tableName2;
            String valuesUnionSql = valuesStatement1 + " " + union + " " + valuesStatement2;
            try (Connection conn = getPolardbxConnection()) {
                ResultSet rs1 = JdbcUtil.executeQuery(selectUnionSql, conn);
                ResultSet rs2 = JdbcUtil.executeQuery(valuesUnionSql, conn);
                resultSetContentSameAssertIgnoringMetaInfoAndOrder(rs1, rs2, false);
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    }

    private String createJoinCondition(String tableName1, String tableName2, String columnName1, String columnName2) {
        return " on " + tableName1 + "." + columnName1 + " = " + tableName2 + "." + columnName2;
    }

    private static String createColumns() {
        String colNames = "";
        for (int i = 0; i < colTypes.length; i++) {
            colNames += createColName(i);
            if (i != colTypes.length - 1) {
                colNames += ",";
            }
        }
        return colNames;
    }

    private static String createAliasAndColumns() {
        return tableNameAlias + "(" + createColumns() + ")";
    }

    private static void initColDef() {
        for (int i = 0; i < colTypes.length; i++) {
            colDef += createColName(i) + " " + colTypes[i];
            if (colTypes[i].equals("varchar")) {
                colDef += "(" + varcharMaxLength + ")";
            } else if (colTypes[i].equals("decimal")) {
                colDef += "(65,30)";
            }
            if (i != colTypes.length - 1) {
                colDef += ", ";
            }
        }
    }

    private static void initValueStatement() {
        insertValues1 = "values ";
        valuesStatement1 = "values ";
        insertValues2 = "values ";
        valuesStatement2 = "values ";

        for (int i = 0; i < rowNum; i++) {
            if (i != rowNum - 1) {
                String row1 = createRandomRow(colTypes);
                String row2 = createRandomRow(colTypes);
                insertValues1 += "(" + row1 + ")";
                valuesStatement1 += "row(" + row1 + ")";
                insertValues2 += "(" + row2 + ")";
                valuesStatement2 += "row(" + row2 + ")";
                insertValues1 += ",";
                valuesStatement1 += ",";
                insertValues2 += ",";
                valuesStatement2 += ",";
            } else {
                //两个表至少包含一条相同记录，用于验证join
                String row = createRandomRow(colTypes);
                insertValues1 += "(" + row + ")";
                valuesStatement1 += "row(" + row + ")";
                insertValues2 += "(" + row + ")";
                valuesStatement2 += "row(" + row + ")";
            }
        }

    }

    private static String createColName(int index) {
        return colNamePrefix + index;
    }

    private static String createRandomRow(String[] colTypes) {
        String row = "";
        for (int i = 0; i < colTypes.length; i++) {
            row += createRandomColValue(colTypes[i]);
            if (i != colTypes.length - 1) {
                row += ",";
            }
        }
        return row;
    }

    private static String createRandomColValue(String colType) {
        String col = "";
        switch (colType) {
        case "varchar":
            col += UUID.randomUUID().toString();
            col = col.length() > varcharMaxLength ?
                col.substring(0, varcharMaxLength) : col;
            col = "'" + col + "'";
            break;
        case "char":
            col += rule.char_testRandom();
            col = "'" + col + "'";
            break;
        case "integer":
            col += rule.integer_testRandom();
            break;
        case "decimal":
            col += rule.decimal_testRandom();
            break;
        case "datetime":
            col += rule.datetime_testRandom();
            col = "'" + col + "'";
            break;
        case "timestamp":
            col += rule.timestamp_testRandom();
            col = "'" + col + "'";
            break;
        default:
            throw new UnsupportedOperationException();
        }
        return col;
    }

    private void resultSetContentSameAssertIgnoringMetaInfo(ResultSet rs1, ResultSet rs2, boolean allowEmptyResultSet) {
        List<List<Object>> rs1Results = JdbcUtil.getAllResult(rs1);
        List<List<Object>> rs2Results = JdbcUtil.getAllResult(rs2);
        // 不允许为空结果集合
        if (!allowEmptyResultSet) {
            Assert.assertTrue("查询的结果集为空，请修改sql语句，保证有结果集", rs1Results.size() != 0);
        }
        assertWithMessage("返回结果不一致").that(rs1Results).containsExactlyElementsIn(rs2Results).inOrder();
    }

    private void resultSetContentSameAssertIgnoringMetaInfoAndOrder(ResultSet rs1, ResultSet rs2,
                                                                    boolean allowEmptyResultSet) {
        List<List<Object>> rs1Results = JdbcUtil.getAllResult(rs1);
        List<List<Object>> rs2Results = JdbcUtil.getAllResult(rs2);
        // 不允许为空结果集合
        if (!allowEmptyResultSet) {
            Assert.assertTrue("查询的结果集为空，请修改sql语句，保证有结果集", rs1Results.size() != 0);
        }
        assertWithMessage("返回结果不一致").that(rs1Results).containsExactlyElementsIn(rs2Results);
    }

}

