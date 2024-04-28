package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
public class CdcFunctionMarkTest extends CdcBaseTest {

    @Test
    public void testParseDrop() {
        String sql = "drop java function if exists myf1bint2bint";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> parseResult = parser.parseStatementList();
        String expectSql = "DROP JAVA FUNCTION IF EXISTS myf1bint2bint";
        String toStringSql = parseResult.get(0).toString();
        System.out.println(toStringSql);
        SQLUtils.parseSingleStatement(toStringSql, DbType.mysql);
        Assert.assertEquals(expectSql, toStringSql);
    }

    @Test
    public void testToString() {
        String sql = "CREATE JAVA FUNCTION myf1bint2bint\n"
            + "  no state\n"
            + "  RETURN_TYPE bigint\n"
            + "  INPUT_TYPES bigint\n"
            + "CODE\n"
            + "  public class Myf1bint2bint extends UserDefinedJavaFunction {\n"
            + "  public Object compute(Object[] args) {\n"
            + "    Long a = (Long) args[0];\n"
            + "    return a;\n"
            + "  }\n"
            + "};\n"
            + "END_CODE";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> parseResult = parser.parseStatementList();
        String expectSql = "CREATE JAVA FUNCTION myf1bint2bint NO STATE  RETURN_TYPE bigint INPUT_TYPES bigint\n"
            + "CODE\n"
            + "public class Myf1bint2bint extends UserDefinedJavaFunction {\n"
            + "  public Object compute(Object[] args) {\n"
            + "    Long a = (Long) args[0];\n"
            + "    return a;\n"
            + "  }\n"
            + "};\n"
            + "END_CODE";
        String toStringSql = parseResult.get(0).toString();
        System.out.println(toStringSql);
        SQLUtils.parseSingleStatement(parseResult.get(0).toString(), DbType.mysql);
        Assert.assertEquals(expectSql, toStringSql);
    }

    @Test
    public void testToString2() {
        String sql = "CREATE JAVA FUNCTION myf1bint2bint2\n"
            + "  no state\n"
            + "  RETURN_TYPE bigint\n"
            + "  INPUT_TYPES bigint, varchar(255), int\n"
            + "CODE\n"
            + "  public class Myf1bint2bint2 extends UserDefinedJavaFunction {\n"
            + "  public Object compute(Object[] args) {\n"
            + "    Long a = (Long) args[0];\n"
            + "    return a;\n"
            + "  }\n"
            + "};\n"
            + "END_CODE";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> parseResult = parser.parseStatementList();
        String expectSql =
            "CREATE JAVA FUNCTION myf1bint2bint2 NO STATE  RETURN_TYPE bigint INPUT_TYPES bigint,varchar(255),int\n"
                + "CODE\n"
                + "public class Myf1bint2bint2 extends UserDefinedJavaFunction {\n"
                + "  public Object compute(Object[] args) {\n"
                + "    Long a = (Long) args[0];\n"
                + "    return a;\n"
                + "  }\n"
                + "};\n"
                + "END_CODE";
        String toStringSql = parseResult.get(0).toString();
        System.out.println(toStringSql);
        SQLUtils.parseSingleStatement(parseResult.get(0).toString(), DbType.mysql);
        Assert.assertEquals(expectSql, toStringSql);
    }

    @Test
    public void testToString3() {
        String sql = "CREATE JAVA FUNCTION myf1bint2bint3\n"
            + "  RETURN_TYPE bigint\n"
            + "CODE\n"
            + "  public class Myf1bint2bint3 extends UserDefinedJavaFunction {\n"
            + "  public Object compute(Object[] args) {\n"
            + "    Long a = (Long) args[0];\n"
            + "    return a;\n"
            + "  }\n"
            + "};\n"
            + "END_CODE";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
        List<SQLStatement> parseResult = parser.parseStatementList();
        String expectSql = "CREATE JAVA FUNCTION myf1bint2bint3 RETURN_TYPE bigint\n"
            + "CODE\n"
            + "public class Myf1bint2bint3 extends UserDefinedJavaFunction {\n"
            + "  public Object compute(Object[] args) {\n"
            + "    Long a = (Long) args[0];\n"
            + "    return a;\n"
            + "  }\n"
            + "};\n"
            + "END_CODE";
        String toStringSql = parseResult.get(0).toString();
        System.out.println(toStringSql);
        SQLUtils.parseSingleStatement(parseResult.get(0).toString(), DbType.mysql);
        Assert.assertEquals(expectSql, toStringSql);
    }

    @Test
    public void testCdcDdlRecord() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            String currentDB = getCurrentDbName(stmt);
            String sql = "CREATE FUNCTION my_mul(x int, y int) \n"
                + "RETURNS int\n"
                + "LANGUAGE SQL\n"
                + "DETERMINISTIC\n"
                + "COMMENT 'my multiply function'\n"
                + "RETURN x*y*31";
            MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(sql));
            List<SQLStatement> parseResult = parser.parseStatementList();
            System.out.println(parseResult.get(0).toString());
            // common function
            stmt.executeUpdate("drop function if exists my_mul");
            executeAndCheck(stmt, "CREATE FUNCTION my_mul(x int, y int) \n"
                + "RETURNS int\n"
                + "LANGUAGE SQL\n"
                + "DETERMINISTIC\n"
                + "COMMENT 'my multiply function'\n"
                + "RETURN x*y*31", 1, currentDB, "mysql.my_mul", SqlKind.CREATE_FUNCTION.name());
            executeAndCheck(stmt, "alter function my_mul comment 'xxx'", 1, currentDB,
                "mysql.my_mul",
                SqlKind.ALTER_FUNCTION.name());
            executeAndCheck(stmt, "drop function my_mul", 1, currentDB, "mysql.my_mul",
                SqlKind.DROP_FUNCTION.name());
            executeAndCheck(stmt, "drop function if exists my_mul", 0, currentDB, "mysql.my_mul",
                SqlKind.DROP_FUNCTION.name());

            // java function
            String functionName = "cdcbint2bint";
            String create_sql = "CREATE JAVA FUNCTION cdcbint2bint\n"
                + "  no state\n"
                + "  RETURN_TYPE bigint\n"
                + "  INPUT_TYPES bigint\n"
                + "CODE\n"
                + "  public class Cdcbint2bint extends UserDefinedJavaFunction {\n"
                + "  public Object compute(Object[] args) {\n"
                + "    Long a = (Long) args[0];\n"
                + "    return a;\n"
                + "  }\n"
                + "};\n"
                + "END_CODE";
            String drop_sql = "DROP JAVA FUNCTION IF EXISTS " + functionName;

            stmt.executeUpdate(drop_sql);
            executeAndCheck(stmt, create_sql, 1, currentDB, functionName, SqlKind.CREATE_JAVA_FUNCTION.name());
            executeAndCheck(stmt, drop_sql, 1, currentDB, functionName, SqlKind.DROP_JAVA_FUNCTION.name());

        }
    }

    private void executeAndCheck(Statement stmt, String ddl, int expectCount, String schemaName, String tableName,
                                 String sqlKind)
        throws SQLException {
        String tokenHints = buildTokenHints();
        String sql = tokenHints + ddl;
        stmt.executeUpdate(sql);

        List<DdlRecordInfo> list = getDdlRecordInfoListByToken(tokenHints);
        Assert.assertEquals(expectCount, list.size());
        if (expectCount != 0) {
            Assert.assertEquals(sql, list.get(0).getDdlSql());
            Assert.assertEquals(schemaName, list.get(0).getSchemaName());
            Assert.assertEquals(tableName, list.get(0).getTableName());
            Assert.assertEquals(sqlKind, list.get(0).getSqlKind());
            Assert.assertEquals(DdlScope.Instance.getValue(), list.get(0).getDdlExtInfo().getDdlScope());
        }
    }
}
