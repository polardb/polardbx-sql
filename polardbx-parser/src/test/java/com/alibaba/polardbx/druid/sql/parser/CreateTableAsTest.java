package com.alibaba.polardbx.druid.sql.parser;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class CreateTableAsTest {
    @Test
    public void testCreateAs()
        throws SQLException {
        String content =
            "CREATE TABLE t2 AS t1";
        String expectResult =
            "CREATE TABLE t2 AS t1";
        ;
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());
        Assert.assertEquals(expectResult, parseResult.get(0).toString());
    }

    @Test
    public void testCreateSelect()
        throws SQLException {
        String content =
            "CREATE TABLE t2 AS SELECT t FROM t1";
        String expectResult =
            "CREATE TABLE t2\n"
                + "AS\n"
                + "SELECT t\n"
                + "FROM t1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());
        Assert.assertEquals(expectResult, parseResult.get(0).toString());
    }

    @Test
    public void testCreateSelect2()
        throws SQLException {
        String content =
            "CREATE TABLE t2 SELECT t FROM t1";
        String expectResult =
            "CREATE TABLE t2\n"
                + "AS\n"
                + "SELECT t\n"
                + "FROM t1";
        MySqlStatementParser parser = new MySqlStatementParser(ByteString.from(content));
        List<SQLStatement> parseResult = parser.parseStatementList();
        Assert.assertEquals(1, parseResult.size());
        Assert.assertEquals(expectResult, parseResult.get(0).toString());
    }

}
