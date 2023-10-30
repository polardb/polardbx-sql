package com.alibaba.polardbx.parser;

public class CreateViewParserTest extends AbstractParserTest {
    public void testCreateViewAsEmpty() {
        String sql = "create view v1 as ;";
        parseSqlShouldFail(sql);

        sql = "create view v1 as";
        parseSqlShouldFail(sql);

        sql = "create or replace v1 as";
        parseSqlShouldFail(sql);

        sql = "create or replace v1 as;";
        parseSqlShouldFail(sql);
    }
}
