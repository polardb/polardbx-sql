package com.alibaba.polardbx.qatest.ddl.sharding.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.DrdsMoveDataBase;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.junit.Test;

public class MovedatabaseParserTest {

    @Test
    public void testParse() {
        String sql = "move database 'A-B-C_000002_GROUP' to 'Dn1'";

        final DrdsMoveDataBase moveDatabase = (DrdsMoveDataBase) FastsqlUtils.parseSql(sql).get(0).clone();

        String astString = moveDatabase.toString();
        System.out.println(astString);
        Assert.assertTrue(astString.contains("'A-B-C_000002_GROUP'") && astString.contains("'DN1"));
    }
}