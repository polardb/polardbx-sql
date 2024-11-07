package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author yudong
 * @since 2024/6/27 10:45
 **/
public class ReplicaCheckerParserTest {
    private final SQLParserFeature[] FEATURES = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup,
        SQLParserFeature.UseInsertColumnsCache, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };

    @Test
    public void testStartReplicaCheck() {
        String sql = "check replica table `testdb`.`testtb` channel='test'";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` CHANNEL='test'", statement.toString());

        sql = "check replica table `testdb` channel='test'";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` CHANNEL='test'", statement.toString());

        sql = "check replica table `testdb`.`testtb` channel='test' mode=direct";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` CHANNEL='test' MODE=direct", statement.toString());

        sql = "check replica table `testdb`.`testtb` mode=direct";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` MODE=direct", statement.toString());
    }

    @Test
    public void testPauseReplicaCheck() {
        String sql = "check replica table `testdb`.`testtb` pause";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` PAUSE", statement.toString());

        sql = "check replica table `testdb` pause";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` PAUSE", statement.toString());
    }

    @Test
    public void testContinueReplicaCheck() {
        String sql = "check replica table `testdb`.`testtb` continue";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` CONTINUE", statement.toString());

        sql = "check replica table `testdb` continue";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` CONTINUE", statement.toString());
    }

    @Test
    public void testCancelReplicaCheck() {
        String sql = "check replica table `testdb`.`testtb` cancel";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` CANCEL", statement.toString());

        sql = "check replica table `testdb` cancel";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` CANCEL", statement.toString());
    }

    @Test
    public void testResetReplicaCheck() {
        String sql = "check replica table `testdb`.`testtb` reset";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` RESET", statement.toString());

        sql = "check replica table `testdb` reset";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` RESET", statement.toString());

    }

    @Test
    public void testShowReplicaCheckProgress() {
        String sql = "check replica table `testdb`.`testtb` show progress";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` SHOW PROGRESS", statement.toString());

        sql = "check replica table `testdb` show progress";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` SHOW PROGRESS", statement.toString());
    }

    @Test
    public void testShowReplicaCheckDiff() {
        String sql = "check replica table `testdb`.`testtb` show diff";
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb`.`testtb` SHOW DIFF", statement.toString());

        sql = "check replica table `testdb` show diff";
        parser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql, FEATURES);
        statementList = parser.parseStatementList();
        statement = statementList.get(0);
        Assert.assertEquals("CHECK REPLICA TABLE `testdb` SHOW DIFF", statement.toString());
    }

}
