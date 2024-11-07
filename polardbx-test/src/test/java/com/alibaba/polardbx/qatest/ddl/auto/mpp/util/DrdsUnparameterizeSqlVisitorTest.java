package com.alibaba.polardbx.qatest.ddl.auto.mpp.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsUnparameterizeSqlVisitor;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.druid.sql.parser.SQLParserUtils.createSQLStatementParser;

public class DrdsUnparameterizeSqlVisitorTest extends TestCase {
    public void unparameterizeSqlCheck(String sql, List<ParameterContext> params) {
        String out = DrdsUnparameterizeSqlVisitor.unparameterizeSql(sql, params);
        SQLStatementParser statementParser = createSQLStatementParser(out, JdbcConstants.MYSQL);
        SQLStatement sqlStatment = statementParser.parseStatement();
        String reParseOut = sqlStatment.toString();
        Assert.assertTrue(out.equals(reParseOut));
    }

    @Test
    public void testUnparameterizeSqlInt() {
        List<ParameterContext> params = new ArrayList<>();
        params.add(new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1}));
        String sql = "select id from t1 where a < ?";
        unparameterizeSqlCheck(sql, params);
    }

    public void testUnparameterizeSql2() {
        List<ParameterContext> params = new ArrayList<>();
        params.add(new ParameterContext(ParameterMethod.setLong, new Object[] {1, 1}));
        params.add(new ParameterContext(ParameterMethod.setLong, new Object[] {2, 100}));
        params.add(new ParameterContext(ParameterMethod.setFloat, new Object[] {3, 0.02}));
        params.add(new ParameterContext(ParameterMethod.setLong, new Object[] {4, 24}));
        String sql = "select id from t1 where a < ? and a > ? and rand() < ? order by a limit ?";
        unparameterizeSqlCheck(sql, params);
    }

    public void testUnparameterizeSql3() {
        List<ParameterContext> params = new ArrayList<>();
        params.add(new ParameterContext(ParameterMethod.setString, new Object[] {1, "t1"}));
        String sql = "select id from ? where a < 3";
        unparameterizeSqlCheck(sql, params);
    }

    public void testUnparameterizeSql4() {
        List<ParameterContext> params = new ArrayList<>();
        params.add(new ParameterContext(ParameterMethod.setString, new Object[] {1, "1"}));
        params.add(new ParameterContext(ParameterMethod.setString, new Object[] {2, "100"}));
        params.add(new ParameterContext(ParameterMethod.setFloat, new Object[] {3, 0.02}));
        params.add(new ParameterContext(ParameterMethod.setLong, new Object[] {4, 24}));
        String sql = "select id from t1 where a < ? and a > ? and rand() < ? order by a limit ?";
        unparameterizeSqlCheck(sql, params);
    }
}