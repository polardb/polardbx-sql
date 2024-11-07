package com.alibaba.polardbx.druid.sql.visitor;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class SQLIntegerExprTest {
    @Test
    public void testBigNumber() {
        SQLBinaryOpExpr expr = new SQLBinaryOpExpr(
            new SQLIntegerExpr(new BigInteger("537456734573625873285347258234657354353425")),
            SQLBinaryOperator.LessThan,
            new SQLIntegerExpr(new BigInteger("-4457823452837432004173813010418234732483952952798543925"))
        );

        Assert.assertTrue(
            "537456734573625873285347258234657354353425 < -4457823452837432004173813010418234732483952952798543925".equals(
                expr.toString()));
    }
}
