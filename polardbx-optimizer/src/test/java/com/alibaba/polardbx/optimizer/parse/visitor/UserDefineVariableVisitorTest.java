package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedVisitor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class UserDefineVariableVisitorTest {

    @Test
    public void test() {
        StringBuilder builder = new StringBuilder();
        ExecutionContext context = new ExecutionContext();
        context.setUserDefVariables(new HashMap<>());
        List<Object> outParameters = new ArrayList<>();

        ParameterizedVisitor visitor = new DrdsParameterizeSqlVisitor(builder, true, context);
        visitor.setOutputParameters(outParameters);

        // a variable that has not been initialized
        SQLVariantRefExpr expr = new SQLVariantRefExpr("@xxx");
        visitor.visit(expr);

        Assert.assertTrue(
            outParameters.size() == 1 && outParameters.get(0) instanceof DrdsParameterizeSqlVisitor.UserDefVariable);
        Assert.assertTrue(context.getUserDefVariables().get("xxx") == null);

    }

}

