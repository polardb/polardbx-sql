package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.vectorized.BuiltInFunctionVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastInVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastNotInVectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class Rex2VecExprTest {

    private final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private ExpressionRewriter rewriter;
    private ExecutionContext context;

    @Before
    public void before() {
        context = new ExecutionContext();
        context.getParamManager().getProps().put("ENABLE_EXPRESSION_VECTORIZATION", "true");
        context.getParamManager().getProps().put("ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE", "true");
        this.rewriter = new ExpressionRewriter(context);
    }

    @Test
    public void testVisitIn() {
        Rex2VectorizedExpressionVisitor visitor = new Rex2VectorizedExpressionVisitor(context, 3);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall inCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN,
            rexBuilder.makeDynamicParam(intType, 0),
            rexBuilder.makeDynamicParam(intType, 1),
            rexBuilder.makeDynamicParam(intType, 2));
        VectorizedExpression vectorizedExpression = visitor.visitCall(inCall1);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof BuiltInFunctionVectorizedExpression);

        visitor = new Rex2VectorizedExpressionVisitor(context, 3);
        RexCall inCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeIntLiteral(100),
            rexBuilder.makeIntLiteral(200),
            rexBuilder.makeIntLiteral(300));
        vectorizedExpression = visitor.visitCall(inCall2);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof FastInVectorizedExpression);

        // all args are of the same types
        RexNode[] nodes = new RexNode[1000];
        nodes[0] = rexBuilder.makeInputRef(intType, 0);
        for (int i = 1; i < nodes.length; i++) {
            nodes[i] = rexBuilder.makeIntLiteral(10 * i);
        }
        RexCall inCall3 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN, nodes);
        vectorizedExpression = visitor.visitCall(inCall3);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof FastInVectorizedExpression);

        // diff arg types
        nodes[1] = rexBuilder.makeLiteral("0");
        RexCall inCall4 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN, nodes);
        vectorizedExpression = visitor.visitCall(inCall4);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof BuiltInFunctionVectorizedExpression);
    }

    @Test
    public void testVisitNotIn() {
        Rex2VectorizedExpressionVisitor visitor = new Rex2VectorizedExpressionVisitor(context, 3);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall inCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.NOT_IN,
            rexBuilder.makeDynamicParam(intType, 0),
            rexBuilder.makeDynamicParam(intType, 1),
            rexBuilder.makeDynamicParam(intType, 2));
        VectorizedExpression vectorizedExpression = visitor.visitCall(inCall1);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof BuiltInFunctionVectorizedExpression);

        visitor = new Rex2VectorizedExpressionVisitor(context, 3);
        RexCall inCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.NOT_IN,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeIntLiteral(100),
            rexBuilder.makeIntLiteral(200),
            rexBuilder.makeIntLiteral(300));
        vectorizedExpression = visitor.visitCall(inCall2);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof FastNotInVectorizedExpression);

        // all args are of the same types
        RexNode[] nodes = new RexNode[1000];
        nodes[0] = rexBuilder.makeInputRef(intType, 0);
        for (int i = 1; i < nodes.length; i++) {
            nodes[i] = rexBuilder.makeIntLiteral(10 * i);
        }
        RexCall inCall3 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.NOT_IN, nodes);
        vectorizedExpression = visitor.visitCall(inCall3);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof FastNotInVectorizedExpression);

        // diff arg types
        nodes[1] = rexBuilder.makeLiteral("0");
        RexCall inCall4 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.NOT_IN, nodes);
        vectorizedExpression = visitor.visitCall(inCall4);
        Assert.assertTrue(vectorizedExpression.getClass().getName(),
            vectorizedExpression instanceof BuiltInFunctionVectorizedExpression);
    }

    @Test
    public void testVisitFieldAccess() {
        Rex2VectorizedExpressionVisitor visitor = new Rex2VectorizedExpressionVisitor(context, 2);
        try {
            VectorizedExpression vectorizedExpression = visitor.visitFieldAccess(Mockito.mock(RexFieldAccess.class));
            Assert.fail("Expect exception");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Correlated variable is not supported"));
        }
    }

    @Test
    public void testVisitDynamicParam() {
        Parameters parameters = new Parameters();
        parameters.getCurrentParameter().put(1, new ParameterContext(ParameterMethod.setInt, new Object[] {0, 100}));
        context.setParams(parameters);
        Rex2VectorizedExpressionVisitor visitor = new Rex2VectorizedExpressionVisitor(context, 2);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexDynamicParam param = new RexDynamicParam(intType, 0);
        VectorizedExpression vectorizedExpression = visitor.visitDynamicParam(param);
        Assert.assertTrue(vectorizedExpression instanceof LiteralVectorizedExpression);
        Assert.assertEquals(100, (int) ((LiteralVectorizedExpression) vectorizedExpression).getValue());
    }
}
