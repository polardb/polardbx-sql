package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class ExpressionRewriterTest {

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
    public void testRewriteTrim() {
        RelDataType varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexCall exprCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.TRIM,
            rexBuilder.makeDynamicParam(varcharType, 0),
            rexBuilder.makeDynamicParam(varcharType, 1),
            rexBuilder.makeDynamicParam(varcharType, 2));
        RexCall rewrite = rewriter.rewrite(exprCall, false);
        Assert.assertTrue(rewrite.op instanceof SqlTrimFunction);
    }

    @Test
    public void testRewriteDataAdd() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall exprCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.DATE_ADD,
            rexBuilder.makeDynamicParam(dateType, 0),
            rexBuilder.makeDynamicParam(intType, 1));
        RexCall rewrite = rewriter.rewrite(exprCall, false);
        // flat the operands tree:
        //
        //       DATE_ADD                                   DATE_ADD
        //       /      \                               /     |         \
        //   TIME    DAY VALUE        =>           TIME    DAY VALUE   INTERVAL_DAY
        //
        Assert.assertEquals(3, rewrite.operands.size());
    }

    @Test
    public void testRewriteDataAddInterval() {
        RelDataType dateType = typeFactory.createSqlType(SqlTypeName.DATE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexLiteral timeUnit = rexBuilder.makeLiteral("DAY");
        RexCall intervalCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.INTERVAL_PRIMARY,
            rexBuilder.makeDynamicParam(intType, 1), timeUnit);
        RexCall exprCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.DATE_ADD,
            rexBuilder.makeDynamicParam(dateType, 0),
            intervalCall);
        RexCall rewrite = rewriter.rewrite(exprCall, false);
        // flat the operands tree:
        //
        //       DATE_ADD                                 DATE_ADD
        //       /      \                               /    |     \
        //   TIME   INTERVAL_PRIMARY        =>      TIME    VALUE   TIME_UNIT
        //                 /    \
        //           VALUE        TIME_UNIT
        Assert.assertEquals(3, rewrite.operands.size());
    }

    @Test
    public void testRewriteOr() {
        Parameters params = new Parameters();
        params.getCurrentParameter().put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {0, 1}));
        params.getCurrentParameter().put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 2}));
        params.getCurrentParameter().put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {2, 3}));
        params.getCurrentParameter().put(4, new ParameterContext(ParameterMethod.setLong, new Object[] {3, 4}));
        context.setParams(params);

        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RexCall rexCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.OR,
            rexBuilder.makeDynamicParam(boolType, 0),
            rexBuilder.makeDynamicParam(boolType, 1));
        RexCall rewrite1 = rewriter.rewrite(rexCall1, false);
        Assert.assertEquals(2, rewrite1.operands.size());

        RexCall andCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.AND,
            rexBuilder.makeDynamicParam(boolType, 0),
            rexBuilder.makeDynamicParam(boolType, 1));
        RexCall andCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.AND,
            rexBuilder.makeDynamicParam(boolType, 2),
            rexBuilder.makeDynamicParam(boolType, 3));
        RexCall rexCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.OR,
            andCall1, andCall2);
        RexCall rewrite2 = rewriter.rewrite(rexCall2, false);
        Assert.assertEquals(2, rewrite2.operands.size());
    }

    @Test
    public void testRewriteAnd() {
        Parameters params = new Parameters();
        params.getCurrentParameter().put(1, new ParameterContext(ParameterMethod.setLong, new Object[] {0, 1}));
        params.getCurrentParameter().put(2, new ParameterContext(ParameterMethod.setLong, new Object[] {1, 2}));
        params.getCurrentParameter().put(3, new ParameterContext(ParameterMethod.setLong, new Object[] {2, 3}));
        params.getCurrentParameter().put(4, new ParameterContext(ParameterMethod.setLong, new Object[] {3, 4}));
        context.setParams(params);

        RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        RexCall rexCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.AND,
            rexBuilder.makeDynamicParam(boolType, 0),
            rexBuilder.makeDynamicParam(boolType, 1));
        RexCall rewrite1 = rewriter.rewrite(rexCall1, false);
        Assert.assertEquals(2, rewrite1.operands.size());

        RexCall orCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.OR,
            rexBuilder.makeDynamicParam(boolType, 0),
            rexBuilder.makeDynamicParam(boolType, 1));
        RexCall orCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.OR,
            rexBuilder.makeDynamicParam(boolType, 2),
            rexBuilder.makeDynamicParam(boolType, 3));
        RexCall rexCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.AND,
            orCall1, orCall2);
        RexCall rewrite2 = rewriter.rewrite(rexCall2, false);
        Assert.assertEquals(2, rewrite2.operands.size());
    }

    @Test
    public void testRewriteIn() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall inCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN,
            rexBuilder.makeInputRef(intType, 1),
            rexBuilder.makeDynamicParam(intType, 1),
            rexBuilder.makeDynamicParam(intType, 2));
        RexCall rewrite1 = rewriter.rewrite(inCall1, false);
        Assert.assertEquals(3, rewrite1.operands.size());

        RexCall row = (RexCall) rexBuilder.makeCall(TddlOperatorTable.ROW,
            rexBuilder.makeDynamicParam(intType, 1),
            rexBuilder.makeDynamicParam(intType, 2),
            rexBuilder.makeDynamicParam(intType, 3));
        RexCall inCall2 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN,
            rexBuilder.makeDynamicParam(intType, 0), row);
        RexCall rewrite2 = rewriter.rewrite(inCall2, false);
        Assert.assertEquals(2, rewrite2.operands.size());
    }

    /**
     * Raw string decimal in test
     */
    @Test
    public void testRewriteInDecimal() {
        List<BigDecimal> list = new ArrayList<>();
        list.add(new BigDecimal("125.12"));
        list.add(new BigDecimal("521.34"));
        list.add(new BigDecimal("999.8"));
        RawString rawString = new RawString(list);
        context.setParams(new Parameters());
        context.getParams().getCurrentParameter().put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {
            1, rawString}));

        RelDataType charType = typeFactory.createSqlType(SqlTypeName.CHAR);
        RexCall rowCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.ROW,
            rexBuilder.makeDynamicParam(charType, 0));
        RexCall inCall1 = (RexCall) rexBuilder.makeCall(TddlOperatorTable.IN,
            rexBuilder.makeInputRef(charType, 1),
            rowCall);
        RexCall rewrite1 = rewriter.rewrite(inCall1, false);
        Assert.assertEquals(4, rewrite1.operands.size());
        for (int i = 0; i < list.size(); i++) {
            Assert.assertEquals(list.get(i), ((RexLiteral) rewrite1.operands.get(i + 1)).getValue3());
        }
    }
}
