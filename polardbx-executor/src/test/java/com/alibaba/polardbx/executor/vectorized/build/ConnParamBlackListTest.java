package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlPrefixOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnParamBlackListTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void test() {
        ExecutionContext executionContext = new ExecutionContext();
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CONSTANT_FOLD_BLACKLIST.getName(), ">, +, <=, %, LIKE , NOT LIKE, XXX");
        connectionMap.put(ConnectionParams.CONSTANT_FOLD_WHITELIST.getName(), "ST_ConvexHull, JSON_UNQUOTE,TRIM ,YYY");
        executionContext.setParamManager(new ParamManager(connectionMap));

        ExpressionRewriter rewriter = new ExpressionRewriter(executionContext);

        List<SqlOperator> blacklist = rewriter.getExtraBlacklist();
        List<SqlOperator> whitelist = rewriter.getExtraWhitelist();

        Assert.assertTrue(blacklist.contains(TddlOperatorTable.GREATER_THAN));
        Assert.assertTrue(blacklist.contains(TddlOperatorTable.PLUS));
        Assert.assertTrue(blacklist.contains(TddlOperatorTable.LESS_THAN_OR_EQUAL));
        Assert.assertTrue(blacklist.contains(TddlOperatorTable.PERCENT_REMAINDER));
        Assert.assertTrue(blacklist.contains(TddlOperatorTable.LIKE));
        Assert.assertTrue(blacklist.contains(TddlOperatorTable.NOT_LIKE));

        Assert.assertTrue(whitelist.contains(TddlOperatorTable.ST_ConvexHull));
        Assert.assertTrue(whitelist.contains(TddlOperatorTable.JSON_UNQUOTE));
        Assert.assertTrue(whitelist.contains(TddlOperatorTable.TRIM));

        Assert.assertFalse(
            FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.GREATER_THAN), blacklist, whitelist));
        Assert.assertFalse(FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.PLUS), blacklist, whitelist));
        Assert.assertFalse(
            FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.LESS_THAN_OR_EQUAL), blacklist, whitelist));
        Assert.assertFalse(
            FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.PERCENT_REMAINDER), blacklist, whitelist));
        Assert.assertFalse(FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.LIKE), blacklist, whitelist));
        Assert.assertFalse(FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.NOT_LIKE), blacklist, whitelist));

        Assert.assertTrue(
            FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.ST_ConvexHull), blacklist, whitelist));
        Assert.assertTrue(
            FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.JSON_UNQUOTE), blacklist, whitelist));
        Assert.assertTrue(FoldFunctionUtils.isFoldEnabled(makeCall(TddlOperatorTable.TRIM), blacklist, whitelist));
    }

    public RexCall makeCall(SqlOperator operator) {
        List<RexNode> operands;

        // Check if the operator is a binary operator
        if (operator instanceof SqlBinaryOperator) {
            // Create operands with literals "1" and "2"
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"), REX_BUILDER.makeLiteral("2"));
        }
        // Check if the operator is EXISTS or NOT_EXISTS
        else if (operator == TddlOperatorTable.EXISTS || operator == TddlOperatorTable.NOT_EXISTS) {
            // No operands for EXISTS or NOT_EXISTS
            operands = ImmutableList.of();
        }
        // For other cases, create a single operand with literal "1"
        else {
            operands = ImmutableList.of(REX_BUILDER.makeLiteral("1"));
        }

        // Create a RexCall with the specified operator and operands
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), operator, operands);
        return call;
    }
}
