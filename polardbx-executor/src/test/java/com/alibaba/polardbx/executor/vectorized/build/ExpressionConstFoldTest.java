package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

public class ExpressionConstFoldTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private ExecutionContext context = new ExecutionContext();

    @Test
    public void test1() {
        // 99 + 1
        RexCall needFold = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.PLUS,
            ImmutableList.of(
                REX_BUILDER.makeBigIntLiteral(99L),
                REX_BUILDER.makeBigIntLiteral(1L)
            ));

        // int_col = (99 + 1)
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0),
                needFold
            ));

        doTest(call, ImmutableList.of(DataTypes.IntegerType),
            "EQIntegerColLongConstVectorizedExpression", DataTypes.LongType);
    }

    @Test
    public void test2() {
        // 99.0 + 1
        RexCall needFold = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL),
            TddlOperatorTable.PLUS,
            ImmutableList.of(
                REX_BUILDER.makeExactLiteral(new BigDecimal("99.0")),
                REX_BUILDER.makeBigIntLiteral(1L)
            ));

        // decimal = (99.0 + 1)
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 15, 2), 0),
                needFold
            ));

        doTest(call, ImmutableList.of(new DecimalType(15, 2)),
            "EQDecimalColDecimalConstVectorizedExpression", DataTypes.LongType);
    }

    @Test
    public void test3() {

        RexCall needFold = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.CHAR),
            TddlOperatorTable.CONCAT,
            ImmutableList.of(
                REX_BUILDER.makeLiteral("2011-11-11 11:11:11")
            )
        );

        // datetime_col = concat("2011-11-11 11:11:11")
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.EQUALS,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.DATETIME, 6), 0),
                needFold
            ));

        doTest(call, ImmutableList.of(new DateTimeType(6)),
            "EQDatetimeColCharConstVectorizedExpression", DataTypes.LongType);
    }

    @Test
    public void testNull1() {
        // 1 + null
        RexCall needFold = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.PLUS,
            ImmutableList.of(
                REX_BUILDER.makeBigIntLiteral(1L),
                REX_BUILDER.makeNullLiteral(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT))
            )
        );

        // integer_col < 1 + null
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            TddlOperatorTable.LESS_THAN,
            ImmutableList.of(
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0),
                needFold
            ));

        doTest(call, ImmutableList.of(DataTypes.IntegerType),
            "LTIntegerColLongConstVectorizedExpression", DataTypes.LongType);
    }

    private void doTest(RexCall call, List<DataType<?>> inputTypes,
                        String expectedClass, DataType<?> expectedType) {

        RexNode root = VectorizedExpressionBuilder.rewriteRoot(call, true);

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());
        VectorizedExpression condition = root.accept(converter);
        List<DataType<?>> filterOutputTypes = converter.getOutputDataTypes();

        Assert.assertTrue(String.format("expected class = %s, actual class = %s", expectedClass, condition.getClass()),
            condition.getClass().toString().contains(expectedClass));
        Assert.assertTrue(String.format("expected type = %s, actual type = %s", expectedType,
                filterOutputTypes.get(filterOutputTypes.size() - 1)),
            DataTypeUtil.equalsSemantically(expectedType, filterOutputTypes.get(filterOutputTypes.size() - 1)));
    }
}
