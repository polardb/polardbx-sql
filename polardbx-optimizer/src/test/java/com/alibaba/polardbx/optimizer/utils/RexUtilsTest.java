package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.BaseRexHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.RexFilter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.TypedRexFilter;
import com.alibaba.polardbx.optimizer.utils.RexUtils.NullableState;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class RexUtilsTest {
    @Test
    public void testIndexHintSerialization() {
        List<String> testLines = Lists.newArrayList();
        testLines.add("FORCE INDEXIDX_NAME");
        testLines.add("IGNORE INDEXIDXID");
        testLines.add("use INDEXIDXID1,name1,name2");

        SqlNode node = RexUtil.deSeriIndexHint(testLines);
        System.out.println(node);
        assert node instanceof SqlNodeList;
        assert ((SqlNodeList) node).getList().size() == 3;
        String rs = node.toSqlString(MysqlSqlDialect.DEFAULT).toString().toUpperCase(Locale.ROOT);
        assert rs.contains("FORCE INDEX(IDX_NAME)");
        assert rs.contains("IGNORE INDEX(IDXID)");
        assert rs.contains("USE INDEX(IDXID1, NAME1, NAME2)");
    }

    @Test
    public void testHandleDefaultExpr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexNode rexNode = mock(RexNode.class);
                Slice slice = mock(Slice.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexNode));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexNode)).thenReturn(slice);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);
                when(slice.getBytes()).thenReturn(new byte[] {1, 2, 3});

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(evalFunc).apply(rexNode);
                verify(slice).getBytes();

                ByteString str = mock(ByteString.class);
                when(str.getBytes()).thenReturn(new byte[] {1, 2, 3});
                when(evalFunc.apply(rexNode)).thenReturn(str);
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(str).getBytes();
            }
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testHandleDefaultExprThrowErr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexCall rexCall = mock(RexCall.class);
                RexNode rexNode = mock(RexNode.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);
                List<RexNode> operands = mock(List.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexCall));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));
                when(rexCall.getOperands()).thenReturn(operands);
                when(operands.get(0)).thenReturn(rexNode);
                when(rexNode.toString()).thenReturn("UUID_TO_BIN");

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexCall)).thenThrow(UnsupportedOperationException.class);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
            }
        }
    }

    @Test
    public void testCheckNullableState() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode nullLiteral = builder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        checkNullState(nullLiteral, NullableState.IS_NULL);

        final RexNode castOfNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullLiteral);
        checkNullState(castOfNull, NullableState.IS_NULL);

        final RexNode nonNullLiteral = builder.makeLiteral("a");
        checkNullState(nonNullLiteral, NullableState.IS_NOT_NULL);

        final RexNode castOfNonNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nonNullLiteral);
        checkNullState(castOfNonNull, NullableState.IS_NOT_NULL);

        final RexNode nullableRex = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 1);
        checkNullState(nullableRex, NullableState.BOTH);

        final RexNode nullableRexCall = builder.makeCall(SqlStdOperatorTable.PLUS, nonNullLiteral, nullLiteral);
        checkNullState(nullableRexCall, NullableState.BOTH);
    }

    private static void checkNullState(RexNode nullLiteral, NullableState expected) {
        NullableState nullableState;
        nullableState = RexUtils.checkNullableState(nullLiteral);
        Truth.assertThat(nullableState).isEqualTo(expected);
    }

    @Test
    public void testNullableState() {
        Truth.assertThat(NullableState.IS_NULL.isNull()).isTrue();
        Truth.assertThat(NullableState.IS_NULL.isNullable()).isTrue();
        Truth.assertThat(NullableState.IS_NULL.isNotNull()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNull()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNullable()).isFalse();
        Truth.assertThat(NullableState.IS_NOT_NULL.isNotNull()).isTrue();
        Truth.assertThat(NullableState.BOTH.isNull()).isFalse();
        Truth.assertThat(NullableState.BOTH.isNullable()).isTrue();
        Truth.assertThat(NullableState.BOTH.isNotNull()).isFalse();
    }

    @Test
    public void testIfNullDefault() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode defaultRex = builder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP);

        final RexNode nullLiteral = builder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        checkWrapper(nullLiteral, defaultRex, builder, defaultRex, NullableState.IS_NOT_NULL);

        final RexNode castOfNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nullLiteral);
        checkWrapper(castOfNull, defaultRex, builder, defaultRex, NullableState.IS_NOT_NULL);

        final RexNode nonNullLiteral = builder.makeLiteral("a");
        checkWrapper(nonNullLiteral, defaultRex, builder, nonNullLiteral, NullableState.IS_NOT_NULL);

        final RexNode castOfNonNull = builder.makeCast(typeFactory.createSqlType(SqlTypeName.BOOLEAN), nonNullLiteral);
        checkWrapper(castOfNonNull, defaultRex, builder, castOfNonNull, NullableState.IS_NOT_NULL);

        final RexNode nullableRex = builder.makeInputRef(typeFactory.createSqlType(SqlTypeName.BOOLEAN), 1);
        checkWrapper(nullableRex,
            defaultRex,
            builder,
            builder.makeCall(nullableRex.getType(),
                TddlOperatorTable.IFNULL,
                ImmutableList.of(nullableRex, defaultRex)),
            NullableState.IS_NOT_NULL);

        final RexNode nullableRexCall = builder.makeCall(SqlStdOperatorTable.PLUS, nonNullLiteral, nullLiteral);
        checkWrapper(nullableRexCall,
            defaultRex,
            builder,
            builder.makeCall(nullableRexCall.getType(),
                TddlOperatorTable.IFNULL,
                ImmutableList.of(nullableRexCall, defaultRex)),
            NullableState.IS_NOT_NULL);

        final RexNode currentTimestamp1 = defaultRex;
        checkWrapper(currentTimestamp1, defaultRex, builder, currentTimestamp1, NullableState.IS_NOT_NULL);

        final RexNode currentTimestamp2 =
            builder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP, builder.makeIntLiteral(6));
        checkWrapper(currentTimestamp2, defaultRex, builder, currentTimestamp2, NullableState.IS_NOT_NULL);

        final RexNode specialDefault =
            builder.makeCall(SqlStdOperatorTable.PLUS, builder.makeIntLiteral(6), builder.makeIntLiteral(1));
        checkWrapper(specialDefault, specialDefault, builder, specialDefault, NullableState.BOTH);
    }

    private static void checkWrapper(RexNode rex, RexNode defaultRex, RexBuilder builder,
                                     RexNode expectedRexNode, NullableState expectedState) {
        // Check ifNullDefault wrapper
        final RexNode wrapped = RexUtils.ifNullDefault(rex, defaultRex, builder);
        Truth.assertThat(wrapped.toString()).isEqualTo(expectedRexNode.toString());
        checkNullState(wrapped, expectedState);

        // Check wrapWithRexCallParam
        final AtomicInteger nextParamIndex = new AtomicInteger(0);
        final RexCallParam rexCallParam = RexUtils.wrapWithRexCallParam(wrapped, nextParamIndex);
        Truth.assertThat(rexCallParam.getIndex()).isEqualTo(nextParamIndex.get());
        Truth.assertThat(rexCallParam.toString()).isEqualTo("?" + nextParamIndex.get());

        final BaseRexHandlerCall rexHandlerCall = new BaseRexHandlerCall(rex);
        final RexFilter rexFilter = TypedRexFilter.of(rexHandlerCall, (r, c) -> true);

        // Check ifNullDefault decorator
        RexUtils.BaseRexNodeTransformer baseTransformer = new RexUtils.BaseRexNodeTransformer(rex);
        final RexUtils.IfNullDefaultDecorator ifNullDefaultDecorator =
            new RexUtils.IfNullDefaultDecorator(baseTransformer, rexFilter, defaultRex, builder, null);
        RexNode ifNullDefaultTransformed = ifNullDefaultDecorator.transform();
        Truth.assertThat(ifNullDefaultTransformed.toString()).isEqualTo(expectedRexNode.toString());
        checkNullState(ifNullDefaultTransformed, expectedState);

        // Check wrapWithRexCallParam
        baseTransformer = new RexUtils.BaseRexNodeTransformer(rex);
        final RexUtils.RexCallParamDecorator rexCallParamDecorator =
            new RexUtils.RexCallParamDecorator(
                new RexUtils.IfNullDefaultDecorator(baseTransformer, rexFilter, defaultRex, builder, null),
                rexFilter,
                nextParamIndex,
                rex instanceof RexDynamicParam);
        final RexNode rexCallParamTransformed = rexCallParamDecorator.transform();
        Truth.assertThat(rexCallParamTransformed).isInstanceOf(RexCallParam.class);
        Truth.assertThat(((RexCallParam) rexCallParamTransformed).getIndex()).isEqualTo(nextParamIndex.get());
        Truth.assertThat(((RexCallParam) rexCallParamTransformed).getRexCall().toString())
            .isEqualTo(expectedRexNode.toString());
        checkNullState(((RexCallParam) rexCallParamTransformed).getRexCall(), expectedState);
        Truth.assertThat(rexCallParamTransformed.toString()).isEqualTo("?" + nextParamIndex.get());
    }
}
