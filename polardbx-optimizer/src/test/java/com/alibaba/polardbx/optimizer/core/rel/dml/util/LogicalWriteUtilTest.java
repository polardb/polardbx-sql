package com.alibaba.polardbx.optimizer.core.rel.dml.util;

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.DynamicImplicitDefaultHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.ReplaceRexWithParamHandlerCall;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.LogicalWriteUtil.UnpushableFunctionFilter;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.truth.Truth;
import junit.framework.TestCase;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallParam;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

public class LogicalWriteUtilTest extends TestCase {

    private static class TestRexHandlerCall extends BaseRexHandlerCall {

        private final Function<TestRexHandlerCall, RexFilter> rexFilterSupplier;

        public TestRexHandlerCall(RexNode rex,
                                  Function<TestRexHandlerCall, RexFilter> rexFilterSupplier) {
            super(rex);
            this.rexFilterSupplier = rexFilterSupplier;
        }

        private TestRexHandlerCall(RexNode rex, RexNode result,
                                   Function<TestRexHandlerCall, RexFilter> rexFilterSupplier) {
            super(rex);
            this.result = result;
            this.rexFilterSupplier = rexFilterSupplier;
        }

        @Override
        public RexFilter buildRexFilter() {
            return rexFilterSupplier.apply(this);
        }

        @Override
        public RexHandlerCall copy(RexNode newRex, RexNode result) {
            return new TestRexHandlerCall(newRex, result, this.rexFilterSupplier);
        }

        @Override
        public RexHandlerCall copy() {
            return copy(this.rex, this.result);
        }
    }

    public void testUnpushableFunctionFilter() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode unpushableFunction = builder.makeCall(TddlOperatorTable.TSO_TIMESTAMP);
        final RexNode pushableFunction = builder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP);

        final TestRexHandlerCall unpushableFunctionFilterCall =
            new TestRexHandlerCall(unpushableFunction, UnpushableFunctionFilter::new);
        Truth.assertThat(unpushableFunctionFilterCall.buildRexFilter().test(unpushableFunctionFilterCall.getRex()))
            .isTrue();

        final RexHandlerCall pushableFunctionFilterCall =
            unpushableFunctionFilterCall.copy(pushableFunction, null);
        Truth.assertThat(pushableFunctionFilterCall.buildRexFilter().test(pushableFunctionFilterCall.getRex()))
            .isFalse();
    }

    public void testDynamicFunctionFilter() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode staticFunction = builder.makeIntLiteral(1);
        final RexNode dynamicFunction = builder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP);

        final ReplaceRexWithParamHandlerCall dynamicFunctionCall =
            RexHandlerCallFactory.createReplaceLogicalDynamicValueCall(
                dynamicFunction,
                false,
                true,
                false,
                false,
                false,
                null,
                builder,
                new AtomicInteger(1),
                false,
                false);
        Truth.assertThat(dynamicFunctionCall.buildRexFilter().test(dynamicFunctionCall.getRex())).isTrue();

        final RexHandlerCall staticFunctionCall =
            dynamicFunctionCall.copy(staticFunction, null);
        Truth.assertThat(staticFunctionCall.buildRexFilter().test(staticFunctionCall.getRex())).isFalse();
    }

    public void testCombinedFilter() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder builder = new RexBuilder(typeFactory);

        final RexNode staticFunction = builder.makeIntLiteral(1);

        final TestRexHandlerCall trueAndFalseHandlerCall = new TestRexHandlerCall(staticFunction,
            (TestRexHandlerCall handlerCall) -> {

                final TypedRexFilter<RexNode, TestRexHandlerCall> alwaysTrue =
                    new TypedRexFilter<RexNode, TestRexHandlerCall>(RexNode.class,
                        handlerCall) {
                        @Override
                        boolean testWithTypeChecked(RexNode rex) {
                            return true;
                        }
                    };

                final TypedRexFilter<RexNode, TestRexHandlerCall> alwaysFalse =
                    new TypedRexFilter<RexNode, TestRexHandlerCall>(RexNode.class,
                        handlerCall) {
                        @Override
                        boolean testWithTypeChecked(RexNode rex) {
                            return false;
                        }
                    };

                return alwaysTrue.and(alwaysFalse);
            });

        Truth.assertThat(trueAndFalseHandlerCall.buildRexFilter().test(trueAndFalseHandlerCall.getRex())).isFalse();

        final TestRexHandlerCall trueOrFalseHandlerCall = new TestRexHandlerCall(staticFunction,
            (TestRexHandlerCall handlerCall) -> {

                final TypedRexFilter<RexNode, TestRexHandlerCall> alwaysTrue =
                    new TypedRexFilter<RexNode, TestRexHandlerCall>(RexNode.class,
                        handlerCall) {
                        @Override
                        boolean testWithTypeChecked(RexNode rex) {
                            return true;
                        }
                    };

                final TypedRexFilter<RexNode, TestRexHandlerCall> alwaysFalse =
                    new TypedRexFilter<RexNode, TestRexHandlerCall>(RexNode.class,
                        handlerCall) {
                        @Override
                        boolean testWithTypeChecked(RexNode rex) {
                            return true;
                        }
                    };

                return alwaysTrue.or(alwaysFalse);
            });

        Truth.assertThat(trueOrFalseHandlerCall.buildRexFilter().test(trueOrFalseHandlerCall.getRex())).isTrue();

        final TestRexHandlerCall notTrueHandlerCall = new TestRexHandlerCall(staticFunction,
            (TestRexHandlerCall handlerCall) -> {

                final TypedRexFilter<RexNode, TestRexHandlerCall> alwaysTrue =
                    new TypedRexFilter<RexNode, TestRexHandlerCall>(RexNode.class,
                        handlerCall) {
                        @Override
                        boolean testWithTypeChecked(RexNode rex) {
                            return true;
                        }
                    };

                return alwaysTrue.negative();
            });

        Truth.assertThat(notTrueHandlerCall.buildRexFilter().test(notTrueHandlerCall.getRex())).isFalse();
    }

    @Data
    @RequiredArgsConstructor
    private static class RexHandlerCallTestBuilder {
        final String id;
        final boolean mustBeLiteral;
        final boolean mustBeDeterministic;
        final boolean withImplicitDefault;
        final boolean withDynamicImplicitDefault;
        final boolean computeAllDynamicImplicitDefault;
        final RexUtils.NullableState nullableState;
        final AtomicInteger currentMaxParamIndex;

        final RexNode rex;
        final RexNode defaultRex;
        final boolean rexFilterTreeResult;
        final boolean doReplaceCallWithParam;
        final RexNode rexTransformResult;

        public RexHandlerCallTestBuilder(String id,
                                         boolean mustBeLiteral,
                                         boolean mustBeDeterministic,
                                         boolean withImplicitDefault,
                                         boolean withDynamicImplicitDefault,
                                         boolean computeAllDynamicImplicitDefault,
                                         RexNode rex,
                                         RexNode defaultRex,
                                         boolean rexFilterTreeResult,
                                         RexNode rexTransformResult) {
            this(id, mustBeLiteral, mustBeDeterministic, withImplicitDefault, withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault, RexUtils.checkNullableState(rex), new AtomicInteger(0), rex,
                defaultRex, rexFilterTreeResult, rexFilterTreeResult, rexTransformResult);
        }

        public RexHandlerCallTestBuilder(String id,
                                         boolean mustBeLiteral,
                                         boolean mustBeDeterministic,
                                         boolean withImplicitDefault,
                                         boolean withDynamicImplicitDefault,
                                         boolean computeAllDynamicImplicitDefault,
                                         AtomicInteger currentMaxParamIndex,
                                         RexNode rex,
                                         RexNode defaultRex,
                                         boolean rexFilterTreeResult,
                                         RexNode rexTransformResult) {
            this(id, mustBeLiteral, mustBeDeterministic, withImplicitDefault, withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault, RexUtils.checkNullableState(rex), currentMaxParamIndex, rex,
                defaultRex, rexFilterTreeResult, rexFilterTreeResult, rexTransformResult);
        }

        public RexHandlerCallTestBuilder(String id,
                                         boolean mustBeLiteral,
                                         boolean mustBeDeterministic,
                                         boolean withImplicitDefault,
                                         boolean withDynamicImplicitDefault,
                                         boolean computeAllDynamicImplicitDefault,
                                         AtomicInteger currentMaxParamIndex,
                                         RexNode rex,
                                         RexNode defaultRex,
                                         boolean rexFilterTreeResult,
                                         boolean doReplaceCallWithParam,
                                         RexNode rexTransformResult) {
            this(id, mustBeLiteral, mustBeDeterministic, withImplicitDefault, withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault, RexUtils.checkNullableState(rex), currentMaxParamIndex, rex,
                defaultRex, rexFilterTreeResult, doReplaceCallWithParam, rexTransformResult);
        }

        public RexHandlerCallTestBuilder copy(RexNode rex, RexNode defaultRex, boolean rexFilterTreeResult,
                                              boolean doReplaceCallWithParam, RexNode rexTransformResult) {
            return new RexHandlerCallTestBuilder(id,
                mustBeLiteral,
                mustBeDeterministic,
                withImplicitDefault,
                withDynamicImplicitDefault,
                computeAllDynamicImplicitDefault,
                RexUtils.checkNullableState(rex),
                currentMaxParamIndex,
                rex,
                defaultRex,
                rexFilterTreeResult,
                doReplaceCallWithParam,
                rexTransformResult);
        }

        public RexHandlerCall build(
            BiFunction<RexNode, RexHandlerCallTestBuilder, RexHandlerCall> rexHandlerCallCreator) {
            return rexHandlerCallCreator.apply(rex,
                this.copy(rex, defaultRex, rexFilterTreeResult, doReplaceCallWithParam, rexTransformResult));
        }

        public void checkRexHandlerCall(RexHandlerCall handlerCall) {
            final String errMsg = handlerCall.getClass().getSimpleName() + ": " + this;
            Truth.assertWithMessage(errMsg).that(handlerCall.buildRexFilter().test(handlerCall.getRex()))
                .isEqualTo(rexFilterTreeResult);
            Truth.assertWithMessage(errMsg).that(handlerCall.buildTransformer().transform().toString())
                .isEqualTo(rexTransformResult.toString());
        }

        public void checkReplaceRexWithParamHandlerCall(ReplaceRexWithParamHandlerCall handlerCall) {
            final String errMsg = handlerCall.getClass().getSimpleName() + ": " + this;
            Truth.assertWithMessage(errMsg).that(handlerCall.buildRexFilterTree().test(handlerCall.getRex()))
                .isEqualTo(rexFilterTreeResult);
            final RexNode transformed = handlerCall.buildTransformer().transform();
            checkReplaceRexWithParamResult(errMsg, transformed, doReplaceCallWithParam);
        }

        public void checkReplaceRexWithParamHandlerChain(RexHandlerChain<ReplaceRexWithParamHandlerCall> handlerChain,
                                                         ReplaceRexWithParamHandlerCall replaceRexWithParamCall) {
            final RexHandlerCall processed = handlerChain.process(replaceRexWithParamCall);

            final String errMsg = replaceRexWithParamCall.getClass().getSimpleName() + ": " + this;
            final RexNode transformed = processed.getResult();
            checkReplaceRexWithParamResult(errMsg, transformed, rexFilterTreeResult);
        }

        private void checkReplaceRexWithParamResult(String errMsg, RexNode transformed, boolean rexFilterResult) {
            if (rexFilterResult) {
                Truth.assertWithMessage(errMsg).that(transformed).isInstanceOf(RexCallParam.class);
                final RexCallParam rexCallParam = (RexCallParam) transformed;
                Truth.assertWithMessage(errMsg).that(rexCallParam.getIndex()).isEqualTo(currentMaxParamIndex.get());
                Truth.assertWithMessage(errMsg).that(rexCallParam.getRexCall().toString())
                    .isEqualTo(rexTransformResult.toString());
                if (rexCallParam.getRexCall() instanceof RexDynamicParam) {
                    Truth.assertWithMessage(errMsg).that(rexCallParam.isWrapRexDynamicParam()).isTrue();
                }
            } else {
                Truth.assertWithMessage(errMsg).that(transformed.toString())
                    .isEqualTo(rexTransformResult.toString());
            }
        }
    }

    private static final List<RexHandlerCallTestBuilder> rexDynamicImplicitDefaultHandlerCallBuilders =
        new ArrayList<>();

    static {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        final RexNode defaultRex = rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP);
        final RexNode nullRex = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        final RexNode nonNullRex = rexBuilder.makeIntLiteral(7527);
        final RexCall rexCall = (RexCall) rexBuilder.makeCall(TddlOperatorTable.PLUS, nullRex, nonNullRex);

        final ImmutableList<RexHandlerCallTestBuilder> rexHandlerCallTestBuilders = ImmutableList.of(
            new RexHandlerCallTestBuilder("1.1.1", false, false, false, false, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.1.2", false, false, false, false, false, nonNullRex, defaultRex, false,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.2.1", false, false, true, false, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.2.2", false, false, true, false, false, rexCall, defaultRex, false,
                rexCall),
            new RexHandlerCallTestBuilder("1.3.1", false, false, true, true, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.3.1", false, false, true, true, true, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.3.2", false, false, true, true, true, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.3.3", false, false, true, true, true, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.3.4", false, false, true, true, true, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.4.1", false, true, false, false, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.5.1", false, true, true, false, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.6.1", false, true, true, true, false, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.6.2", false, true, true, true, false, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.6.3", false, true, true, true, false, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.6.4", false, true, true, true, false, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.7.1", false, true, true, true, true, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.7.2", false, true, true, true, true, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.7.3", false, true, true, true, true, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.7.4", false, true, true, true, true, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.8.1", true, false, false, false, false, nullRex, defaultRex, false,
                nullRex),
            new RexHandlerCallTestBuilder("1.9.1", true, false, true, false, false, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.9.2", true, false, true, false, false, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.9.3", true, false, true, false, false, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.9.4", true, false, true, false, false, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.10.1", true, false, true, true, false, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.10.2", true, false, true, true, false, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.10.3", true, false, true, true, false, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.10.4", true, false, true, true, false, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.11.1", true, false, true, true, true, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.11.2", true, false, true, true, true, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.11.3", true, false, true, true, true, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.11.4", true, false, true, true, true, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("1.12.1", true, true, true, true, true, nullRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.12.2", true, true, true, true, true, nonNullRex, defaultRex, true,
                nonNullRex),
            new RexHandlerCallTestBuilder("1.12.3", true, true, true, true, true, defaultRex, defaultRex, true,
                defaultRex),
            new RexHandlerCallTestBuilder("1.12.4", true, true, true, true, true, rexCall, defaultRex, true,
                RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder))
        );

        rexDynamicImplicitDefaultHandlerCallBuilders.addAll(rexHandlerCallTestBuilders);
    }

    public void testDynamicImplicitDefaultHandlerCall() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        for (final RexHandlerCallTestBuilder rexHandlerCallBuilder : rexDynamicImplicitDefaultHandlerCallBuilders) {
            final DynamicImplicitDefaultHandlerCall dynamicFunctionCall =
                (DynamicImplicitDefaultHandlerCall) rexHandlerCallBuilder.build(
                    (RexNode rex, RexHandlerCallTestBuilder handlerCallBuilder) ->
                        RexHandlerCallFactory.createDynamicImplicitDefaultHandlerCall(
                            handlerCallBuilder.rex,
                            handlerCallBuilder.mustBeLiteral,
                            handlerCallBuilder.mustBeDeterministic,
                            handlerCallBuilder.withImplicitDefault,
                            handlerCallBuilder.withDynamicImplicitDefault,
                            handlerCallBuilder.computeAllDynamicImplicitDefault,
                            handlerCallBuilder.defaultRex,
                            rexBuilder,
                            handlerCallBuilder.nullableState));

            rexHandlerCallBuilder.checkRexHandlerCall(dynamicFunctionCall);
        }
    }

    public void testDynamicImplicitDefaultHandler() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        final RexHandlerChain<DynamicImplicitDefaultHandlerCall> handlerChain = RexHandlerChain.create(
            RexHandlerFactory.DYNAMIC_IMPLICIT_DEFAULT_ALL_HANDLERS);

        for (final RexHandlerCallTestBuilder rexHandlerCallBuilder : rexDynamicImplicitDefaultHandlerCallBuilders) {
            final DynamicImplicitDefaultHandlerCall dynamicFunctionCall =
                (DynamicImplicitDefaultHandlerCall) rexHandlerCallBuilder.build(
                    (RexNode rex, RexHandlerCallTestBuilder handlerCallBuilder) ->
                        RexHandlerCallFactory.createDynamicImplicitDefaultHandlerCall(
                            handlerCallBuilder.rex,
                            handlerCallBuilder.mustBeLiteral,
                            handlerCallBuilder.mustBeDeterministic,
                            handlerCallBuilder.withImplicitDefault,
                            handlerCallBuilder.withDynamicImplicitDefault,
                            handlerCallBuilder.computeAllDynamicImplicitDefault,
                            handlerCallBuilder.defaultRex,
                            rexBuilder,
                            handlerCallBuilder.nullableState));

            final RexHandlerCall processed = handlerChain.process(dynamicFunctionCall);

            final String errMsg = dynamicFunctionCall.getClass().getSimpleName() + ": " + rexHandlerCallBuilder;
            Truth.assertWithMessage(errMsg).that(processed.getResult().toString())
                .isEqualTo(rexHandlerCallBuilder.rexTransformResult.toString());
        }
    }

    private static final List<RexHandlerCallTestBuilder> rexReplaceRexWithParamHandlerCallBuilders =
        new ArrayList<>();

    static {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        final RexNode defaultRex = rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP);
        final RexNode nullRex = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        final RexNode nonNullLiteral = rexBuilder.makeIntLiteral(7527);
        final RexNode nullableRex = rexBuilder.makeCall(TddlOperatorTable.PLUS, nullRex, nonNullLiteral);

        final RexLiteral rexLiteral = rexBuilder.makeLiteral("null");
        final RexInputRef rexInputRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        final RexDynamicParam rexDynamicParam =
            rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);

        final AtomicInteger nextParamIndex = new AtomicInteger(0);

        final ImmutableList<RexHandlerCallTestBuilder> rexHandlerCallTestBuilders = ImmutableList.of(
            new RexHandlerCallTestBuilder("2.1.1", false, false, false, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("2.1.2", false, false, false, false, false, nextParamIndex, nonNullLiteral,
                defaultRex, false, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.2.1", false, false, true, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("2.2.2", false, false, true, false, false, nextParamIndex, nullableRex,
                defaultRex, false, nullableRex),
            new RexHandlerCallTestBuilder("2.3.1", false, false, true, true, false, nextParamIndex, nullRex, defaultRex,
                false, nullRex),
            new RexHandlerCallTestBuilder("2.4.1", false, false, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, false, defaultRex),
            new RexHandlerCallTestBuilder("2.4.2", false, false, true, true, true, nextParamIndex, nonNullLiteral,
                defaultRex, true, false, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.4.3", false, false, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, false, defaultRex),
            new RexHandlerCallTestBuilder("2.4.4", false, false, true, true, true, nextParamIndex, nullableRex,
                defaultRex, true, false, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.5.1", false, true, false, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("2.6.1", false, true, true, false, false, nextParamIndex, nullRex, defaultRex,
                false, nullRex),
            new RexHandlerCallTestBuilder("2.7.1", false, true, true, true, false, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.7.2", false, true, true, true, false, nextParamIndex, nonNullLiteral,
                defaultRex, true, false, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.7.3", false, true, true, true, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.7.4", false, true, true, true, false, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.8.1", false, true, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.8.2", false, true, true, true, true, nextParamIndex, nonNullLiteral,
                defaultRex, true, false, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.8.3", false, true, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.8.4", false, true, true, true, true, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.9.1", true, false, false, false, false, nextParamIndex, nullRex,
                defaultRex, true, nullRex),
            new RexHandlerCallTestBuilder("2.10.1", true, false, true, false, false, nextParamIndex, nullRex,
                defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.10.2", true, false, true, false, false, nextParamIndex, nonNullLiteral,
                defaultRex, true, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.10.3", true, false, true, false, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.10.4", true, false, true, false, false, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.11.1", true, false, true, true, false, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.11.2", true, false, true, true, false, nextParamIndex, nonNullLiteral,
                defaultRex, true, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.11.3", true, false, true, true, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.11.4", true, false, true, true, false, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.12.1", true, false, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.12.2", true, false, true, true, true, nextParamIndex, nonNullLiteral,
                defaultRex, true, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.12.3", true, false, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.12.4", true, false, true, true, true, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("2.13.1", true, true, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("2.13.2", true, true, true, true, true, nextParamIndex, nonNullLiteral,
                defaultRex, true, nonNullLiteral),
            new RexHandlerCallTestBuilder("2.13.3", true, true, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("2.13.4", true, true, true, true, true, nextParamIndex, nullableRex,
                defaultRex, true, RexUtils.ifNullDefault(nullableRex, defaultRex, rexBuilder))
        );

        rexReplaceRexWithParamHandlerCallBuilders.addAll(rexHandlerCallTestBuilders);
    }

    public void testReplaceRexWithParamHandlerCall() {

        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        for (final RexHandlerCallTestBuilder rexHandlerCallBuilder : rexReplaceRexWithParamHandlerCallBuilders) {
            final ReplaceRexWithParamHandlerCall replaceRexWithParamCall =
                (ReplaceRexWithParamHandlerCall) rexHandlerCallBuilder.build(
                    (RexNode rex, RexHandlerCallTestBuilder handlerCallBuilder) ->
                        RexHandlerCallFactory.createReplaceLogicalDynamicValueCall(
                            handlerCallBuilder.rex,
                            handlerCallBuilder.mustBeLiteral,
                            handlerCallBuilder.mustBeDeterministic,
                            handlerCallBuilder.withImplicitDefault,
                            handlerCallBuilder.withDynamicImplicitDefault,
                            handlerCallBuilder.computeAllDynamicImplicitDefault,
                            handlerCallBuilder.defaultRex,
                            rexBuilder,
                            handlerCallBuilder.currentMaxParamIndex,
                            false,
                            false));

            rexHandlerCallBuilder.checkReplaceRexWithParamHandlerCall(replaceRexWithParamCall);
        }
    }

    private static final List<RexHandlerCallTestBuilder> rexReplaceRexWithParamHandlerBuilders =
        new ArrayList<>();

    static {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        final RexNode defaultRex = rexBuilder.makeCall(TddlOperatorTable.CURRENT_TIMESTAMP);
        final RexNode nullRex = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        final RexNode nonNullRex = rexBuilder.makeIntLiteral(7527);

        final RexLiteral rexLiteral = rexBuilder.makeLiteral("null");
        final RexInputRef rexInputRef = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        final RexDynamicParam rexDynamicParam =
            rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), 0);
        final RexNode rexCall = rexBuilder.makeCall(TddlOperatorTable.PLUS, nullRex, nonNullRex);

        final AtomicInteger nextParamIndex = new AtomicInteger(0);

        final ImmutableList<RexHandlerCallTestBuilder> rexHandlerCallTestBuilders = ImmutableList.of(
            new RexHandlerCallTestBuilder("3.1.1", false, false, false, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("3.1.2", false, false, false, false, false, nextParamIndex, nonNullRex,
                defaultRex, false, nonNullRex),
            new RexHandlerCallTestBuilder("3.1.3", false, false, false, false, false, nextParamIndex, rexInputRef,
                defaultRex, true, rexInputRef),
            new RexHandlerCallTestBuilder("3.2.1", false, false, true, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("3.2.2", false, false, true, false, false, nextParamIndex, rexCall,
                defaultRex, false, rexCall),
            new RexHandlerCallTestBuilder("3.2.3", false, false, true, false, false, nextParamIndex, rexInputRef,
                defaultRex, true, rexInputRef),
            new RexHandlerCallTestBuilder("3.3.1", false, false, true, true, false, nextParamIndex, nullRex, defaultRex,
                false, nullRex),
            new RexHandlerCallTestBuilder("3.3.2", false, false, true, true, false, nextParamIndex, rexInputRef,
                defaultRex, true, rexInputRef),
            new RexHandlerCallTestBuilder("3.4.1", false, false, true, true, true, nextParamIndex, nullRex, defaultRex,
                false, defaultRex),
            new RexHandlerCallTestBuilder("3.4.2", false, false, true, true, true, nextParamIndex, nonNullRex,
                defaultRex, false, nonNullRex),
            new RexHandlerCallTestBuilder("3.4.3", false, false, true, true, true, nextParamIndex, defaultRex,
                defaultRex, false, defaultRex),
            new RexHandlerCallTestBuilder("3.4.4", false, false, true, true, true, nextParamIndex, rexCall,
                defaultRex, false, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.4.5", false, false, true, true, true, nextParamIndex, rexInputRef,
                defaultRex, true, RexUtils.ifNullDefault(rexInputRef, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.4.6", false, false, true, true, true, nextParamIndex, rexLiteral,
                defaultRex, false, rexLiteral),
            new RexHandlerCallTestBuilder("3.4.7", false, false, true, true, true, nextParamIndex, rexDynamicParam,
                defaultRex, false, RexUtils.ifNullDefault(rexDynamicParam, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.5.1", false, true, false, false, false, nextParamIndex, nullRex,
                defaultRex, false, nullRex),
            new RexHandlerCallTestBuilder("3.5.2", false, true, false, false, false, nextParamIndex, rexInputRef,
                defaultRex, true, rexInputRef),
            new RexHandlerCallTestBuilder("3.6.1", false, true, true, false, false, nextParamIndex, nullRex, defaultRex,
                false, nullRex),
            new RexHandlerCallTestBuilder("3.6.2", false, true, true, false, false, nextParamIndex, rexInputRef,
                defaultRex, true, rexInputRef),
            new RexHandlerCallTestBuilder("3.7.1", false, true, true, true, false, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("3.7.2", false, true, true, true, false, nextParamIndex, nonNullRex,
                defaultRex, false, nonNullRex),
            new RexHandlerCallTestBuilder("3.7.3", false, true, true, true, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.7.4", false, true, true, true, false, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.7.5", false, true, true, true, false, nextParamIndex, rexInputRef,
                defaultRex, true, RexUtils.ifNullDefault(rexInputRef, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.7.6", false, true, true, true, false, nextParamIndex, rexLiteral,
                defaultRex, false, rexLiteral),
            new RexHandlerCallTestBuilder("3.7.7", false, true, true, true, false, nextParamIndex, rexDynamicParam,
                defaultRex, true, RexUtils.ifNullDefault(rexDynamicParam, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.8.1", false, true, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("3.8.2", false, true, true, true, true, nextParamIndex, nonNullRex,
                defaultRex, false, nonNullRex),
            new RexHandlerCallTestBuilder("3.8.3", false, true, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.8.4", false, true, true, true, true, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.8.5", false, true, true, true, true, nextParamIndex, rexInputRef,
                defaultRex, true, RexUtils.ifNullDefault(rexInputRef, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.8.6", false, true, true, true, true, nextParamIndex, rexLiteral,
                defaultRex, false, rexLiteral),
            new RexHandlerCallTestBuilder("3.8.7", false, true, true, true, true, nextParamIndex, rexDynamicParam,
                defaultRex, true, RexUtils.ifNullDefault(rexDynamicParam, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.9.1", true, false, false, false, false, nextParamIndex, nullRex,
                defaultRex, true, nullRex),
            new RexHandlerCallTestBuilder("3.10.1", true, false, true, false, false, nextParamIndex, nullRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.10.2", true, false, true, false, false, nextParamIndex, nonNullRex,
                defaultRex, true, nonNullRex),
            new RexHandlerCallTestBuilder("3.10.3", true, false, true, false, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.10.4", true, false, true, false, false, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.11.1", true, false, true, true, false, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("3.11.2", true, false, true, true, false, nextParamIndex, nonNullRex,
                defaultRex, true, nonNullRex),
            new RexHandlerCallTestBuilder("3.11.3", true, false, true, true, false, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.11.4", true, false, true, true, false, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.12.1", true, false, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("3.12.2", true, false, true, true, true, nextParamIndex, nonNullRex,
                defaultRex, true, nonNullRex),
            new RexHandlerCallTestBuilder("3.12.3", true, false, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.12.4", true, false, true, true, true, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder)),
            new RexHandlerCallTestBuilder("3.13.1", true, true, true, true, true, nextParamIndex, nullRex, defaultRex,
                true, defaultRex),
            new RexHandlerCallTestBuilder("3.13.2", true, true, true, true, true, nextParamIndex, nonNullRex,
                defaultRex, true, nonNullRex),
            new RexHandlerCallTestBuilder("3.13.3", true, true, true, true, true, nextParamIndex, defaultRex,
                defaultRex, true, defaultRex),
            new RexHandlerCallTestBuilder("3.13.4", true, true, true, true, true, nextParamIndex, rexCall,
                defaultRex, true, RexUtils.ifNullDefault(rexCall, defaultRex, rexBuilder))
        );

        rexReplaceRexWithParamHandlerBuilders.addAll(rexHandlerCallTestBuilders);
    }

    public void testReplaceRexWithParamHandler() {
        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);

        final RexHandlerChain<ReplaceRexWithParamHandlerCall> handlerChain = RexHandlerChain.create(
            RexHandlerFactory.REPLACE_REX_WITH_PARAM_ALL_HANDLERS);

        for (final RexHandlerCallTestBuilder rexHandlerCallBuilder : rexReplaceRexWithParamHandlerBuilders) {
            final ReplaceRexWithParamHandlerCall replaceRexWithParamCall =
                (ReplaceRexWithParamHandlerCall) rexHandlerCallBuilder.build(
                    (RexNode rex, RexHandlerCallTestBuilder handlerCallBuilder) ->
                        RexHandlerCallFactory.createReplaceLogicalDynamicValueCall(
                            handlerCallBuilder.rex,
                            handlerCallBuilder.mustBeLiteral,
                            handlerCallBuilder.mustBeDeterministic,
                            handlerCallBuilder.withImplicitDefault,
                            handlerCallBuilder.withDynamicImplicitDefault,
                            handlerCallBuilder.computeAllDynamicImplicitDefault,
                            handlerCallBuilder.defaultRex,
                            rexBuilder,
                            handlerCallBuilder.currentMaxParamIndex,
                            false,
                            false));

            rexHandlerCallBuilder.checkReplaceRexWithParamHandlerChain(handlerChain, replaceRexWithParamCall);
        }
    }

}