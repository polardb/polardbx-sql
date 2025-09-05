/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.BenchmarkVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.BuiltInFunctionVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.CaseVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.CoalesceVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.InEmptyListVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.InValuesVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.InputRefVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.NotInEmptyListVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionRegistry;
import com.alibaba.polardbx.executor.vectorized.compare.FastInVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.compare.FastNotInVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ArgumentInfo;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionConstructor;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionMode;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignature;
import com.alibaba.polardbx.executor.vectorized.metadata.Rex2ArgumentInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.expression.ExtraFunctionManager;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSystemVar;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.IntervalString;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT_UNSIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.SIGNED;
import static org.apache.calcite.sql.type.SqlTypeName.UNSIGNED;

/**
 * Visit the rational expression tree and binding to vectorized expression node-by-node.
 * Don't support sub query util now.
 */
public class Rex2VectorizedExpressionVisitor extends RexVisitorImpl<VectorizedExpression> {
    /**
     * Special vectorized expressions.
     */
    public static final Map<SqlOperator, Class<? extends VectorizedExpression>> SPECIAL_VECTORIZED_EXPRESSION_MAPPING =
        ImmutableMap
            .<SqlOperator, Class<? extends VectorizedExpression>>builder()
            .put(TddlOperatorTable.CASE, CaseVectorizedExpression.class)
            .put(TddlOperatorTable.COALESCE, CoalesceVectorizedExpression.class)
            .put(TddlOperatorTable.BENCHMARK, BenchmarkVectorizedExpression.class)
            .build();
    public static final String CAST_TO_DOUBLE = "CastToDouble";
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final int MAX_VARCHAR_CODEGEN_IN_NUMS = 100;
    private static final String CAST_TO_DECIMAL = "CastToDecimal";
    private static final String CAST_TO_UNSIGNED = "CastToUnsigned";
    private static final String CAST_TO_SIGNED = "CastToSigned";
    /**
     * The vectorized function names of the Cast function
     */
    public static final Map<SqlTypeName, String> VECTORIZED_CAST_FUNCTION_NAMES =
        ImmutableMap
            .<SqlTypeName, String>builder()
            .put(DECIMAL, CAST_TO_DECIMAL)
            .put(BIGINT_UNSIGNED, CAST_TO_UNSIGNED)
            .put(BIGINT, CAST_TO_SIGNED)
            .put(SIGNED, CAST_TO_SIGNED)
            .put(UNSIGNED, CAST_TO_UNSIGNED)
            .put(DOUBLE, CAST_TO_DOUBLE)
            .build();

    /**
     * Denote filter mode calls. Must be identified by reference.
     */
    private final Map<RexCall, RexCall> callsInFilterMode = new IdentityHashMap<>();
    private final ExecutionContext executionContext;
    private final List<DataType<?>> outputDataTypes = new ArrayList<>(64);

    // collect output index of literal expression
    private final List<Integer> outputIndexOfLiteral = new ArrayList<>();

    private final boolean fallback;
    private final boolean enableCSE;

    private final boolean enableDrdsTypeSystem;

    private int currentOutputIndex;
    private ExprContextProvider contextProvider;

    private boolean allowConstantFold;
    private ExpressionRewriter expressionRewriter;

    public Rex2VectorizedExpressionVisitor(ExecutionContext executionContext, int startIndex) {
        super(false);
        this.executionContext = executionContext;
        this.currentOutputIndex = startIndex;
        this.fallback =
            !executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION);
        this.enableCSE =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COMMON_SUB_EXPRESSION_TREE_ELIMINATE);
        this.contextProvider = new ExprContextProvider(executionContext);
        this.allowConstantFold =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_EXPRESSION_CONSTANT_FOLD);
        this.expressionRewriter = new ExpressionRewriter(executionContext);
        this.enableDrdsTypeSystem =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_DRDS_TYPE_SYSTEM);
    }

    public void rewriteIn(Map<Integer, Map<String, List>> rewriterParams, String currentPhyTable) {
        expressionRewriter.rewriteIn(rewriterParams, currentPhyTable);
    }

    private static boolean isSpecialFunction(RexCall call) {
        SqlKind sqlKind = call.getKind();
        return sqlKind == SqlKind.MINUS_PREFIX
            || sqlKind == SqlKind.PLUS_PREFIX
            || call.getOperator() == SqlStdOperatorTable.TRIM
            || call.getOperands() == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
    }

    /**
     * Generate the proper function name that vectorized registry can recognize.
     */
    private static String normalizeFunctionName(RexCall call) {
        if (call.op == TddlOperatorTable.CAST
            || call.op == TddlOperatorTable.CONVERT
            || call.op instanceof SqlCastFunction
        ) {
            SqlTypeName castToType = call.getType().getSqlTypeName();
            String castFunctionName = VECTORIZED_CAST_FUNCTION_NAMES.get(castToType);
            if (castFunctionName != null) {
                return castFunctionName;
            }
        } else if (call.op == TddlOperatorTable.IMPLICIT_CAST) {
            // for implicit cast, we only use cast to double util now.
            SqlTypeName castToType = call.getType().getSqlTypeName();
            if (castToType == DOUBLE) {
                return CAST_TO_DOUBLE;
            }
        }
        return call.op.getName().toUpperCase();
    }

    static boolean canBindingToCommonFilterExpression(RexNode node) {
        Preconditions.checkNotNull(node);
        if (!(node instanceof RexCall)) {
            return false;
        }
        RexCall call = (RexCall) node;
        if (TddlOperatorTable.VECTORIZED_COMPARISON_OPERATORS.contains(call.op)) {
            // if the call belongs to comparison operators, all it's operands must be int type or approx type.
            boolean allOperandTypesMatch = call.getOperands().stream()
                .map(e -> e.getType())
                .allMatch(t -> (SqlTypeUtil.isIntType(t) && !SqlTypeUtil.isUnsigned(t)) || SqlTypeUtil
                    .isApproximateNumeric(t));
            // now, we don't support constant folding.
            boolean anyOperandRexNodeMatch = call.getOperands().stream()
                .anyMatch(e -> e instanceof RexCall || e instanceof RexInputRef);
            return allOperandTypesMatch && anyOperandRexNodeMatch;
        }
        return false;
    }

    private void setAllowConstantFold(boolean allowConstantFold) {
        this.allowConstantFold = allowConstantFold;
    }

    private RexCall rewrite(RexCall call, boolean isScalar) {
        return expressionRewriter.rewrite(call, isScalar);
    }

    private void registerFilterModeChildren(RexCall call) {
        Preconditions.checkNotNull(call);
        final RexCall parent = call;
        // register the children that should be in filter mode.
        if (call.op == TddlOperatorTable.CASE) {
            // for case operator, we should set all when condition expressions to filter mode.
            final int operandSize = call.getOperands().size();
            IntStream.range(0, operandSize)
                .filter(i -> i % 2 == 0 && i != operandSize - 1)
                .mapToObj(i -> call.getOperands().get(i))
                .filter(child -> canBindingToCommonFilterExpression(child))
                .forEach(child -> callsInFilterMode.put((RexCall) child, parent));
        }
    }

    private boolean isInFilterMode(RexCall call) {
        return callsInFilterMode.containsKey(call);
    }

    @NotNull
    private LiteralVectorizedExpression doConstantFold(RexCall call) {
        Rex2VectorizedExpressionVisitor constantVisitor = new Rex2VectorizedExpressionVisitor(
            executionContext, 0
        );
        // prevent from stack overflow.
        constantVisitor.setAllowConstantFold(false);

        VectorizedExpression constantCall = call.accept(constantVisitor);

        // allocate 1 slot for constant expression.
        List<DataType<?>> constantOutputTypes = constantVisitor.getOutputDataTypes();
        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(1)
            .addEmptySlots(constantOutputTypes)
            .build();

        preAllocatedChunk.reallocate(1, 0);
        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, executionContext);

        // Do evaluate.
        constantCall.eval(evaluationContext);

        RandomAccessBlock constantBlock = preAllocatedChunk.slotIn(constantCall.getOutputIndex());
        Object constantFolded = constantBlock.elementAt(0);

        // Build constaint
        DataType<?> constantDataType = DataTypeUtil.calciteToDrdsType(call.getType());
        LiteralVectorizedExpression literalVectorizedExpression
            = new LiteralVectorizedExpression(constantDataType, constantFolded, addOutput(constantDataType));

        literalVectorizedExpression.setFolded(constantCall);
        return literalVectorizedExpression;
    }

    @Override
    public VectorizedExpression visitLiteral(RexLiteral literal) {
        int outputIndex = addOutput(DataTypeUtil.calciteToDrdsType(literal.getType()));
        outputIndexOfLiteral.add(outputIndex);
        return LiteralVectorizedExpression
            .from(literal, outputIndex);
    }

    @Override
    public VectorizedExpression visitInputRef(RexInputRef inputRef) {
        return new InputRefVectorizedExpression(DataTypeUtil.calciteToDrdsType(inputRef.getType()),
            inputRef.getIndex(), inputRef.getIndex());
    }

    @Override
    public VectorizedExpression visitCall(RexCall call) {
        if (TddlOperatorTable.CONTROL_FLOW_VECTORIZED_OPERATORS.contains(call.op)) {
            allowConstantFold = false;
        }

        // for constant expression
        RexCall rewrittenCall = call;
        if (allowConstantFold) {
            rewrittenCall = expressionRewriter.rewriteConstFold(rewrittenCall);
        }

        // normal rewrite for other function.
        rewrittenCall = rewrite(rewrittenCall, false);

        if (areInValueAllPruned(rewrittenCall)) {
            // for input ref.
            VectorizedExpression[] children = new VectorizedExpression[1];
            RexNode rexNode = call.operands.get(0);
            children[0] = rexNode.accept(this);

            int outputIndex = addOutput(DataTypes.LongType);

            if (rewrittenCall.op == TddlOperatorTable.IN) {
                return new InEmptyListVectorizedExpression(outputIndex, children);
            } else {
                // must be not in.
                return new NotInEmptyListVectorizedExpression(outputIndex, children);
            }
        }

        if (isInConstCall(rewrittenCall)) {
            Optional<VectorizedExpression> expression = createInVecExpr(rewrittenCall);
            if (expression.isPresent()) {
                return expression.get();
            }
        }

        if (!fallback && !isSpecialFunction(rewrittenCall)) {
            Optional<VectorizedExpression> expression = createVectorizedExpression(rewrittenCall);
            if (expression.isPresent()) {
                return expression.get();
            }
        }

        // Fallback method
        return createGeneralVectorizedExpression(call);
    }

    /**
     * Including IN and NOT_IN
     */
    private Optional<VectorizedExpression> createInVecExpr(RexCall call) {
        try {
            ExpressionConstructor<?> constructor;
            if (call.op == SqlStdOperatorTable.IN) {
                constructor = ExpressionConstructor.of(FastInVectorizedExpression.class);
            } else if (call.op == SqlStdOperatorTable.NOT_IN) {
                constructor = ExpressionConstructor.of(FastNotInVectorizedExpression.class);
            } else {
                // should not reach here
                return Optional.empty();
            }

            int outputIndex = -1;
            boolean isInFilterMode = isInFilterMode(call);
            DataType<?> dataType = getOutputDataType(call);

            VectorizedExpression[] children = new VectorizedExpression[2];
            RexNode rexNode = call.operands.get(0);
            RexNode literalNode1 = call.operands.get(1);
            children[0] = rexNode.accept(this);

            // if left column is not input-ref, we can't get packed long array from block.
            boolean isLeftInterMediate = !(children[0] instanceof InputRefVectorizedExpression);
            outputIndex = addOutput(DataTypeUtil.calciteToDrdsType(literalNode1.getType()));
            children[1] = InValuesVectorizedExpression.from(call.operands, outputIndex, isLeftInterMediate);

            if (!isInFilterMode) {
                outputIndex = addOutput(dataType);
            }

            VectorizedExpression vecExpr = constructor.build(dataType, outputIndex, children);

            if (!isInFilterMode && !DataTypeUtil.equalsSemantically(vecExpr.getOutputDataType(), dataType)) {
                throw new IllegalStateException(String
                    .format("Vectorized expression %s output type %s not equals to rex call type %s!",
                        vecExpr.getClass().getSimpleName(), vecExpr.getOutputDataType(), dataType));
            }
            return Optional.of(vecExpr);
        } catch (Exception e) {
            throw GeneralUtil.nestedException("Failed to create IN vectorized expression", e);
        }
    }

    private boolean areInValueAllPruned(RexCall call) {
        if (call.op != SqlStdOperatorTable.IN &&
            call.op != SqlStdOperatorTable.NOT_IN) {
            return false;
        }

        return call.operands != null
            && call.operands.size() == 1
            && call.operands.get(0) instanceof RexInputRef;
    }

    /**
     * check all in values are constants and of same datatype
     */
    private boolean isInConstCall(RexCall call) {
        if (call.op != SqlStdOperatorTable.IN &&
            call.op != SqlStdOperatorTable.NOT_IN) {
            return false;
        }

        SqlTypeName typeName = null;
        for (int i = 0; i < call.operands.size(); i++) {
            RexNode rexNode = call.operands.get(i);
            if (i == 0 && rexNode instanceof RexInputRef) {
                // only support one column in (const ...) now
                continue;
            }
            if (!(rexNode instanceof RexLiteral)) {
                return false;
            }
            SqlTypeName currentTypeName = ((RexLiteral) rexNode).getTypeName();
            if (typeName == null) {
                typeName = currentTypeName;
            } else {
                // allow NULL values
                if (typeName != currentTypeName && currentTypeName != SqlTypeName.NULL) {
                    return false;
                }
            }
        }

        if (typeName == SqlTypeName.CHAR || typeName == SqlTypeName.VARCHAR) {
            // does not support collation compare in fast path
            boolean compatible = executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_OSS_COMPATIBLE);
            return !compatible;
        }
        return true;
    }

    @Override
    public VectorizedExpression visitFieldAccess(RexFieldAccess fieldAccess) {
        throw new IllegalArgumentException("Correlated variable is not supported in vectorized expression!");
    }

    @Override
    public VectorizedExpression visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == -3 || dynamicParam.getIndex() == -2) {
            throw new IllegalStateException("Subquery not supported yet!");
        }

        Object value = extractDynamicValue(dynamicParam);

        DataType<?> dataType = DataTypeUtil.calciteToDrdsType(dynamicParam.getType());
        return new LiteralVectorizedExpression(dataType, value, addOutput(dataType));
    }

    private Object extractDynamicValue(RexDynamicParam dynamicParam) {
        // pre-compute the dynamic value when binging expression.
        DynamicParamExpression dynamicParamExpression =
            new DynamicParamExpression(dynamicParam.getIndex(), contextProvider,
                dynamicParam.getSubIndex(), dynamicParam.getSkIndex());

        return dynamicParamExpression.eval(null, executionContext);
    }

    public AbstractScalarFunction createFunction(RexCall call, List<VectorizedExpression> args) {
        call = rewrite(call, true);

        String functionName = call.getOperator().getName();
        if (call.getKind().equals(SqlKind.MINUS_PREFIX)) {
            functionName = "UNARY_MINUS";
        }
        if (call.getKind().equals(SqlKind.PLUS_PREFIX)) {
            functionName = "UNARY_PLUS";
        }

        List<RexNode> operands = call.getOperands();
        if (args == null) {
            args = new ArrayList<>(operands.size());
        }
        for (RexNode rexNode : operands) {
            args.add(rexNode.accept(this));
        }
        args.addAll(visitExtraParams(call));

        // deal with extra parameters like type in cast function.
        DataType resultType = DataTypeUtil.calciteToDrdsType(call.getType());
        List<DataType> operandTypes = operands.stream()
            .map(RexNode::getType)
            .map(type -> DataTypeUtil.calciteToDrdsType(type))
            .collect(Collectors.toList());
        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction(functionName,
            operandTypes, resultType);
        if (scalarFunction instanceof AbstractCollationScalarFunction) {
            CollationName collation = Rex2ExprUtil.fixCollation(call, executionContext);
            ((AbstractCollationScalarFunction) scalarFunction).setCollation(collation);
        }
        return scalarFunction;
    }

    private VectorizedExpression createGeneralVectorizedExpression(RexCall call) {
        List<VectorizedExpression> args = new ArrayList<>();
        AbstractScalarFunction expression = createFunction(call, args);
        return BuiltInFunctionVectorizedExpression
            .from(args.toArray(new VectorizedExpression[0]), addOutput(expression.getReturnType()),
                expression,
                executionContext);
    }

    private Optional<VectorizedExpression> createVectorizedExpression(RexCall call) {
        Preconditions.checkNotNull(call);
        Optional<ExpressionConstructor<?>> constructor;

        if (SPECIAL_VECTORIZED_EXPRESSION_MAPPING.containsKey(call.getOperator())) {
            // special class binding.
            constructor = Optional.of(
                ExpressionConstructor.of(
                    SPECIAL_VECTORIZED_EXPRESSION_MAPPING.get(call.getOperator())
                )
            );
        } else {
            // common code-generated class binding.
            constructor = createExpressionFromRegistry(call);
        }

        // register child expression to filter mode map.
        registerFilterModeChildren(call);

        if (constructor.isPresent()) {
            // Determine whether to update the date type list and output index, according to the expression mode.
            int outputIndex = -1;
            boolean isInFilterMode = isInFilterMode(call);
            DataType<?> dataType = getOutputDataType(call);

            VectorizedExpression[] children =
                call.getOperands().stream().map(node -> node.accept(this)).toArray(VectorizedExpression[]::new);

            if (!isInFilterMode) {
                outputIndex = addOutput(dataType);
            }

            try {
                VectorizedExpression vecExpr = constructor.get().build(dataType, outputIndex, children);

                if (!isInFilterMode && !DataTypeUtil.equalsSemantically(vecExpr.getOutputDataType(), dataType)) {
                    throw new IllegalStateException(String
                        .format("Vectorized expression %s output type %s not equals to rex call type %s!",
                            vecExpr.getClass().getSimpleName(), vecExpr.getOutputDataType(), dataType));
                }
                return Optional.of(vecExpr);
            } catch (Exception e) {
                throw GeneralUtil.nestedException("Failed to create vectorized expression", e);
            }
        } else {
            return Optional.empty();
        }
    }

    private DataType getOutputDataType(RexCall call) {
        RelDataType relDataType = call.getType();
        if ((call.op == TddlOperatorTable.CONVERT || call.op == TddlOperatorTable.CAST)
            && SqlTypeUtil.isDecimal(relDataType)) {
            // For decimal type of cast operator, we should use precious type info.
            int precision = relDataType.getPrecision();
            int scale = relDataType.getScale();
            return new DecimalType(precision, scale);
        }
        return DataTypeUtil.calciteToDrdsType(relDataType);
    }

    private Optional<ExpressionConstructor<?>> createExpressionFromRegistry(RexCall call) {
        String functionName = normalizeFunctionName(call);
        List<ArgumentInfo> argumentInfos = new ArrayList<>(call.getOperands().size());

        for (RexNode arg : call.getOperands()) {
            ArgumentInfo info = Rex2ArgumentInfo.toArgumentInfo(arg);
            if (info != null) {
                argumentInfos.add(info);
            } else {
                // Unable to convert operand to argument, so skip conversion.
                return Optional.empty();
            }
        }
        ExpressionMode mode = callsInFilterMode.containsKey(call) ? ExpressionMode.FILTER : ExpressionMode.PROJECT;
        ExpressionSignature signature =
            new ExpressionSignature(functionName, argumentInfos.toArray(new ArgumentInfo[0]), mode);

        return VectorizedExpressionRegistry.builderConstructorOf(signature);
    }

    private List<VectorizedExpression> visitExtraParams(RexCall call) {
        String functionName = call.getOperator().getName();
        if ("CAST".equalsIgnoreCase(functionName)) {
            RelDataType relDataType = call.getType();
            DataType<?> dataType = DataTypeUtil.calciteToDrdsType(relDataType);
            if ((relDataType.getSqlTypeName() == CHAR || relDataType.getSqlTypeName() == BINARY)
                && relDataType.getPrecision() >= 0) {
                return ImmutableList
                    .of(new LiteralVectorizedExpression(DataTypes.StringType, dataType.getStringSqlType(),
                            addOutput(DataTypes.StringType)),
                        new LiteralVectorizedExpression(DataTypes.IntegerType, relDataType.getPrecision(),
                            addOutput(DataTypes.IntegerType)));
            }

            if (relDataType.getSqlTypeName() == DECIMAL && relDataType.getPrecision() > 0) {
                return ImmutableList
                    .of(new LiteralVectorizedExpression(DataTypes.StringType, dataType.getStringSqlType(),
                            addOutput(DataTypes.StringType)),
                        new LiteralVectorizedExpression(DataTypes.IntegerType, relDataType.getPrecision(),
                            addOutput(DataTypes.IntegerType)),
                        new LiteralVectorizedExpression(DataTypes.IntegerType,
                            relDataType.getScale() == -1 ? 0 : relDataType.getScale(),
                            addOutput(DataTypes.IntegerType)));
            }

            // For fractional time type, preserve scale value.
            if (DataTypeUtil.isFractionalTimeType(dataType)) {
                return ImmutableList
                    .of(new LiteralVectorizedExpression(DataTypes.StringType, dataType.getStringSqlType(),
                            addOutput(DataTypes.StringType)),
                        new LiteralVectorizedExpression(DataTypes.IntegerType,
                            relDataType.getScale() == -1 ? 0 : relDataType.getScale(),
                            addOutput(DataTypes.IntegerType)));
            }

            return ImmutableList.of(new LiteralVectorizedExpression(DataTypes.StringType, dataType.getStringSqlType(),
                addOutput(DataTypes.StringType)));
        }

        if ("CONVERT".equalsIgnoreCase(functionName)) {
            return ImmutableList
                .of(new LiteralVectorizedExpression(DataTypes.IntegerType, 1, addOutput(DataTypes.IntegerType)));
        }

        return Collections.emptyList();
    }

    private VectorizedExpression buildIntervalFunction(RexLiteral literal) {
        Preconditions.checkArgument(literal.getType() instanceof IntervalSqlType, "Input type should be interval!");
        VectorizedExpression[] args = new VectorizedExpression[2];
        String unit = literal.getType().getIntervalQualifier().getUnit().name();
        String value = ((IntervalString) literal.getValue()).getIntervalStr();

        args[0] = new LiteralVectorizedExpression(DataTypes.StringType, unit, addOutput(DataTypes.StringType));
        args[1] = new LiteralVectorizedExpression(DataTypes.StringType, value, addOutput(DataTypes.StringType));

        AbstractScalarFunction scalarFunction = ExtraFunctionManager.getExtraFunction("INTERVAL_PRIMARY", null, null);
        return BuiltInFunctionVectorizedExpression
            .from(args, addOutput(scalarFunction.getReturnType()), scalarFunction, executionContext);
    }

    @Override
    public VectorizedExpression visitSystemVar(RexSystemVar systemVar) {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "System variables (not pushed down)");
    }

    /**
     * Add output datatype and update index.
     *
     * @param outputDataType Datatype of this output
     * @return The output index of this output.
     */
    private int addOutput(DataType<?> outputDataType) {
        this.outputDataTypes.add(outputDataType);
        int ret = this.currentOutputIndex;
        this.currentOutputIndex += 1;
        return ret;
    }

    public List<DataType<?>> getOutputDataTypes() {
        return Collections.unmodifiableList(outputDataTypes);
    }

    public BitSet getLiteralBitmap() {
        BitSet literalBitmap = new BitSet(currentOutputIndex);
        for (int i = 0; i < outputIndexOfLiteral.size(); i++) {
            literalBitmap.set(outputIndexOfLiteral.get(i));
        }
        return literalBitmap;
    }

}