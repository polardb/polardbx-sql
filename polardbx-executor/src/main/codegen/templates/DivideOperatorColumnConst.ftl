<@pp.dropOutputFile />

<#list mathOperators.divideOperators as operator>

    <#list operator.types as type>

        <#assign className = "Divide${type.inputDataType1}Col${type.inputDataType2}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/math/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

import com.alibaba.polardbx.common.datatype.*;
import com.alibaba.polardbx.optimizer.core.datatype.*;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.*;
import java.util.Optional;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Const})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean rightIsNull;
    private final ${type.inputType2} right;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.${type.outputDataType}Type, outputIndex, children);
        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        if (rightValue  == null) {
            rightIsNull = true;
            <#if type.inputDataType2 == "Decimal">
            right = Decimal.ZERO;
            <#else>
            right = (${type.inputType2}) 0;
            </#if>
        } else {
            rightIsNull = false;
            <#if type.inputDataType2 == "ULong">
            right = ((Number) rightValue).longValue();
            <#elseif type.inputDataType2 == "Decimal">
            right = (Decimal) rightValue;
            <#else>
            right = (${type.inputType2}) rightValue;
            </#if>
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        if (rightIsNull) {
            VectorizedExpressionUtils.setNulls(chunk, outputIndex);
            return;
        }

        // get div_precision_increment user variables from session.
        int divPrecisionIncrement = Optional.ofNullable(ctx)
            .map(EvaluationContext::getExecutionContext)
            .map(ExecutionContext::getServerVariables)
            .map(m -> m.get(DIV_PRECISION_INCREMENT))
            .map(n -> ((Number) n).intValue())
            .map(i -> Math.min(i, MAX_DECIMAL_SCALE))
            .orElse(DEFAULT_DIV_PRECISION_INCREMENT);

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        <#if type.inputDataType1 == "Decimal">

        <#else>
        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();
        </#if>
        <#if type.outputDataType == "Decimal">
        Slice output = ((DecimalBlock) outputVectorSlot).getMemorySegments();
        <#else>
        ${type.outputType}[] res = ((${type.outputVectorType}) outputVectorSlot).${type.outputType}Array();
        </#if>

        DecimalStructure leftDec = new DecimalStructure();
        DecimalStructure rightDec = new DecimalStructure();
        <#if type.inputDataType2 == "ULong">
        DecimalConverter.unsignedlongToDecimal(right, rightDec);
        <#elseif type.inputDataType2 == "Decimal">
        rightDec = right.getDecimalStructure();
            <#if type.inputDataType1 == "Double" || type.inputDataType1 == "Float">
            double rightDouble = DecimalConverter.decimalToDouble(rightDec);
            </#if>
        <#elseif type.inputDataType2 != "Double" && type.inputDataType2 != "Float">
        DecimalConverter.longToDecimal(right, rightDec, children[1].getOutputDataType().isUnsigned());
        </#if>

        DecimalStructure tmpDec = new DecimalStructure();
        boolean isNull[] = outputVectorSlot.nulls();

        boolean isLeftUnsigned = children[0].getOutputDataType().isUnsigned();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                <#if type.outputDataType == "Decimal">
                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);
                </#if>

            <#if type.outputDataType == "Decimal">
                // do reset
                <#if type.inputDataType1 != "Decimal">
                leftDec.reset();
                </#if>
                tmpDec.reset();

                // fetch left decimal value
                <#if type.inputDataType1 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array1[j], leftDec);
                <#elseif type.inputDataType1 == "Decimal">
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(j));
                <#else>
                DecimalConverter.longToDecimal(array1[j], leftDec, isLeftUnsigned);
                </#if>

                // do divide
                int error = FastDecimalUtils.div(leftDec, rightDec, tmpDec, divPrecisionIncrement);

                if (error == E_DEC_DIV_ZERO) {
                    // divide zero, set null
                    isNull[j] = true;
                } else {
                    // do round
                    FastDecimalUtils.round(tmpDec, toValue, divPrecisionIncrement, DecimalRoundMod.HALF_UP);
                }
            <#else>
                <#if type.inputDataType1 == "Decimal">
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(j));
                double leftDouble = DecimalConverter.decimalToDouble(leftDec);
                res[j] = leftDouble / right;
                </#if>
                <#if type.inputDataType2 == "Decimal">
                res[j] = array1[j] / rightDouble;
                </#if>
            </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                <#if type.outputDataType == "Decimal">
                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);
                </#if>

            <#if type.outputDataType == "Decimal">
                // do reset
                <#if type.inputDataType1 != "Decimal">
                leftDec.reset();
                </#if>
                tmpDec.reset();

                // fetch left decimal value
                <#if type.inputDataType1 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array1[i], leftDec);
                <#elseif type.inputDataType1 == "Decimal">
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(i));
                <#else>
                DecimalConverter.longToDecimal(array1[i], leftDec, isLeftUnsigned);
                </#if>

                // do divide
                int error = FastDecimalUtils.div(leftDec, rightDec, tmpDec, divPrecisionIncrement);

                if (error == E_DEC_DIV_ZERO) {
                    // divide zero, set null
                    isNull[i] = true;
                } else {
                    // do round
                    FastDecimalUtils.round(tmpDec, toValue, divPrecisionIncrement, DecimalRoundMod.HALF_UP);
                }
            <#else>
                <#if type.inputDataType1 == "Decimal">
                leftDec = new DecimalStructure(((DecimalBlock) leftInputVectorSlot).getRegion(i));
                double leftDouble = DecimalConverter.decimalToDouble(leftDec);
                res[i] = leftDouble / right;
                </#if>
                <#if type.inputDataType2 == "Decimal">
                res[i] = array1[i] / rightDouble;
                </#if>
            </#if>
            }
        }
    }
}

    </#list>
</#list>