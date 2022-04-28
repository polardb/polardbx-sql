<@pp.dropOutputFile />

<#list mathOperators.divideOperators as operator>

    <#list operator.types as type>

        <#assign className = "Divide${type.inputDataType1}Col${type.inputDataType2}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Variable})
public class ${className} extends AbstractVectorizedExpression {
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.${type.outputDataType}Type, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        // get div_precision_increment user variables from session.
        int divPrecisionIncrement = Optional.ofNullable(ctx)
            .map(EvaluationContext::getExecutionContext)
            .map(ExecutionContext::getServerVariables)
            .map(m -> m.get(DIV_PRECISION_INCREMENT))
            .map(n -> ((Number) n).intValue())
            .map(i -> Math.min(i, MAX_DECIMAL_SCALE))
            .orElse(DEFAULT_DIV_PRECISION_INCREMENT);

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        <#if type.inputDataType1 == "Decimal">

        <#else>
        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();
        </#if>

        <#if type.inputDataType2 == "Decimal">

        <#else>
        ${type.inputType2}[] array2 = ((${type.inputVectorType2}) rightInputVectorSlot).${type.inputType2}Array();
        </#if>

        <#if type.outputDataType == "Decimal">
        Slice output = ((DecimalBlock) outputVectorSlot).getMemorySegments();
        <#else>
        ${type.outputType}[] res = ((${type.outputVectorType}) outputVectorSlot).${type.outputType}Array();
        </#if>

        DecimalStructure leftDec = new DecimalStructure();
        DecimalStructure rightDec = new DecimalStructure();
        DecimalStructure tmpDec = new DecimalStructure();
        boolean isNull[] = outputVectorSlot.nulls();

        boolean isLeftUnsigned = children[0].getOutputDataType().isUnsigned();
        boolean isRightUnsigned = children[1].getOutputDataType().isUnsigned();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(), children[1].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                <#if type.outputDataType == "Decimal">
                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);
                <#elseif type.inputDataType2 == "Double" || type.inputDataType2 == "Float">
                if (array2[j] == 0) {
                    isNull[j] = true;
                    continue;
                }
                </#if>

            <#if type.outputDataType == "Decimal">
                // do reset
                <#if type.inputDataType1 != "Decimal">
                leftDec.reset();
                </#if>
                <#if type.inputDataType2 != "Decimal">
                rightDec.reset();
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

                // fetch right decimal value
                <#if type.inputDataType2 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array2[j], rightDec);
                <#elseif type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(((DecimalBlock) rightInputVectorSlot).getRegion(j));
                <#else>
                DecimalConverter.longToDecimal(array2[j], rightDec, isRightUnsigned);
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
                res[j] = leftDouble / array2[j];
                </#if>
                <#if type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(((DecimalBlock) rightInputVectorSlot).getRegion(j));
                double rightDouble = DecimalConverter.decimalToDouble(rightDec);
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
                <#elseif type.inputDataType2 == "Double" || type.inputDataType2 == "Float">
                if (array2[i] == 0) {
                    isNull[i] = true;
                    continue;
                }
                </#if>

            <#if type.outputDataType == "Decimal">
                // do reset
                <#if type.inputDataType1 != "Decimal">
                leftDec.reset();
                </#if>
                <#if type.inputDataType2 != "Decimal">
                rightDec.reset();
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

                // fetch right decimal value
                <#if type.inputDataType2 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array2[i], rightDec);
                <#elseif type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(((DecimalBlock) rightInputVectorSlot).getRegion(i));
                <#else>
                DecimalConverter.longToDecimal(array2[i], rightDec, isRightUnsigned);
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
                res[i] = leftDouble / array2[i];
                </#if>
                <#if type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(((DecimalBlock) rightInputVectorSlot).getRegion(i));
                double rightDouble = DecimalConverter.decimalToDouble(rightDec);
                res[i] = array1[i] / rightDouble;
                </#if>
            </#if>
            }
        }
    }
}

    </#list>
</#list>