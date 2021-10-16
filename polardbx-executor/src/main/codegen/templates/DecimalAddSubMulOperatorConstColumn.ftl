<@pp.dropOutputFile />

<#list mathOperators.decimalAddSubMulOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Const${type.inputDataType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/math/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.optimizer.chunk.*;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;

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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Const, Variable})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final ${type.inputType1} left;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.${type.outputDataType}Type, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (leftValue == null) {
            leftIsNull = true;
            <#if type.inputDataType1 == "Decimal">
            left = Decimal.ZERO;
            <#else>
            left = (${type.inputType1}) 0;
            </#if>
        } else {
            leftIsNull = false;
            <#if type.inputDataType1 == "ULong">
            left = ((Number) leftValue).longValue();
            <#elseif type.inputDataType1 == "Decimal">
            left = (Decimal) leftValue;
            <#else>
            left = (${type.inputType1}) leftValue;
            </#if>
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[1].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        if (leftIsNull) {
            VectorizedExpressionUtils.setNulls(chunk, outputIndex);
            return;
        }

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        <#if type.inputDataType2 == "Decimal">
        Slice input2 = ((DecimalBlock) rightInputVectorSlot).getMemorySegments();
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

        <#if type.inputDataType1 == "ULong">
        DecimalConverter.unsignedlongToDecimal(left, leftDec);
        <#elseif type.inputDataType1 == "Decimal">
        leftDec = left.getDecimalStructure();
            <#if type.inputDataType2 == "Double" || type.inputDataType2 == "Float">
            double leftDouble = DecimalConverter.decimalToDouble(leftDec);
            </#if>
        <#elseif type.inputDataType1 != "Double" && type.inputDataType1 != "Float">
        DecimalConverter.longToDecimal(left, leftDec, children[0].getOutputDataType().isUnsigned());
        </#if>

        DecimalStructure tmpDec = new DecimalStructure();
        boolean isNull[] = outputVectorSlot.nulls();

        boolean isRightUnsigned = children[1].getOutputDataType().isUnsigned();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
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
                <#if type.inputDataType2 != "Decimal">
                rightDec.reset();
                </#if>

                // fetch right decimal value
                <#if type.inputDataType2 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array2[j], rightDec);
                <#elseif type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(input2.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                <#else>
                DecimalConverter.longToDecimal(array2[j], rightDec, isRightUnsigned);
                </#if>

                // do operator
                FastDecimalUtils.${operator.decimalOp}(leftDec, rightDec, toValue);
             <#else>
                <#if type.inputDataType1 == "Decimal">
                res[j] = leftDouble ${operator.doubleOp} array2[j];
                </#if>
                <#if type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(input2.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                double rightDouble = DecimalConverter.decimalToDouble(rightDec);
                res[j] = left ${operator.doubleOp} rightDouble;
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
                <#if type.inputDataType2 != "Decimal">
                rightDec.reset();
                </#if>

                // fetch right decimal value
                <#if type.inputDataType2 == "ULong">
                DecimalConverter.unsignedlongToDecimal(array2[i], rightDec);
                <#elseif type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(input2.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                <#else>
                DecimalConverter.longToDecimal(array2[i], rightDec, isRightUnsigned);
                </#if>

                // do operator
                FastDecimalUtils.${operator.decimalOp}(leftDec, rightDec, toValue);
             <#else>
                <#if type.inputDataType1 == "Decimal">
                res[i] = leftDouble ${operator.doubleOp} array2[i];
                </#if>
                <#if type.inputDataType2 == "Decimal">
                rightDec = new DecimalStructure(input2.slice(fromIndex, DECIMAL_MEMORY_SIZE));
                double rightDouble = DecimalConverter.decimalToDouble(rightDec);
                res[i] = left ${operator.doubleOp} rightDouble;
                </#if>
            </#if>
            }
        }
    }
}

    </#list>
</#list>

