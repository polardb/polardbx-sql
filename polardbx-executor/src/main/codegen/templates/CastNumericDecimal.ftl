<@pp.dropOutputFile />

<#list castOperators.castToDecimalOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType}To${type.outputDataType}VectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/convert/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.convert;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;

import com.alibaba.polardbx.common.datatype.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.*;
import io.airlift.slice.Slice;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType}"}, argumentKinds = {Variable})
public class ${className} extends AbstractVectorizedExpression {
    public ${className}(DataType<?> outputDataType, int outputIndex, VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType}[] input = ((${type.inputVectorType}) inputVectorSlot).${type.inputType}Array();
        Slice output = ((DecimalBlock) outputVectorSlot).getMemorySegments();

        DecimalStructure tmpDecimal = new DecimalStructure();
        int precision = outputDataType.getPrecision();
        int scale = outputDataType.getScale();
        boolean isUnsigned = children[0].getOutputDataType().isUnsigned();

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do convert operation
                tmpDecimal.reset();
                DecimalConverter.longToDecimal(input[j], tmpDecimal);
                DecimalConverter.rescale(tmpDecimal, toValue, precision, scale, isUnsigned);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // The convert result will directly wrote to decimal memory segment
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do convert operation
                tmpDecimal.reset();
                DecimalConverter.longToDecimal(input[i], tmpDecimal);
                DecimalConverter.rescale(tmpDecimal, toValue, precision, scale, isUnsigned);
            }
        }
    }
}

    </#list>
</#list>