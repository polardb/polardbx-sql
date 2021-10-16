<@pp.dropOutputFile />

<#list castOperators.castToUnsignedOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType}ToUnsignedVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/convert/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.convert;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.optimizer.chunk.*;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;

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
        long[] output = ((ULongBlock) outputVectorSlot).longArray();

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                <#if type.inputType == "float" || type.inputType == "double">
                output[j] = (long) Math.rint(input[j]);
                </#if>
                <#if type.inputType != "float" && type.inputType != "double">
                output[j] = input[j];
                </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                <#if type.inputType == "float" || type.inputType == "double">
                output[i] = (long) Math.rint(input[i]);
                </#if>
                <#if type.inputType != "float" && type.inputType != "double">
                output[i] = input[i];
                </#if>
            }
        }
    }

}

    </#list>
</#list>