<@pp.dropOutputFile />

<#list mathOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/math/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

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

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();
        ${type.inputType2}[] array2 = ((${type.inputVectorType2}) rightInputVectorSlot).${type.inputType2}Array();
        ${type.outputType}[] res = ((${type.outputVectorType}) outputVectorSlot).${type.outputType}Array();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(), children[1].getOutputIndex());
        boolean[] outputNulls = outputVectorSlot.nulls();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                ${type.outputType} right = (${type.outputType})array2[j];
                <#if operator.classHeader == "Divide">
                if (right == 0) {
                    outputNulls[j] = true;
                }
                </#if>
                <#if operator.classHeader == "Modulo">
                if (right == 0) {
                    outputNulls[j] = true;
                    right = 1;
                }
                </#if>
                res[j] = ((${type.outputType})array1[j]) ${operator.op} right;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                ${type.outputType} right = (${type.outputType})array2[i];
                <#if operator.classHeader == "Divide">
                if (right == 0) {
                    outputNulls[i] = true;
                }
                </#if>
                <#if operator.classHeader == "Modulo">
                if (right == 0) {
                    outputNulls[i] = true;
                    right = 1;
                }
                </#if>
                res[i] = ((${type.outputType}) array1[i]) ${operator.op} (${type.outputType}) right;
            }
        }
    }
}

    </#list>
</#list>

