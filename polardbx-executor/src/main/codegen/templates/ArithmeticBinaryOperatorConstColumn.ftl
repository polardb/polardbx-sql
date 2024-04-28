<@pp.dropOutputFile />

<#list mathOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Const${type.inputDataType2}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Const, Variable})
public class ${className} extends AbstractVectorizedExpression {
        private final boolean leftIsNull;
        private final ${type.inputType1} left;
        public ${className}(int outputIndex, VectorizedExpression[] children) {
            super(DataTypes.${type.outputDataType}Type, outputIndex, children);
            Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
            if (leftValue == null) {
                leftIsNull = true;
                left = (${type.inputType1}) 0;
                } else {
                leftIsNull = false;
                left =  (${type.inputType1}) leftValue;
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
		RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType2}[] array2 = (rightInputVectorSlot.cast(${type.inputVectorType2}.class)).${type.inputType2}Array();
        ${type.outputType}[] res = (outputVectorSlot.cast(${type.outputVectorType}.class)).${type.outputType}Array();

		VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());
		boolean[] outputNulls = outputVectorSlot.nulls();
		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
        ${type.outputType} right = (${type.outputType}) array2[j];
        <#if operator.classHeader == "Modulo">
			if (right == 0) {
			outputNulls[j] = true;
                    right = 1;
                }
                </#if>
                <#if operator.classHeader == "Divide">
                if (right == 0) {
                    outputNulls[j] = true;
                }
                </#if>
                res[j] = ((${type.outputType})left) ${operator.op} right;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                ${type.outputType} right = (${type.outputType}) array2[i];
                <#if operator.classHeader == "Modulo">
                 if (right == 0) {
                    outputNulls[i] = true;
                    right = 1;
                 }
                </#if>
                <#if operator.classHeader == "Divide">
                if (right == 0) {
                    outputNulls[i] = true;
                }
                </#if>
                res[i] = ((${type.outputType})left) ${operator.op} right;
            }
        }
    }
}

    </#list>
</#list>

