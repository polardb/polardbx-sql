<@pp.dropOutputFile />

<#list mathOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ConstVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Const})
public class ${className} extends AbstractVectorizedExpression {
        private final boolean rightIsNull;
        private final ${type.inputType2} right;
        public ${className}(int outputIndex, VectorizedExpression[] children) {
            super(DataTypes.${type.outputDataType}Type, outputIndex, children);
            Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
            if (rightValue  == null) {
                rightIsNull = true;
                right = (${type.inputType2}) 0;
            } else {
                rightIsNull = false;
                right = (${type.inputType2}) rightValue;
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

        <#if (operator.classHeader == "Divide") || (operator.classHeader == "Modulo")>
			if (right == 0) {
			VectorizedExpressionUtils.setNulls(chunk, outputIndex);
			return;
			}
        </#if>

		RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
		RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType1}[] array1 = (leftInputVectorSlot.cast(${type.inputVectorType1}.class)).${type.inputType1}Array();
        ${type.outputType}[] res = (outputVectorSlot.cast(${type.outputVectorType}.class)).${type.outputType}Array();

		VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		res[j] = ((${type.outputType})array1[j]) ${operator.op} ((${type.outputType})right);
		}
		} else {
		for (int i = 0; i < batchSize; i++) {
		res[i] = ((${type.outputType})array1[i]) ${operator.op} ((${type.outputType})right);
            }
        }

    }
}

    </#list>
</#list>

