<@pp.dropOutputFile />
    <#list mathOperators.unaryMinus.types as type>

        <#assign className = "${mathOperators.unaryMinus.classHeader}${type.inputDataType1}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${mathOperators.unaryMinus.functionNames}}, argumentTypes = {"${type.inputDataType1}"}, argumentKinds = {Variable})
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

        ${type.inputType1}[] array1 = (leftInputVectorSlot.cast(${type.inputVectorType1}.class)).${type.inputType1}Array();
        ${type.outputType}[] res = (outputVectorSlot.cast(${type.outputVectorType}.class)).${type.outputType}Array();

		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		res[j] = -1 * (${type.outputType})array1[j];
		}
		} else {
		for (int i = 0; i < batchSize; i++) {
		res[i] = -1 * (${type.outputType})array1[i];
		}
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
    }
}

</#list>

