<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

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
        super(DataTypes.LongType, outputIndex, children);
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
        ${type.inputType2}[] array2 = (rightInputVectorSlot.cast(${type.inputVectorType2}.class)).${type.inputType2}Array();
		long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

        <#if operator.classHeader != "SEQ">
			if (isSelectionInUse) {
			for (int i = 0; i < batchSize; i++) {
			int j = sel[i];
			res[j] = (array1[j] ${operator.op} array2[j]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
			}
			} else {
			for (int i = 0; i < batchSize; i++) {
			res[i] = (array1[i] ${operator.op} array2[i]) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
            }
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(), children[1].getOutputIndex());
        </#if>

        <#if operator.classHeader = "SEQ">
        boolean[] nulls1 = leftInputVectorSlot.nulls();
        boolean input1HasNull = leftInputVectorSlot.hasNull();
        boolean[] nulls2 = rightInputVectorSlot.nulls();
        boolean input2HasNull = rightInputVectorSlot.hasNull();
        boolean[] outputNulls = outputVectorSlot.nulls();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                boolean null1 = input1HasNull ? nulls1[j] : false;
                boolean null2 = input2HasNull ? nulls2[j] : false;
                res[j] = ((null1 && null2) || ((null1 == null2) && (array1[j] == array2[j]))) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                outputNulls[j] = false;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                boolean null1 = input1HasNull ? nulls1[i] : false;
                boolean null2 = input2HasNull ? nulls2[i] : false;
                res[i] = ((null1 && null2) || ((null1 == null2) && (array1[i] == array2[i]))) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                outputNulls[i] = false;
            }
        }
        </#if>
    }
}

    </#list>
</#list>

