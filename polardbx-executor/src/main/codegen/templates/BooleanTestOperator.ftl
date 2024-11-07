<@pp.dropOutputFile />

<#list cmpOperators.booleanTestOperators.operators as operator>

    <#list cmpOperators.booleanTestOperators.types as type>

        <#assign className = "${operator.classHeader}${type.inputDataType}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType}"}, argumentKinds = {Variable})
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
		RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType}[] inputArray = (inputVectorSlot.cast(${type.inputVectorType}.class)).${type.inputType}Array();
		boolean[] inputNulls = inputVectorSlot.nulls();
		boolean inputHasNull = inputVectorSlot.hasNull();
		long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
		boolean[] outputNulls = outputVectorSlot.nulls();
		outputVectorSlot.setHasNull(inputVectorSlot.hasNull());

		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		outputNulls[j] = false;
		boolean inputNull = !inputHasNull ? false : inputNulls[j];
        <#if operator.classHeader = "IsTrue">
                res[j] = inputNull ? 0 : (inputArray[j] != 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsFalse">
                res[j] = inputNull ? 0 : (inputArray[j] == 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsNotFalse">
                res[j] = inputNull ? 1 : (inputArray[j] != 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsUnknown">
                res[j] = inputNull ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsNotUnknown">
                res[j] = !inputNull ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = false;
                boolean inputNull = !inputHasNull ? false : inputNulls[i];
                <#if operator.classHeader = "IsTrue">
                res[i] = inputNull ? 0 : (inputArray[i] != 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsFalse">
                res[i] = inputNull ? 0 : (inputArray[i] == 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsNotFalse">
                res[i] = inputNull ? 1 : (inputArray[i] != 0) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsUnknown">
                res[i] = inputNull ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
                <#if operator.classHeader = "IsNotUnknown">
                res[i] = !inputNull ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                </#if>
            }
        }
    }
}
    </#list>
</#list>

