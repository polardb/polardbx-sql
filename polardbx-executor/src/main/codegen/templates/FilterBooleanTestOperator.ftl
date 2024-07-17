<@pp.dropOutputFile />

<#list cmpOperators.booleanTestOperators.operators as operator>

    <#list cmpOperators.booleanTestOperators.types as type>

        <#assign className = "Filter${operator.classHeader}${type.inputDataType}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/comparison/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.comparison;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionMode;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.executor.chunk.*;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType}"}, argumentKinds = {Variable}, mode = ExpressionMode.FILTER)
public class ${className} extends AbstractVectorizedExpression {
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(null, outputIndex, children);
    }

		@Override
		public void eval(EvaluationContext ctx) {
		super.evalChildren(ctx);
		MutableChunk chunk = ctx.getPreAllocatedChunk();
		int batchSize = chunk.batchSize();
		boolean isSelectionInUse = chunk.isSelectionInUse();
		int[] sel = chunk.selection();

		RandomAccessBlock inputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        ${type.inputType}[] inputArray = (inputVectorSlot.cast(${type.inputVectorType}.class)).${type.inputType}Array();

		boolean[] inputNulls = inputVectorSlot.nulls();

		int newSize = 0;
		if (inputVectorSlot.hasNull()) {
		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		boolean inputNull = inputNulls[j];
        <#if operator.classHeader = "IsTrue">
                    if (!inputNull && (inputArray[j] != 0)) {
                    </#if>
                    <#if operator.classHeader = "IsNotTrue">
                    if (inputNull || (inputArray[j] == 0)) {
                    </#if>
                    <#if operator.classHeader = "IsFalse">
                    if (!inputNull && (inputArray[j] == 0)) {
                    </#if>
                    <#if operator.classHeader = "IsNotFalse">
                    if (inputNull || (inputArray[j] != 0)) {
                    </#if>
                    <#if operator.classHeader = "IsUnknown">
                    if (inputNull) {
                    </#if>
                    <#if operator.classHeader = "IsNotUnknown">
                    if (!inputNull) {
                    </#if>
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    boolean inputNull = inputNulls[i];
                    <#if operator.classHeader = "IsTrue">
                    if (!inputNull && (inputArray[i] != 0)) {
                    </#if>
                    <#if operator.classHeader = "IsNotTrue">
                    if (inputNull || (inputArray[i] == 0)) {
                    </#if>
                    <#if operator.classHeader = "IsFalse">
                    if (!inputNull && (inputArray[i] == 0)) {
                    </#if>
                    <#if operator.classHeader = "IsNotFalse">
                    if (inputNull || (inputArray[i] != 0)) {
                    </#if>
                    <#if operator.classHeader = "IsUnknown">
                    if (inputNull) {
                    </#if>
                    <#if operator.classHeader = "IsNotUnknown">
                    if (!inputNull) {
                    </#if>
                        sel[newSize++] = i;
                    }
                }
            }
        } else {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    <#if operator.classHeader = "IsTrue">
                    if (inputArray[j] != 0) {
                        sel[newSize++] = j;
                    }
                    </#if>
                    <#if operator.classHeader = "IsNotTrue">
                    if (inputArray[j] == 0) {
                        sel[newSize++] = j;
                    }
                    </#if>
                    <#if operator.classHeader = "IsFalse">
                    if (inputArray[j] == 0) {
                        sel[newSize++] = j;
                    }
                    </#if>
                    <#if operator.classHeader = "IsNotFalse">
                    if (inputArray[j] != 0) {
                        sel[newSize++] = j;
                    }
                    </#if>
                    <#if operator.classHeader = "IsUnknown">
                    break;
                    </#if>
                    <#if operator.classHeader = "IsNotUnknown">
                    newSize = batchSize;
                    break;
                    </#if>
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    <#if operator.classHeader = "IsTrue">
                    if (inputArray[i] != 0) {
                        sel[newSize++] = i;
                    }
                    </#if>
                    <#if operator.classHeader = "IsNotTrue">
                    if (inputArray[i] == 0) {
                        sel[newSize++] = i;
                    }
                    </#if>
                    <#if operator.classHeader = "IsFalse">
                    if (inputArray[i] == 0) {
                        sel[newSize++] = i;
                    }
                    </#if>
                    <#if operator.classHeader = "IsNotFalse">
                    if (inputArray[i] != 0) {
                        sel[newSize++] = i;
                    }
                    </#if>
                    <#if operator.classHeader = "IsUnknown">
                    break;
                    </#if>
                    <#if operator.classHeader = "IsNotUnknown">
                    newSize = batchSize;
                    break;
                    </#if>
                }
            }
        }

        if(newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
        }
    }
}
    </#list>
</#list>

