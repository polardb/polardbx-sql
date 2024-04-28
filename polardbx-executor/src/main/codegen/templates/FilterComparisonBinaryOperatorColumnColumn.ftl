<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "Filter${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ColVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/comparison/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Variable}, mode = ExpressionMode.FILTER)
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

		RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
		RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType1}[] array1 = (leftInputVectorSlot.cast(${type.inputVectorType1}.class)).${type.inputType1}Array();
        ${type.inputType2}[] array2 = (rightInputVectorSlot.cast(${type.inputVectorType2}.class)).${type.inputType2}Array();

		int newSize = 0;
        <#if operator.classHeader != "SEQ">
			newSize = VectorizedExpressionUtils.filterNulls(leftInputVectorSlot, isSelectionInUse, sel, batchSize);
			if(newSize < batchSize) {
			chunk.setBatchSize(newSize);
			chunk.setSelectionInUse(true);
			batchSize = newSize;
			isSelectionInUse = true;
			}
        newSize = VectorizedExpressionUtils.filterNulls(rightInputVectorSlot, isSelectionInUse, sel, batchSize);
        if(newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
            batchSize = newSize;
            isSelectionInUse = true;
        }
        newSize = 0;
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                if(array1[j] ${operator.op} array2[j]) {
                    sel[newSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if(array1[i] ${operator.op} array2[i]) {
                    sel[newSize++] = i;
                }
            }
        }
        </#if>

        <#if operator.classHeader = "SEQ">
        if(!leftInputVectorSlot.hasNull() && !rightInputVectorSlot.hasNull()) {
            newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(array1[j] ${operator.op} array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(array1[i] ${operator.op} array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (leftInputVectorSlot.hasNull() && !rightInputVectorSlot.hasNull()) {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(!nulls1[j] && array1[j] ${operator.op} array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(!nulls1[i] && array1[i] ${operator.op} array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (!leftInputVectorSlot.hasNull() && rightInputVectorSlot.hasNull()) {
            boolean[] nulls2 = rightInputVectorSlot.nulls();
            newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(!nulls2[j] && array1[j] ${operator.op} array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(!nulls2[i] && array1[i] ${operator.op} array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            boolean[] nulls2 = rightInputVectorSlot.nulls();
            newSize = 0;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if((nulls1[j] && nulls2[j]) || ((!nulls1[j] && !nulls2[j]) && array1[j] ${operator.op} array2[j])) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if((nulls1[i] && nulls2[i]) || ((!nulls1[i] && !nulls2[i]) && array1[i] ${operator.op} array2[i])) {
                        sel[newSize++] = i;
                    }
                }
            }
        }
        </#if>
        if(newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
        }
    }
}

    </#list>
</#list>

