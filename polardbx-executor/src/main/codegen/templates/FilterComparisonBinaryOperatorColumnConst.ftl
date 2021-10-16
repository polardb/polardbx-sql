<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "Filter${operator.classHeader}${type.inputDataType1}Col${type.inputDataType2}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/comparison/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionMode;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;
import com.alibaba.polardbx.optimizer.chunk.*;
import com.alibaba.polardbx.optimizer.context.EvaluationContext;

/*
* This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Variable, Const}, mode = ExpressionMode.FILTER)
public class ${className} extends AbstractVectorizedExpression {
    private final boolean rightIsNull;
    private final ${type.inputType2} right;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(null, outputIndex, children);
        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        rightIsNull = (rightValue == null);
        right = rightIsNull ? (${type.inputType2})0 : (${type.inputType2}) rightValue;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock leftInputVectorSlot = chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        ${type.inputType1}[] array1 = ((${type.inputVectorType1}) leftInputVectorSlot).${type.inputType1}Array();

        int newSize = 0;
        <#if operator.classHeader != "SEQ">
        newSize = VectorizedExpressionUtils.filterNulls(leftInputVectorSlot, isSelectionInUse, sel, batchSize);
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
                if(array1[j] ${operator.op} right) {
                    sel[newSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if(array1[i] ${operator.op} right) {
                    sel[newSize++] = i;
                }
            }
        }
        </#if>

        <#if operator.classHeader = "SEQ">
        if(!leftInputVectorSlot.hasNull() && !rightIsNull) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(array1[j] ${operator.op} right) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(array1[i] ${operator.op} right) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (leftInputVectorSlot.hasNull() && !rightIsNull) {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(!nulls1[j] && array1[j] ${operator.op} right) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(!nulls1[i] && array1[i] ${operator.op} right) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (!leftInputVectorSlot.hasNull() && rightIsNull) {
            // do nothing.
        } else {
            boolean[] nulls1 = leftInputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(nulls1[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(nulls1[i]) {
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

