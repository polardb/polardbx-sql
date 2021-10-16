<@pp.dropOutputFile />

<#list cmpOperators.binaryOperators as operator>

    <#list operator.types as type>

        <#assign className = "Filter${operator.classHeader}${type.inputDataType1}Const${type.inputDataType2}ColVectorizedExpression">
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
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${type.inputDataType1}", "${type.inputDataType2}"}, argumentKinds = {Const, Variable}, mode = ExpressionMode.FILTER)
public class ${className} extends AbstractVectorizedExpression {
    private final boolean leftIsNull;
    private final ${type.inputType1} left;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(null, outputIndex, children);
        Object leftValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        leftIsNull = (leftValue == null);
        left = leftIsNull ? (${type.inputType1})0 : (${type.inputType1}) leftValue;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[1].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock rightInputVectorSlot = chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        ${type.inputType2}[] array2 = ((${type.inputVectorType2}) rightInputVectorSlot).${type.inputType2}Array();

        int newSize = 0;
        <#if operator.classHeader != "SEQ">
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
                if(left ${operator.op} array2[j]) {
                    sel[newSize++] = j;
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if(left ${operator.op} array2[i]) {
                    sel[newSize++] = i;
                }
            }
        }
        </#if>

        <#if operator.classHeader = "SEQ">
        if(!rightInputVectorSlot.hasNull() && !leftIsNull) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(array2[j] ${operator.op} left) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(array2[i] ${operator.op} left) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (rightInputVectorSlot.hasNull() && !leftIsNull) {
            boolean[] nulls2 = rightInputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(!nulls2[j] && array2[j] ${operator.op} left) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(!nulls2[i] && array2[i] ${operator.op} left) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else if (!rightInputVectorSlot.hasNull() && leftIsNull) {
            // do nothing.
        } else {
            boolean[] nulls2 = rightInputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if(nulls2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    if(nulls2[i]) {
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

