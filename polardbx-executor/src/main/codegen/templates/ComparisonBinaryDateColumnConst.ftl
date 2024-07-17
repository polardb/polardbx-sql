<@pp.dropOutputFile />

<#list cmpDateOperators.operators as operator>

    <#list operator.types as type>

        <#assign className = "${operator.classHeader}DateCol${type.constType}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
 * This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"Date", "${type.constType}"}, argumentKinds = {Variable, Const})
public class ${className} extends AbstractVectorizedExpression {
    private final boolean rightIsNull;
    private final long right;
    public ${className}(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.LongType, outputIndex, children);

        Object rightValue = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        rightIsNull = (rightValue == null);

        if (rightIsNull) {
            right = 0;
        } else {
            OriginalDate date = (OriginalDate) DataTypes.DateType.convertFrom(rightValue);
            right = date == null ? 0 : TimeStorage.writeDate(date.getMysqlDateTime());
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

		MutableChunk chunk = ctx.getPreAllocatedChunk();
		int batchSize = chunk.batchSize();
		boolean isSelectionInUse = chunk.isSelectionInUse();
		int[] sel = chunk.selection();

		RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
		RandomAccessBlock leftInputVectorSlot =
		chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

		if (leftInputVectorSlot instanceof DateBlock) {
		long[] array1 = (leftInputVectorSlot.cast(DateBlock.class)).getPacked();
		long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();

		if (rightIsNull) {
		boolean[] outputNulls = outputVectorSlot.nulls();
		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		outputNulls[j] = true;
		}
		} else {
		for (int i = 0; i < batchSize; i++) {
                        outputNulls[i] = true;
                    }
                }
            } else {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        res[j] = (array1[j] ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        res[i] = (array1[i] ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
		}
		}

		VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
		}
		} else if (leftInputVectorSlot instanceof ReferenceBlock) {
		long[] res = (outputVectorSlot.cast(LongBlock.class)).longArray();
		if (rightIsNull) {
		boolean[] outputNulls = outputVectorSlot.nulls();
		if (isSelectionInUse) {
		for (int i = 0; i < batchSize; i++) {
		int j = sel[i];
		outputNulls[j] = true;
		}
		} else {
		for (int i = 0; i < batchSize; i++) {
		outputNulls[i] = true;
                    }
                }
            } else {
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];

                        OriginalDate date = (OriginalDate) leftInputVectorSlot.elementAt(j);
                        long left = date == null ? 0 : TimeStorage.writeDate(date.getMysqlDateTime());

                        res[j] = (left ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        OriginalDate date = (OriginalDate) leftInputVectorSlot.elementAt(i);
                        long left = date == null ? 0 : TimeStorage.writeDate(date.getMysqlDateTime());

                        res[i] = (left ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    }
                }

                VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
            }
        }

    }
}

    </#list>
</#list>

