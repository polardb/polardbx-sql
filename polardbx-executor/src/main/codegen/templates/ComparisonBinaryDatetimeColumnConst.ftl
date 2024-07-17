<@pp.dropOutputFile />
<#assign argTypes = ["Datetime", "Timestamp"]>
<#list argTypes as argType>
    <#list cmpDateOperators.operators as operator>
        <#list operator.types as type>

        <#assign className = "${operator.classHeader}${argType}Col${type.constType}ConstVectorizedExpression">
        <@pp.changeOutputFile name="/com/alibaba/polardbx/executor/vectorized/compare/${className}.java" />
package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.chunk.TimestampBlock;
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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
 * This class is generated using freemarker and the ${.template_name} template.
*/
@SuppressWarnings("unused")
@ExpressionSignatures(names = {${operator.functionNames}}, argumentTypes = {"${argType}", "${type.constType}"}, argumentKinds = {Variable, Const})
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
            OriginalTimestamp datetime = (OriginalTimestamp) DateTimeType.DATE_TIME_TYPE_6.convertFrom(rightValue);
            right = datetime == null ? 0 : TimeStorage.writeTimestamp(datetime.getMysqlDateTime());
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

			if (leftInputVectorSlot instanceof TimestampBlock) {
			long[] array1 = (leftInputVectorSlot.cast(TimestampBlock.class)).getPacked();
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

                        OriginalTimestamp datetime = (OriginalTimestamp) leftInputVectorSlot.elementAt(j);
                        long left = datetime == null ? 0 : TimeStorage.writeTimestamp(datetime.getMysqlDateTime());

                        res[j] = (left ${operator.op} right) ? LongBlock.TRUE_VALUE : LongBlock.FALSE_VALUE;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        OriginalTimestamp datetime = (OriginalTimestamp) leftInputVectorSlot.elementAt(i);
                        long left = datetime == null ? 0 : TimeStorage.writeTimestamp(datetime.getMysqlDateTime());

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
</#list>
