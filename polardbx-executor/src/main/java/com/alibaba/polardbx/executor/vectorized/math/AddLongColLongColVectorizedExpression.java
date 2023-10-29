package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import com.alibaba.polardbx.executor.vectorized.*;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.*;

import com.alibaba.polardbx.executor.chunk.*;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import com.alibaba.polardbx.common.utils.logger.Logger;

/*
 * This class is generated using freemarker and the ArithmeticBinaryOperatorColumnColumn.ftl template.
 */
@SuppressWarnings("unused")
@ExpressionSignatures(names = {"+", "add", "plus"}, argumentTypes = {"Long", "Long"},
    argumentKinds = {Variable, Variable})
public class AddLongColLongColVectorizedExpression extends AbstractVectorizedExpression {
    private static final Logger logger = LoggerFactory.getLogger(ScheduledJobsManager.class);
    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    private static final int LONG_BYTE = SPECIES.length();

    public AddLongColLongColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
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
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        long[] array1 = ((LongBlock) leftInputVectorSlot).longArray();
        long[] array2 = ((LongBlock) rightInputVectorSlot).longArray();
        long[] res = ((LongBlock) outputVectorSlot).longArray();

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex(),
            children[1].getOutputIndex());
        boolean[] outputNulls = outputVectorSlot.nulls();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                long right = (long) array2[j];
                res[j] = ((long) array1[j]) + right;
            }
        } else {

            boolean useSIMD = ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_SIMD);
            if (useSIMD) {
                Long beginTime = System.nanoTime();
                int i;
                int end = batchSize / LONG_BYTE * LONG_BYTE;
                for (i = 0; i < end; i += LONG_BYTE) {
                    LongVector va = LongVector.fromArray(SPECIES, array1, i);
                    LongVector vb = LongVector.fromArray(SPECIES, array2, i);
                    LongVector vc = va.add(vb);
                    vc.intoArray(res, i);
                }
                for (; i < batchSize; ++i) {
                    res[i] = ((long) array1[i] + (long) array2[i]);
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    res[i] = ((long) array1[i] + (long) array2[i]);
                }
            }
        }
    }
}


