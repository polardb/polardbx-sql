package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionMode;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

/*
 * This class is generated using freemarker and the FilterComparisonBinaryOperatorColumnColumn.ftl template.
 */
@SuppressWarnings("unused")
@ExpressionSignatures(names = {"le", "<="}, argumentTypes = {"Integer", "Integer"},
    argumentKinds = {Variable, Variable}, mode = ExpressionMode.FILTER)
public class FilterLEIntegerColIntegerColVectorizedExpression extends AbstractVectorizedExpression {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    public FilterLEIntegerColIntegerColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(null, outputIndex, children);
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());
        RandomAccessBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        int[] array1 = ((IntegerBlock) leftInputVectorSlot).intArray();
        int[] array2 = ((IntegerBlock) rightInputVectorSlot).intArray();

        int newSize = 0;
        newSize = VectorizedExpressionUtils.filterNulls(leftInputVectorSlot, isSelectionInUse, sel, batchSize);
        if (newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
            batchSize = newSize;
            isSelectionInUse = true;
        }
        newSize = VectorizedExpressionUtils.filterNulls(rightInputVectorSlot, isSelectionInUse, sel, batchSize);
        if (newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
            batchSize = newSize;
            isSelectionInUse = true;
        }
        newSize = 0;
        if (isSelectionInUse) {
            boolean useSIMD = ctx.getExecutionContext().getParamManager().getBoolean(ConnectionParams.ENABLE_SIMD);
            if (useSIMD) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    if (array1[j] <= array2[j]) {
                        sel[newSize++] = j;
                    }
                }
            } else {
                int i;
                int end = SPECIES.loopBound(batchSize);
                for (i = 0; i < end; i += SPECIES.length()) {
                    IntVector va = IntVector.fromArray(SPECIES, array1, 0, sel, i);
                    IntVector vb = IntVector.fromArray(SPECIES, array2, 0, sel, i);
                    VectorMask<Integer> vc = va.compare(VectorOperators.LE, vb);
                    if (!vc.anyTrue()) {
                        continue;
                    }
                    int res = (int) vc.toLong();
                    while (res != 0) {
                        int last = res & -res;
                        sel[newSize++] = i + Integer.numberOfTrailingZeros(last);
                        res ^= last;
                    }
                }
                for (; i < batchSize; ++i) {
                    if (array1[i] <= array2[i]) {
                        sel[newSize++] = i;
                    }
                }
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                if (array1[i] <= array2[i]) {
                    sel[newSize++] = i;
                }
            }
        }

        if (newSize < batchSize) {
            chunk.setBatchSize(newSize);
            chunk.setSelectionInUse(true);
        }
    }
}


