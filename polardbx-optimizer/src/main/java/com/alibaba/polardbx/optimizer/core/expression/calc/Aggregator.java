package com.alibaba.polardbx.optimizer.core.expression.calc;

import com.alibaba.polardbx.optimizer.core.expression.bean.FunctionSignature;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumMerge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSumV2Merge;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.DenseRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.GroupConcat;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Lag;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.LastValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NThValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.NTile;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CheckSum;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.PercentRank;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.Rank;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.memory.ObjectSizeUtils;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IExtraFunction;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by chuanqin on 17/8/9.
 */
public abstract class Aggregator extends AbstractExpression implements IExtraFunction {

    /**
     * Incrementally aggregate the value with the current row
     */
    protected int[] aggTargetIndexes;

    protected boolean isDistinct;

    protected Field resultField;

    protected DataType returnType;

    private final HashSet<Object> effectRows;

    protected final MemoryAllocatorCtx memoryAllocator;

    protected final int filterArg;

    // For some suck reasons we need a default constructor here
    public Aggregator() {
        this(null, false, -1);
    }

    public Aggregator(int[] targetIndexes, int filterArg) {
        this(targetIndexes, false, filterArg);
    }

    /**
     * Constructor for Non-distinct Aggregators
     */
    public Aggregator(int[] targetIndexes, boolean distinct, int filterArg) {
        Preconditions.checkArgument(!distinct, "Please use distinct constructor");
        this.aggTargetIndexes = targetIndexes;
        this.isDistinct = false;
        this.memoryAllocator = null;
        this.effectRows = null;
        this.filterArg = filterArg;
    }

    /**
     * Constructor for Distinct Aggregators (which contains a HashSet to remove duplicates)
     */
    public Aggregator(int[] targetIndexes, boolean distinct, MemoryAllocatorCtx memoryAllocator, int filterArg) {
        this.aggTargetIndexes = targetIndexes;
        this.isDistinct = distinct;
        this.memoryAllocator = memoryAllocator;
        if (distinct) {
            this.effectRows = new HashSet<>();
        } else {
            this.effectRows = null; // to save memory usage
        }
        this.filterArg = filterArg;
    }

    public int getFilterArg() {
        return filterArg;
    }

    public abstract SqlKind getSqlKind();

    public boolean isDistinct() {
        return isDistinct;
    }

    public int[] getAggTargetIndexes() {
        return aggTargetIndexes;
    }

    public int[] getInputColumnIndexes() {
        // default input column indexes is the same with aggTargetIndexes
        return aggTargetIndexes;
    }

    protected abstract void conductAgg(Object value);

    public void aggregate(Row row) {
        if (row == null) {
            GeneralUtil.nestedException("illegal aggregate operation: aggregate row of null");
        }

        if (this instanceof GroupConcat) {
            aggregateGroupConcat(row);
            return;
        }

        if (this instanceof Rank || this instanceof DenseRank || this instanceof PercentRank) {
            aggregateRank(row);
            return;
        }

        if (this instanceof CheckSum || this instanceof CheckSumMerge) {
            aggregateChecksum(row);
            return;
        }

        if (this instanceof CheckSumV2 || this instanceof CheckSumV2Merge) {
            aggregateChecksumV2(row);
            return;
        }

        if (aggTargetIndexes.length == 0) { // e.g. COUNT(*)
            if (isDistinct) {
                Row arrayRow = FunctionUtils.fromIRowSetToArrayRowSet(row);
                if (effectRows.add(arrayRow)) {
                    memoryAllocator.allocateReservedMemory(arrayRow);
                    conductAgg(arrayRow);
                }
            } else {
                conductAgg(row);
            }
            return;
        }

        boolean notContainsNull = true;
        Object[] aggValueSet = new Object[aggTargetIndexes.length];
        for (int i = 0; i < aggTargetIndexes.length; i++) {
            aggValueSet[i] = row.getObject(aggTargetIndexes[i]);
            notContainsNull = notContainsNull && (aggValueSet[i] != null);
            // for count(distinct a,b), {x, null} and {null, y} are regarded
            // as a null row, which will not be counted.
        }
        if (aggValueSet[0] != null) { // for
            if (returnType == null) {
                returnType = DataTypeUtil.getTypeOfObject(aggValueSet[0]);
            }
        }
        if (isDistinct) {
            assert memoryAllocator != null && effectRows != null;
            ArrayRow aggValueRow = new ArrayRow(aggValueSet);
            if (effectRows.add(aggValueRow) && notContainsNull) {
                // use ArrayRowSet as decorator to do distinct operation
                memoryAllocator.allocateReservedMemory(aggValueRow);
                conductAgg(aggValueSet[0]);
            }
        } else if (this instanceof NThValue) {
            /* NTHValue calculate null value */
            conductAgg(aggValueSet[0]);
        } else if (this instanceof Lag) {
            /* NTHValue calculate null value */
            conductAgg(aggValueSet[0]);
        } else if (this instanceof FirstValue || this instanceof LastValue || this instanceof NTile) {
            conductAgg(aggValueSet[0]);
        } else if (aggValueSet[0] != null) {
            conductAgg(aggValueSet[0]);
        }
    }

    private void aggregateChecksum(Row row) {
        conductAgg(row);
    }

    private void aggregateRank(Row row) {
        conductAgg(row);
    }

    private void aggregateChecksumV2(Row row) {
        conductAgg(row);
    }

    private void aggregateGroupConcat(Row row) {
        boolean notContainsNull = true;
        if (returnType == null) {
            returnType = DataTypes.StringType;
        }
        assert aggTargetIndexes.length > 0;
        Object[] aggValueSet = new Object[aggTargetIndexes.length];
        for (int i = 0; i < aggTargetIndexes.length; i++) {
            aggValueSet[i] = row.getObject(aggTargetIndexes[i]);
            notContainsNull = notContainsNull && (aggValueSet[i] != null);
        }
        if (isDistinct) {
            assert memoryAllocator != null && effectRows != null;
            ArrayRow aggValueRow = new ArrayRow(aggValueSet);
            memoryAllocator.allocateReservedMemory(aggValueRow.estimateSize()
                + ObjectSizeUtils.HASH_ENTRY_SIZE + ObjectSizeUtils.REFERENCE_SIZE * 2);
            if (effectRows.add(aggValueRow) && notContainsNull) {
                // use ArrayRowSet as decorator to do distinct operation
                conductAgg(row);
            }
        } else if (notContainsNull) {
            conductAgg(row);
        }
    }

    @Override
    public void setFunction(IFunction function) {

    }

    @Override
    public Object evaluateResult(Object[] args, ExecutionContext ec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object compute() {
        throw new UnsupportedOperationException();
    }

    public Object value() {
        return this.eval(null);
    }

    @Override
    public Object computeWithArgs(Object[] funArgs, Map<String, Object> extraParams) {
        throw new UnsupportedOperationException();
    }

    public abstract Aggregator getNew();

    public void setAggTargetIndexes(int[] aggTargetIndexes) {
        this.aggTargetIndexes = aggTargetIndexes;
    }

    /**
     * <pre>
     * this method is not a copy as common,just for accumulate agg,
     * because its target index is inited from zero.
     * </pre>
     */
    public Aggregator getNewForAccumulator() {
        Aggregator newAggregator = getNew();
        // change targetIndexes to be compatible with accumulator inputChunk
        if (newAggregator.aggTargetIndexes != null) {
            int[] newAggTargetIndexes = new int[newAggregator.aggTargetIndexes.length];
            for (int i = 0; i < newAggTargetIndexes.length; i++) {
                newAggTargetIndexes[i] = i;
            }
            newAggregator.aggTargetIndexes = newAggTargetIndexes;
        }
        // accumulator do not need aggregator to do distinct by itself
        newAggregator.isDistinct = false;
        return newAggregator;
    }

    protected List<Object> getAggregateKey(Row row) {
        if (row == null) {
            return null;
        }
        if (aggTargetIndexes == null || aggTargetIndexes.length == 0) {
            return null;
        }
        List<Object> lastRowValues = row.getValues();
        List<Object> aggTargetIndexValues = new ArrayList<>();
        for (int index : aggTargetIndexes) {
            aggTargetIndexValues.add(lastRowValues.get(index));
        }
        return aggTargetIndexValues;
    }

    @Override
    public void setResultField(Field field) {
        this.resultField = field;
    }

    @Override
    public FunctionSignature[] getFunctionSignature() {
        return new FunctionSignature[0];
    }
}
