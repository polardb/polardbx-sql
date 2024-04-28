package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;

import static org.apache.calcite.sql.SqlKind.N_TILE;

/**
 * Return the N th value
 *
 * @author hongxi.chx
 */
public class NTile extends Aggregator {
    private long tile;
    private long count;
    private long currentPosition;
    private long lastTilePosition = 0;
    private long currentPartition;
    private long currentTileMaxCount;
    private long currentTilePosition;

    public NTile() {
    }

    public NTile(long tile, int filterArg) {
        super(new int[0], filterArg);
        returnType = DataTypes.LongType;
        this.tile = tile;
    }

    @Override
    public SqlKind getSqlKind() {
        return N_TILE;
    }

    @Override
    protected void conductAgg(Object value) {
        assert value instanceof Row;
        count++;
    }

    @Override
    public Aggregator getNew() {
        return new NTile(tile, filterArg);
    }

    @Override
    public Object eval(Row row) {
        currentPosition++;
        if (isPartitionChange()) {
            currentTileMaxCount = (count - lastTilePosition) / tile;
            if ((count - lastTilePosition) % tile > 0) {
                currentTileMaxCount++;
            }
            currentTilePosition = 0;
            // current partition
            currentPartition++;
            // last position;
            lastTilePosition = currentPosition - 1;
        }
        currentTilePosition++;
        return currentPartition;
    }

    @Override
    public void setFunction(IFunction function) {

    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public DataType getReturnType() {
        return returnType;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"N TILE"};
    }

    @Override
    public void clear() {

    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    private boolean isPartitionChange() {
        if (currentPosition == 1) {
            return true;
        }
        if (currentTileMaxCount == currentTilePosition) {
            return true;
        }
        return false;
    }
}
