package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.apache.calcite.sql.SqlOperator;

/**
 * @author chenghui.lch
 * <p>
 * The router info of the partFunc of the part key
 */
public class PartFuncExprRouterInfo {

    /**
     * The part key of the part func rounter info
     */
    protected int partKeyIndex;

    /**
     * The part func expr of part key 0, just for range/list/hash
     */
    protected SqlOperator partFuncOperator = null;

    /**
     * The flag that label if need eval the part func value for the first part key by using the expr value of predicate
     */
    protected boolean needEvalPartFunc = false;

    /**
     * the return data type of the partFuncOperator
     */
    protected DataType partExprReturnType = null;

    /**
     * The part func definition
     */
    protected PartitionIntFunction partFunc;

    public PartFuncExprRouterInfo() {
    }

    public int getPartKeyIndex() {
        return partKeyIndex;
    }

    public void setPartKeyIndex(int partKeyIndex) {
        this.partKeyIndex = partKeyIndex;
    }

    public SqlOperator getPartFuncOperator() {
        return partFuncOperator;
    }

    public void setPartFuncOperator(SqlOperator partFuncOperator) {
        this.partFuncOperator = partFuncOperator;
    }

    public boolean isNeedEvalPartFunc() {
        return needEvalPartFunc;
    }

    public void setNeedEvalPartFunc(boolean needEvalPartFunc) {
        this.needEvalPartFunc = needEvalPartFunc;
    }

    public DataType getPartExprReturnType() {
        return partExprReturnType;
    }

    public void setPartExprReturnType(DataType partExprReturnType) {
        this.partExprReturnType = partExprReturnType;
    }

    public PartitionIntFunction getPartFunc() {
        return partFunc;
    }

    public void setPartFunc(PartitionIntFunction partFunc) {
        this.partFunc = partFunc;
    }
}
