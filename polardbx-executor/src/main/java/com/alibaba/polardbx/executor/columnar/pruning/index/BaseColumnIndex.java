package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;

/**
 * @author fangwu
 */
public abstract class BaseColumnIndex implements ColumnIndex {
    private long rgNum;

    protected BaseColumnIndex(long rgNum) {
        this.rgNum = rgNum;
    }

    @Override
    public long rgNum() {
        return rgNum;
    }

    /**
     * Use this method to force index handling the same type of data internal.
     * Index data should be written by same data type, and the datatype could also handle comparison correctly.
     *
     * @param value the origin value of column datatype
     * @return return the same type with index data
     * @throws IllegalArgumentException if value was not meet the requirements, throw IllegalArgumentException
     */
    protected Long paramTransform(Object value, DataType dt) {
        if (value == null || dt == null) {
            return null;
        }
        if (DataTypeUtil.isIntType(dt)) {
            return ((Number) value).longValue();
        }
        if (DataTypeUtil.isDateType(dt)) {
            MysqlDateTime date = DataTypeUtil.toMySQLDatetime(value, dt.getSqlType());
            if (date == null) {
                return null;
            }
            return date.toPackedLong();
        }
        throw new IllegalArgumentException("not supported index value:" + value);
    }
}
