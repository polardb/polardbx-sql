package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author fangwu
 */
public class BloomFilterIndex extends BaseColumnIndex {
    protected BloomFilterIndex(long rgNum) {
        super(rgNum);
    }

    @Override
    public boolean checkSupport(int columnId, SqlTypeName columnType) {
        return false;
    }

    @Override
    public DataType getColumnDataType(int columnId) {
        return null;
    }
}
