package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * @author fangwu
 */
public interface ColumnIndex {
    long rgNum();

    boolean checkSupport(int columnId, SqlTypeName columnType);

    DataType getColumnDataType(int columnId);
}
