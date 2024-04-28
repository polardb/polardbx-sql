package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.Nullable;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Optional;

/**
 * @author fangwu
 */
public class IndexPruneContext {
    private Parameters parameters;
    private ColumnarTracer pruneTracer;

    public Object acquireFromParameter(int paramIndex, DataType dataType, SqlTypeName type) {
        return parameters.getCurrentParameter().get(paramIndex).getValue();
    }

    public Object[] acquireArrayFromParameter(int paramIndex, DataType dataType, SqlTypeName typeName) {
        Object value = parameters.getCurrentParameter().get(paramIndex).getValue();
        if (value instanceof RawString) {
            RawString rs = (RawString) value;
            return rs.getObjList().toArray();
        } else {
            return null;
        }
    }

    @Nullable
    public Object transformToObject(DataType dataType, SqlTypeName typeName, Object value) {
        switch (typeName) {
        case TIMESTAMP:
        case DATE:
        case DATETIME:
        case TIME:
            return packDateTypeToLong(dataType, value);
        default:
            return value;
        }
    }

    private static long packDateTypeToLong(DataType dataType, Object obj) {
        if (DataTypeUtil.equalsSemantically(DataTypes.TimestampType, dataType) ||
            DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)) {
            Timestamp timestamp = (Timestamp) dataType.convertFrom(obj);
            return Optional.ofNullable(timestamp)
                .map(TimeStorage::packDatetime)
                .orElse(-1L);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType)) {
            Date date = (Date) dataType.convertFrom(obj);
            return Optional.ofNullable(date)
                .map(TimeStorage::packDate)
                .orElse(-1L);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.TimeType, dataType)) {
            Time time = (Time) dataType.convertFrom(obj);
            return Optional.ofNullable(time)
                .map(TimeStorage::packTime)
                .orElse(-1L);
        } else if (DataTypeUtil.equalsSemantically(DataTypes.YearType, dataType)) {
            Long year = (Long) dataType.convertFrom(obj);
            return Optional.ofNullable(year)
                .orElse(-1L);
        }
        throw new IllegalStateException("Unexpected value: " + dataType);
    }

    public Parameters getParameters() {
        return parameters;
    }

    public void setParameters(Parameters parameters) {
        this.parameters = parameters;
    }

    public ColumnarTracer getPruneTracer() {
        return pruneTracer;
    }

    public void setPruneTracer(ColumnarTracer pruneTracer) {
        this.pruneTracer = pruneTracer;
    }
}
