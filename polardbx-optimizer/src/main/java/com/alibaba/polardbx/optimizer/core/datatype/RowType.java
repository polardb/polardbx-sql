package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.expression.build.Rex2ExprUtil;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.alibaba.polardbx.common.type.MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
import static com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil.getTypeOfObject;

public class RowType extends AbstractDataType<RowValue> {
    private List<DataType> dataTypes;

    public RowType() {
    }

    public RowType(List<DataType> dataTypes) {
        this.dataTypes = dataTypes;
    }
    @Override
    public Class getDataClass() {
        return RowValue.class;
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                Object val = rs.getString(index);
                return convertFrom(val);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public RowValue getMaxValue() {
        return null;
    }

    @Override
    public RowValue getMinValue() {
        return null;
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("row类型不支持计算操作");
    }

    @Override
    public int getSqlType() {
        return UNDECIDED_SQL_TYPE;
    }

    @Override
    public String getStringSqlType() {
        return "VARCHAR";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MYSQL_TYPE_VAR_STRING;
    }

    public Long nullNotSafeEqual(Object o1, Object o2) {
        if (o1 == o2) {
            if (o1 == null || o2 == null) {
                return null;
            } else {
                return 1L;
            }
        }
        RowValue no1 = convertFrom(o1);
        RowValue no2 = convertFrom(o2);
        int num1 = no1.getValues().size();
        int num2 = no2.getValues().size();
        if (num1 != num2) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Operand should contain %s column(s)", num1));
        }
        boolean hasNull = false;
        for (int i = 0; i < num1; i++) {
            Object val1 = no1.getValues().get(i);
            Object val2 = no2.getValues().get(i);
            if (val1 == null || val2 == null) {
                hasNull = true;
                continue;
            } else {
                DataType type = inferCmpType(getTypeOfObject(val1), getTypeOfObject(val2));
                int ret = type.compare(val1, val2);
                if (ret != 0) {
                    return 0L;
                }
            }
        }
        return hasNull ? null : 1L;
    }

    public Integer nullNotSafeCompare(Object o1, Object o2, boolean mustGreater) {
        if (o1 == o2) {
            if (o1 == null || o2 == null) {
                return null;
            } else {
                return mustGreater ? -1 : 0;
            }
        }
        RowValue no1 = convertFrom(o1);
        RowValue no2 = convertFrom(o2);
        int num1 = no1.getValues().size();
        int num2 = no2.getValues().size();
        if (num1 != num2) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Operand should contain %s column(s)", num1));
        }
        for (int i = 0; i < num1; i++) {
            Object val1 = no1.getValues().get(i);
            Object val2 = no2.getValues().get(i);
            if (val1 == null || val2 == null) {
                return null;
            } else {
                DataType type = inferCmpType(getTypeOfObject(val1), getTypeOfObject(val2));
                int ret = type.compare(val1, val2);
                if (ret != 0) {
                    return ret;
                }
            }
        }
        return mustGreater ? -1 : 0;
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        RowValue no1 = convertFrom(o1);
        RowValue no2 = convertFrom(o2);
        int num1 = no1.getValues().size();
        int num2 = no2.getValues().size();
        if (num1 != num2) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Operand should contain %s column(s)", num1));
        }
        for (int i = 0; i < num1; i++) {
            Object val1 = no1.getValues().get(i);
            Object val2 = no2.getValues().get(i);
            if (val1 == val2) {
                continue;
            }
            if (val1 == null) {
                return -1;
            } else if (val2 == null) {
                return 1;
            } else {
                DataType type = inferCmpType(getTypeOfObject(val1), getTypeOfObject(val2));
                int ret = type.compare(val1, val2);
                if (ret != 0) {
                    return ret;
                }
            }
        }
        return 0;
    }

    private DataType inferCmpType(DataType leftType, DataType rightType) {
        if (leftType == DataTypes.RowType || rightType == DataTypes.RowType) {
            return DataTypes.RowType;
        } else {
            return Rex2ExprUtil.compareTypeOf(leftType, rightType, true, true);
        }
    }
}
