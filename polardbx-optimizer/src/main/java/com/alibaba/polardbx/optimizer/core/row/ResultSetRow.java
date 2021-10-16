/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.row;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BufferResultSet;
import com.alibaba.polardbx.common.jdbc.InvalidDate;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.EnumType;
import com.alibaba.polardbx.optimizer.core.datatype.TinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.UTinyIntType;
import com.alibaba.polardbx.optimizer.core.datatype.YearType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:06:10
 * @since 5.0.0
 */
public class ResultSetRow extends AbstractRow implements Row {

    ResultSet rs = null;
    Object cache[] = null;

    public ResultSetRow(CursorMeta meta, ResultSet rs) throws SQLException {
        super(meta);
        this.rs = rs;
        int colCount = rs.getMetaData().getColumnCount();
        this.colNum = colCount;
        cache = new Object[colCount];
        throw new AssertionError("ResultSetRow unreachable");
    }

    public ResultSetMetaData getOriginMeta() throws SQLException {
        return rs.getMetaData();
    }

    @Override
    public Object getObject(int index) {

        if (cache != null && cache[index] != null) {
            return cache[index];
        }

        try {
            int actIndex = index + 1;
            Object obValue;
            try {
                obValue = rs.getObject(actIndex);
            } catch (SQLException ex) {
                if (TStringUtil.containsIgnoreCase(ex.getMessage(), "can not be represented as java.sql.Date")) {
                    // Mainly for year type
                    return InvalidDate.instance;
                } else if (rs.getMetaData().getColumnType(actIndex) == 92 && TStringUtil
                    .containsIgnoreCase(ex.getMessage(), "Bad format for Time")) {
                    try {
                        byte[] rawBytes = new byte[rs.getAsciiStream(actIndex).available()];
                        rs.getAsciiStream(actIndex).read(rawBytes);
                        String str = new String(rawBytes);
                        //hh:mm:ss format, remove the millsecond part
                        if (str.indexOf('.') != -1) {
                            str = str.substring(0, str.indexOf('.'));
                        }
                        Time t = Time.valueOf(str);
                        return t;
                    } catch (Exception e) {
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            }

            ColumnMeta cm = this.getParentCursorMeta().getColumnMeta(index);
            if (obValue != null) {
                DataType dataType = cm.getDataType();

                if (dataType instanceof EnumType) {
                    obValue = new EnumValue((EnumType) dataType, DataTypes.StringType.convertFrom(obValue));
                }

                // boolean类型可以表示tinyint1范围的数字，直接getobject返回的是true/false，丢精度
                if (dataType instanceof BooleanType

                    || (dataType instanceof TinyIntType && obValue instanceof Boolean)
                    || (dataType instanceof UTinyIntType && obValue instanceof Boolean)) {
                    obValue = rs.getInt(actIndex);
                }

                // For now, we only support YEAR(4).
                if (dataType instanceof YearType && obValue instanceof Date) {
                    if (obValue instanceof ZeroDate) {
                        obValue = 0;
                    } else {
                        obValue = ((Date) obValue).getYear() + 1900;
                    }
                }
            }

            if (rs.wasNull()) {
                return null;
            }

            cache[index] = obValue;
            return obValue;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public void setObject(int index, Object value) {
        throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "ResultSetRowSet.setObject()");
    }

    @Override
    public List<Object> getValues() {
        List<Object> res = new ArrayList<Object>();
        for (int i = 0; i < getParentCursorMeta().getColumns().size(); i++) {
            res.add(this.getObject(i));
        }
        return res;
    }

    @Override
    public Boolean getBoolean(int index) {
        try {
            int actIndex = index + 1;
            boolean bool = rs.getBoolean(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return bool;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Date getDate(int index) {
        try {
            int actIndex = index + 1;
            return rs.getDate(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }

    }

    @Override
    public Blob getBlob(int index) {
        try {
            int actIndex = index + 1;
            return rs.getBlob(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Clob getClob(int index) {
        try {
            int actIndex = index + 1;
            return rs.getClob(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Double getDouble(int index) {
        try {
            int actIndex = index + 1;
            double d = rs.getDouble(actIndex);
            if (rs.wasNull()) {
                return null;
            } else {
                return d;
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Float getFloat(int index) {
        try {
            int actIndex = index + 1;
            float f = rs.getFloat(actIndex);
            if (rs.wasNull()) {
                return null;
            } else {
                return f;
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Integer getInteger(int index) {
        try {
            int actIndex = index + 1;
            int inte = rs.getInt(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return inte;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Long getLong(int index) {
        try {
            int actIndex = index + 1;
            long l = rs.getLong(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return l;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Short getShort(int index) {
        try {
            int actIndex = index + 1;
            short s = rs.getShort(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return s;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Byte getByte(int index) {
        try {
            int actIndex = index + 1;
            byte b = rs.getByte(actIndex);
            if (rs.wasNull()) {
                return null;
            }
            return b;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public String getString(int index) {
        try {
            int actIndex = index + 1;
            return rs.getString(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Timestamp getTimestamp(int index) {
        try {
            int actIndex = index + 1;
            return rs.getTimestamp(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public Time getTime(int index) {
        try {
            int actIndex = index + 1;
            return rs.getTime(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public byte[] getBytes(int index) {
        try {
            int actIndex = index + 1;
            return rs.getBytes(actIndex);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        // 直接代理到jdbc的getBytes返回
        return getBytes(index);
    }

    /**
     * Get the internal ResultSet (used in CursorExec)
     */
    public ResultSet getResultSet() {
        return rs;
    }

    public void clearCache() {
        if (cache != null) {
            cache = new Object[cache.length];
        }
    }

    @Override
    public long estimateSize() {
        if (rs != null && rs instanceof BufferResultSet) {
            return ((BufferResultSet) rs).estimateCurrentSetSize();
        }
        return 0;
    }
}
