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

package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowSumCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.BooleanType;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author mengshi.sunmengshi 2013-11-22 下午3:26:23
 * @since 5.0.0
 */
public class TResultSet implements ResultSet {

    // Has this result set been closed?
    protected boolean isClosed = false;
    private ResultCursor resultCursor;
    private Row currentKVPair;
    private Row cacheRowToBuildMeta = null;
    private TResultSetMetaData resultSetMetaData = null;
    private boolean wasNull;
    private Map<String, Integer> columnLabelToIndex;
    private Map<String, Integer> fullColumnNameToIndex;
    private Map<String, Integer> columnNameToIndex;
    private boolean hasBuiltIndexMapping = false;
    private final Map<String, Integer> columnToIndexCache = new HashMap<>();
    private boolean useColumnNamesInFindColumn = false;
    private Map<String, Object> extraCmd;

    public TResultSet(ResultCursor resultCursor, Map<String, Object> extraCmd) {
        this.resultCursor = resultCursor;
        this.extraCmd = extraCmd;
    }

    public void buildIndexMapping() throws SQLException {
        int numFields = this.getMetaData().getColumnCount();
        this.columnLabelToIndex = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.fullColumnNameToIndex = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        this.columnNameToIndex = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        // We do this in reverse order, so that the 'first' column
        // with a given name ends up as the final mapping in the
        // hashtable...
        //
        // Quoting the JDBC Spec:
        //
        // "Column names used as input to getter
        // methods are case insensitive. When a getter method is called with a
        // column
        // name and several columns have the same name, the value of the first
        // matching column will be returned. "
        //

        List<ColumnMeta> cms = this.getMetaData().getColumnMetas();
        for (int i = numFields - 1; i >= 0; i--) {
            Integer index = i;
            String columnName = cms.get(i).getName();
            String columnLabel = cms.get(i).getAlias() == null ? columnName : cms.get(i).getAlias();

            String fullColumnName = cms.get(i).getFullName();

            if (columnLabel != null) {
                this.columnLabelToIndex.put(columnLabel, index);
            }

            if (fullColumnName != null) {
                this.fullColumnNameToIndex.put(fullColumnName, index);
            }

            if (columnName != null) {
                this.columnNameToIndex.put(columnName, index);
            }
        }

        // set the flag to prevent rebuilding...
        this.hasBuiltIndexMapping = true;
    }

    public Row getCurrentKVPair() {
        return currentKVPair;
    }

    @Override
    public synchronized int findColumn(String columnName) throws SQLException {
        Integer index;
        checkClosed();

        if (!this.hasBuiltIndexMapping) {
            buildIndexMapping();
        }

        index = this.columnToIndexCache.get(columnName);
        if (index != null) {
            return index + 1;
        }

        index = this.columnLabelToIndex.get(columnName);
        if (index == null && this.useColumnNamesInFindColumn) {
            index = this.columnNameToIndex.get(columnName);
        }

        if (index == null) {
            index = this.fullColumnNameToIndex.get(columnName);
        }

        if (index != null) {
            this.columnToIndexCache.put(columnName, index);
            return index + 1;
        }

        // Try this inefficient way, now

        List<ColumnMeta> cms = this.getMetaData().getColumnMetas();
        for (int i = 0; i < cms.size(); i++) {
            String cn = cms.get(i).getAlias() == null ? cms.get(i).getName() : cms.get(i).getAlias();
            if (columnName.equalsIgnoreCase(cn)) {
                return i + 1;
            } else if (columnName.equalsIgnoreCase(cms.get(i).getFullName())) {
                return i + 1;
            }
        }

        throw new SQLException("column " + columnName + " doesn't exist!, " + this.getMetaData().getColumnMetas());
    }

    // 游标指向下一跳记录
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        Row kvPair;
        try {
            if (cacheRowToBuildMeta != null) {
                kvPair = cacheRowToBuildMeta;
                cacheRowToBuildMeta = null;
            } else {
                kvPair = resultCursor.next();
            }

            this.currentKVPair = kvPair;
        } catch (Exception e) {
            this.currentKVPair = null;
            throw GeneralUtil.nestedException(e);
        }
        if (null != kvPair) {
            return true;
        } else {
            return false;
        }
    }

    public int getAffectRows() throws SQLException {
        Cursor cursor = this.resultCursor.getCursor();
        if (cursor instanceof AffectRowCursor || cursor instanceof AffectRowSumCursor) {
            if (currentKVPair != null || next()) {
                return currentKVPair.getInteger(0);
            } else {
                return 0;
            }
        }
        return -1;
    }

    private void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("ResultSet.Operation_not_allowed_after_ResultSet_closed");
        }
    }

    @Override
    public void close() throws SQLException {
        close(null);
    }

    public void close(Throwable ex) throws SQLException {
        if (isClosed) {
            return;
        }
        try {
            this.resultSetMetaData = null;
            List<Throwable> exs = new ArrayList<>();
            if (ex != null) {
                exs.add(ex);
            }
            exs = this.resultCursor.close(exs);
            if (!exs.isEmpty()) {
                throw GeneralUtil.mergeException(exs);
            }
            isClosed = true;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    private void validateColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 0 || columnIndex > this.getMetaData().getColumnCount()) {
            throw new SQLException("columnIndex 越界，column size：" + this.getMetaData().getColumnCount());
        }
    }

    /**
     * @param logicalIndex 用户select时的index
     * @return IRowSet中实际的index
     */
    private int getActualIndex(int logicalIndex) {
        return logicalIndex;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        String str = currentKVPair.getString(getActualIndex(columnIndex));
        if (str == null) {
            wasNull = true;
            return str;
        } else {
            wasNull = false;
            return str;
        }
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Boolean bool = currentKVPair.getBoolean(getActualIndex(columnIndex));
        if (null == bool) {
            wasNull = true;
            return false;
        } else {
            wasNull = false;
            return bool;
        }
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Short st = currentKVPair.getShort(getActualIndex(columnIndex));
        if (st == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return st;
        }
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Integer inte = currentKVPair.getInteger(getActualIndex(columnIndex));
        if (inte == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return inte;
        }
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Long l = currentKVPair.getLong(getActualIndex(columnIndex));
        if (l == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return l;
        }
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));

    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Float fl = currentKVPair.getFloat(getActualIndex(columnIndex));
        if (fl == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return fl;
        }
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Double doub = currentKVPair.getDouble(getActualIndex(columnIndex));
        if (doub == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return doub;
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        byte[] bytes = currentKVPair.getBytes(getActualIndex(columnIndex));
        if (bytes == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return bytes;
        }
    }

    public byte[] getBytes(String columnLabel, String encoding) throws SQLException {
        return getBytes(findColumn(columnLabel), encoding);
    }

    public byte[] getBytes(int columnIndex, String encoding) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        // byte[] bytes = currentKVPair.getBytes(getActualIndex(columnIndex),
        // encoding);
        byte[] bytes = currentKVPair.getBytes(columnIndex, encoding);
        if (bytes == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return bytes;
        }
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Date date = currentKVPair.getDate(getActualIndex(columnIndex));

        if (date instanceof ZeroDate) {
            throw new SQLException("Value '0000-00-00' can not be represented as java.sql.Date");
        }
        if (date == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return date;
        }
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Byte b = currentKVPair.getByte(getActualIndex(columnIndex));
        if (b == null) {
            wasNull = true;
            return 0;
        } else {
            wasNull = false;
            return b;
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Timestamp ts = currentKVPair.getTimestamp(getActualIndex(columnIndex));

        if (ts instanceof ZeroTimestamp) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
        }
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return ts;
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnIndex);

        if (ts instanceof ZeroTimestamp) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
        }
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            cal.setTimeInMillis(ts.getTime());
            return new Timestamp(cal.getTimeInMillis());
        }
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        Timestamp ts = getTimestamp(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            cal.setTimeInMillis(ts.getTime());
            return new Timestamp(cal.getTimeInMillis());
        }
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        Timestamp ts = getTimestamp(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return new Time(ts.getTime());
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Time ts = currentKVPair.getTime(getActualIndex(columnIndex));

        if (ts instanceof ZeroTime) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Time");
        }

        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return ts;
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Time ts = currentKVPair.getTime(getActualIndex(columnIndex));

        if (ts instanceof ZeroTime) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Time");
        }

        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            cal.setTimeInMillis(ts.getTime());
            return new Time(cal.getTimeInMillis());
        }
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        Time ts = getTime(columnLabel);
        if (ts == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            cal.setTimeInMillis(ts.getTime());
            return new Time(cal.getTimeInMillis());
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Object obj = currentKVPair.getObject(getActualIndex(columnIndex));

        if (obj instanceof ZeroTimestamp) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Timestamp");
        } else if (obj instanceof ZeroDate) {
            throw new SQLException("Value '0000-00-00' can not be represented as java.sql.Date");
        } else if (obj instanceof ZeroTime) {
            throw new SQLException("Value '0000-00-00 00:00:00' can not be represented as java.sql.Time");
        }
        DataType type = this.getMetaData().getColumnMetas().get(columnIndex).getDataType();
        if (DataTypeUtil.equalsSemantically(type, DataTypes.BooleanType) || DataTypeUtil
            .equalsSemantically(type, DataTypes.BitType)) {
            obj = DataTypes.BooleanType.convertFrom(obj);
            if (obj != null) {
                obj = BooleanType.isTrue((Integer) obj);
            }
        }

        if (DataTypeUtil.equalsSemantically(type, DataTypes.JsonType) && obj instanceof byte[]) {
            obj = new String((byte[]) obj, StandardCharsets.UTF_8);
        }

        if (obj == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return obj;
        }
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal decimal = getBigDecimal(columnIndex);
        if (decimal != null) {
            try {
                return decimal.setScale(scale);
            } catch (ArithmeticException ex) {
                try {
                    return decimal.setScale(scale, BigDecimal.ROUND_HALF_UP);
                } catch (ArithmeticException arEx) {
                    throw GeneralUtil.nestedException(arEx);
                }
            }
        }

        return decimal;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        BigDecimal value = currentKVPair.getBigDecimal(getActualIndex(columnIndex));
        if (value == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return value;
        }

    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        Date date = getDate(columnIndex);

        if (date instanceof ZeroDate) {
            throw new SQLException("Value '0000-00-00' can not be represented as java.sql.Date");
        }

        if (date == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            cal.setTimeInMillis(date.getTime());
            return new Date(cal.getTimeInMillis());
        }
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public TResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        if (this.resultSetMetaData != null) {
            return this.resultSetMetaData;
        }

        resultSetMetaData = new TResultSetMetaData(resultCursor.getReturnColumns(), extraCmd);

        return resultSetMetaData;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Blob value = currentKVPair.getBlob(getActualIndex(columnIndex));
        if (value == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return value;
        }
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        validateColumnIndex(columnIndex);
        columnIndex--;
        Clob value = currentKVPair.getClob(getActualIndex(columnIndex));
        if (value == null) {
            wasNull = true;
            return null;
        } else {
            wasNull = false;
            return value;
        }
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        return;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        return;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    // ----------------------------- 未实现的类型 ------------------------

    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean first() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean last() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Statement getStatement() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new UnsupportedOperationException();

    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public ResultCursor getResultCursor() {
        return this.resultCursor;
    }

    public Map<String, Object> getExtraCmd() {
        return extraCmd;
    }
}
