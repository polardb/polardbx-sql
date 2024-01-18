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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class OssLoadDataRow implements Row {

    Map<Integer, ParameterContext> params;

    public OssLoadDataRow(Map<Integer, ParameterContext> params) {
        this.params = params;
    }

    @Override
    public Object getObject(int index) {
        return params.get(index).getValue();
    }

    @Override
    public void setObject(int index, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Integer getInteger(int index) {
        return Integer.parseInt(params.get(index).getValue().toString());
    }

    @Override
    public Long getLong(int index) {
        return Long.parseLong(params.get(index).toString());
    }

    @Override
    public List<Object> getValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<byte[]> getBytes() {
        return null;
    }

    @Override
    public String getString(int index) {
        return params.get(index).getValue().toString();
    }

    @Override
    public Boolean getBoolean(int index) {
        return Boolean.parseBoolean(params.get(index).getValue().toString());
    }

    @Override
    public Short getShort(int index) {
        return Short.parseShort(params.get(index).getValue().toString());
    }

    @Override
    public Float getFloat(int index) {
        return Float.parseFloat(params.get(index).getValue().toString());
    }

    @Override
    public Double getDouble(int index) {
        return Double.parseDouble(params.get(index).getValue().toString());
    }

    @Override
    public byte[] getBytes(int index) {
        return params.get(index).getValue().toString().getBytes();
    }

    @Override
    public Byte getByte(int index) {
        return Byte.parseByte(params.get(index).getValue().toString());
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return BigDecimal.valueOf(getDouble(index));
    }

    @Override
    public Time getTime(int index) {
        return Time.valueOf(params.get(index).getValue().toString());
    }

    @Override
    public Date getDate(int index) {
        return Date.valueOf(params.get(index).getValue().toString());
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return Timestamp.valueOf(params.get(index).getValue().toString());
    }

    @Override
    public Blob getBlob(int index) {
        return (Blob) params.get(index).getValue();
    }

    @Override
    public Clob getClob(int index) {
        return (Clob) params.get(index).getValue();
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        return new byte[0];
    }

    @Override
    public byte[] getBytes(DataType dataType, int index, String encoding) {
        return new byte[0];
    }

    @Override
    public CursorMeta getParentCursorMeta() {
        return null;
    }

    @Override
    public void setCursorMeta(CursorMeta cursorMeta) {

    }

    @Override
    public int getColNum() {
        return 0;
    }

    @Override
    public long estimateSize() {
        return 0;
    }
}
