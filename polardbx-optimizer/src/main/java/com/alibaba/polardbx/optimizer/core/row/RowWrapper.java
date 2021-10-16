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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

import com.alibaba.polardbx.optimizer.core.CursorMeta;

/**
 * 可以用来给列改名，去除一个列
 *
 * @author mengshi.sunmengshi 2013-12-3 上午11:05:57
 * @since 5.0.0
 */
public class RowWrapper extends AbstractRow implements Row {

    protected final CursorMeta newCursorMeta;
    protected Row parentRow;

    public RowWrapper(CursorMeta cursorMeta, Row row) {
        super(cursorMeta);
        this.newCursorMeta = cursorMeta;
        this.parentRow = row;
    }

    @Override
    public Object getObject(int index) {
        return parentRow.getObject(index);
    }

    @Override
    public void setObject(int index, Object value) {
        parentRow.setObject(index, value);
    }

    @Override
    public Blob getBlob(int index) {
        return parentRow.getBlob(index);
    }

    @Override
    public Clob getClob(int index) {

        return parentRow.getClob(index);
    }

    @Override
    public Integer getInteger(int index) {
        return parentRow.getInteger(index);
    }

    @Override
    public Long getLong(int index) {
        return parentRow.getLong(index);
    }

    @Override
    public List<Object> getValues() {
        return parentRow.getValues();
    }

    @Override
    public CursorMeta getParentCursorMeta() {
        return newCursorMeta;
    }

    @Override
    public String getString(int index) {
        return parentRow.getString(index);
    }

    @Override
    public Boolean getBoolean(int index) {
        return parentRow.getBoolean(index);
    }

    @Override
    public Short getShort(int index) {
        return parentRow.getShort(index);
    }

    @Override
    public Float getFloat(int index) {
        return parentRow.getFloat(index);
    }

    @Override
    public Double getDouble(int index) {
        return parentRow.getDouble(index);
    }

    @Override
    public byte[] getBytes(int index) {
        return parentRow.getBytes(index);
    }

    @Override
    public byte[] getBytes(int index, String encoding) {
        return parentRow.getBytes(index, encoding);
    }

    @Override
    public Date getDate(int index) {
        return parentRow.getDate(index);
    }

    @Override
    public Timestamp getTimestamp(int index) {
        return parentRow.getTimestamp(index);
    }

    @Override
    public Time getTime(int index) {
        return parentRow.getTime(index);
    }

    @Override
    public BigDecimal getBigDecimal(int index) {
        return parentRow.getBigDecimal(index);
    }

    @Override
    public List<byte[]> getBytes() {
        return parentRow.getBytes();
    }

    @Override
    public Byte getByte(int index) {
        return parentRow.getByte(index);
    }

    public Row getParentRowSet() {
        return parentRow;
    }

    public void setParentRowSet(Row parentRow) {
        this.parentRow = parentRow;
    }

    @Override
    public long estimateSize() {
        return parentRow.estimateSize();
    }
}
