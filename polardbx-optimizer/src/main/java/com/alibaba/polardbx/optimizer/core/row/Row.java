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

import com.alibaba.polardbx.optimizer.core.CursorMeta;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public interface Row {

    Object getObject(int index);

    /**
     * Get the Object for comparison.
     * Suitable for sorting.
     */
    default Object getObjectForCmp(int index) {
        return getObject(index);
    }

    void setObject(int index, Object value);

    Integer getInteger(int index);

    Long getLong(int index);

    List<Object> getValues();

    List<byte[]> getBytes();

    String getString(int index);

    Boolean getBoolean(int index);

    Short getShort(int index);

    Float getFloat(int index);

    Double getDouble(int index);

    byte[] getBytes(int index);

    Byte getByte(int index);

    BigDecimal getBigDecimal(int index);

    Time getTime(int index);

    Date getDate(int index);

    Timestamp getTimestamp(int index);

    Blob getBlob(int index);

    Clob getClob(int index);

    /**
     * 特殊的接口,针对server需要根据用户指定的set names返回特定的编码bytes
     */
    byte[] getBytes(int index, String encoding);

    CursorMeta getParentCursorMeta();

    void setCursorMeta(CursorMeta cursorMeta);

    int getColNum();

    /**
     * Estimate the memory usage of this row
     */
    long estimateSize();
}
