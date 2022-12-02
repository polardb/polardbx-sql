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

package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.google.protobuf.ByteString;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorException;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import io.airlift.slice.Slice;

import java.sql.Blob;

public abstract class AbstractDataType<DATA> implements DataType<DATA> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractDataType.class);

    @Override
    public DATA convertFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        }

        if (value instanceof Slice) {
            // converting from slice, is equal to convert from a string with utf-8 encoding.
            value = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
        }

        if (value instanceof org.apache.calcite.avatica.util.ByteString) {
            value = ((org.apache.calcite.avatica.util.ByteString) value).toString();
        }

        if (value instanceof UInt64) {
            value = ((UInt64) value).toBigInteger();
        }

        if (value instanceof Blob) {
            com.alibaba.polardbx.optimizer.core.datatype.Blob blob =
                (com.alibaba.polardbx.optimizer.core.datatype.Blob) DataTypes.BlobType.convertFrom(value);
            value = blob.getSlice().toString(CharsetName.LATIN1.toJavaCharset());
        }

        if (value instanceof EnumValue) {
            return (DATA) ((EnumValue) value).type.convertTo(this, (EnumValue) value);
        }

        Class clazz = value.getClass();
        if (value instanceof ZeroDate) {
            clazz = java.sql.Date.class;
        } else if (value instanceof ZeroTimestamp) {
            clazz = java.sql.Timestamp.class;
        } else if (value instanceof ZeroTime) {
            clazz = java.sql.Time.class;
        } else if (value instanceof BigIntegerType) {
            clazz = Long.class;
        }
        if (this.getClass() == clazz) {
            //In same class, no need to convert
            return (DATA) value;
        }
        Convertor convertor = null;
        try {
            convertor = getConvertor(clazz);

            if (convertor != null) {
                // 没有convertor代表类型和目标相同，不需要做转化
                return (DATA) convertor.convert(value, getDataClass());
            } else {
                return (DATA) value;
            }
        } catch (Exception e) {
            logger.error("Failed to convert " + value.getClass() + " to " + this.getDataClass() + ": "
                + e.getMessage());
            throw e;
        }
    }

    @Override
    public Object convertJavaFrom(Object value) {
        return convertFrom(value);
    }

    @Override
    public Class getJavaClass() {
        return getDataClass();
    }

    /**
     * 获取convertor接口
     */
    protected Convertor getConvertor(Class clazz) {
        if (clazz.equals(getDataClass())) {
            return null;
        }

        Convertor convertor = ConvertorHelper.getInstance().getConvertor(clazz, getDataClass());

        if (convertor == null) {
            throw new ConvertorException(
                "Unsupported convert: [" + clazz.getName() + "," + getDataClass().getName() + "]");
        } else {
            return convertor;
        }
    }

    @Override
    public boolean isUnsigned() {
        return false;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public boolean equalDeeply(DataType that) {
        if (that == null || that.getClass() != this.getClass()) {
            return false;
        }
        return true;
    }

}
