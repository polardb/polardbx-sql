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
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.charset.CharsetFactory;
import com.alibaba.polardbx.common.collation.CollationHandler;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import io.airlift.slice.Slice;

import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * {@linkplain Blob}类型，使用BytesType来替代
 *
 * @author jianghang 2014-1-21 下午5:42:45
 * @since 5.0.0
 */
public class BlobType extends AbstractDataType<Blob> {
    protected final static CollationHandler BINARY_COLLATION =
        CharsetFactory.INSTANCE.createCharsetHandler(CharsetName.BINARY).getCollationHandler();

    @Override
    public Class getDataClass() {
        return Blob.class;
    }

    @Override
    public Blob getMaxValue() {
        throw new NotSupportException("range query is not supported for blob type");
    }

    @Override
    public Blob getMinValue() {
        throw new NotSupportException("range query is not supported for blob type");

    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("blob类型不支持计算操作");
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getBlob(index);
            }

            @Override
            public Object get(Row rs, int index) {
                Object val = rs.getObject(index);
                return convertFrom(val);
            }
        };
    }

    @Override
    public int compare(Object o1, Object o2) {
        if (o1 == o2) {
            return 0;
        }

        com.alibaba.polardbx.optimizer.core.datatype.Blob value =
            (com.alibaba.polardbx.optimizer.core.datatype.Blob) convertFrom(o1);
        com.alibaba.polardbx.optimizer.core.datatype.Blob targetValue =
            (com.alibaba.polardbx.optimizer.core.datatype.Blob) convertFrom(o2);

        if (value == null) {
            return -1;
        }

        if (targetValue == null) {
            return 1;
        }

        Slice left = value.getSlice();
        Slice right = targetValue.getSlice();

        return BINARY_COLLATION.compareSp(left, right);
    }

    public int hashcode(Object o) {
        com.alibaba.polardbx.optimizer.core.datatype.Blob value =
            (com.alibaba.polardbx.optimizer.core.datatype.Blob) convertFrom(o);

        if (value == null) {
            return 0;
        }

        return BINARY_COLLATION.hashcode(value.getSlice());
    }

    @Override
    public Blob convertFrom(Object value) {
        if (value == null || value instanceof NullValue) {
            return null;
        } else {
            if (value instanceof Blob) {
                if (value instanceof com.alibaba.polardbx.optimizer.core.datatype.Blob) {
                    return (Blob) value;
                } else {
                    try {
                        // If we get non-internal blob implementation, copy memory and recreate blob object.
                        long length = ((Blob) value).length();
                        byte[] rawBytes = ((Blob) value).getBytes(1, (int) length);
                        return new com.alibaba.polardbx.optimizer.core.datatype.Blob(rawBytes);
                    } catch (SQLException e) {
                        GeneralUtil.nestedException(e);
                    }
                }
            }

            // Maybe we should convert object in all type to byte array so we can get a blob object.
            byte[] bytes = DataTypes.BinaryType.convertFrom(value);
            return new com.alibaba.polardbx.optimizer.core.datatype.Blob(bytes);
        }
    }

    @Override
    public int getSqlType() {
        return Types.BINARY;
    }

    @Override
    public String getStringSqlType() {
        return "BLOB";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_BLOB;
    }
}
