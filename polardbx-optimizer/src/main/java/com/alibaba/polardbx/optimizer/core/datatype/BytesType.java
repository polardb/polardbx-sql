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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * bytes 类型
 *
 * @author mengshi.sunmengshi 2014年1月21日 下午5:16:00
 * @since 5.0.0
 */
public class BytesType extends AbstractDataType<byte[]> {

    @Override
    public Class getDataClass() {
        return byte[].class;
    }

    @Override
    public byte[] getMaxValue() {
        return new byte[] {Byte.MAX_VALUE};
    }

    @Override
    public byte[] getMinValue() {
        return new byte[] {Byte.MIN_VALUE};
    }

    @Override
    public Calculator getCalculator() {
        throw new NotSupportException("bytes类型不支持计算操作");
    }

    @Override
    public ResultGetter getResultGetter() {
        return new ResultGetter() {

            @Override
            public Object get(ResultSet rs, int index) throws SQLException {
                return rs.getBytes(index);
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
        if (o1 == null) {
            return -1;
        }

        if (o2 == null) {
            return 1;
        }

        byte[] value = convertFrom(o1);
        byte[] targetValue = convertFrom(o2);
        int notZeroOffset = 0;
        int targetNotZeroOffset = 0;
        for (notZeroOffset = 0; notZeroOffset < value.length; notZeroOffset++) {
            if (value[notZeroOffset] != 0) {
                break;
            }
        }

        for (targetNotZeroOffset = 0; targetNotZeroOffset < targetValue.length; targetNotZeroOffset++) {
            if (targetValue[targetNotZeroOffset] != 0) {
                break;
            }
        }

        int actualLength = value.length - notZeroOffset;
        int actualTargetLength = targetValue.length - targetNotZeroOffset;

        int minLength = Math.min(actualLength, actualTargetLength);
        int index = notZeroOffset;
        int targetIndex = targetNotZeroOffset;

        while (index - notZeroOffset < minLength) {
            int code1 = Byte.toUnsignedInt(value[index]);
            int code2 = Byte.toUnsignedInt(targetValue[targetIndex]);

            int flag = code1 - code2;
            if (flag != 0) {
                return flag;
            }
            index++;
            targetIndex++;
        }

        return actualLength - actualTargetLength;
    }

    @Override
    public int getSqlType() {
        return java.sql.Types.BINARY;
    }

    @Override
    public String getStringSqlType() {
        return "BINARY";
    }

    @Override
    public MySQLStandardFieldType fieldType() {
        return MySQLStandardFieldType.MYSQL_TYPE_VAR_STRING;
    }

}
