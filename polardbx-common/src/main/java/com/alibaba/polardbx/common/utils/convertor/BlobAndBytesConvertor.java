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

package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.datatype.Decimal;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.SQLException;


public class BlobAndBytesConvertor {

    public static final BigDecimal BIGINT_MAX_VALUE = new BigDecimal("18446744073709551615");

    public static class BlobToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Blob.class.isInstance(src) && destClass.equals(byte[].class)) {
                if (src == null) {
                    return null;
                } else {
                    try {
                        Blob blob = (Blob) src;
                        return blob.getBytes(1, (int) blob.length());
                    } catch (SQLException e) {
                        throw new ConvertorException(e);
                    }
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                + destClass.getName() + "]");
        }
    }

    public static class NumberToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Number.class.isInstance(src) && destClass.equals(byte[].class)) {
                if (src == null) {
                    return null;
                } else {
                    return String.valueOf((Number) src).getBytes();
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                + destClass.getName() + "]");
        }
    }

    public static class BitBytesToNumber extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (src.getClass().equals(byte[].class) && Number.class.isAssignableFrom(destClass)) {
                BigDecimal result = null;

                try {

                    String value = new String((byte[]) src);
                    result = new BigDecimal(value);
                } catch (NumberFormatException e) {

                    byte[] asBytes = (byte[]) src;
                    int shift = 0;
                    long[] steps = new long[asBytes.length];
                    for (int i = asBytes.length - 1; i >= 0; i--) {
                        steps[i] = (long) (asBytes[i] & 0xff) << shift;
                        shift += 8;
                    }

                    long valueAsLong = 0;
                    for (int i = 0; i < asBytes.length; i++) {
                        valueAsLong |= steps[i];
                    }

                    result = (valueAsLong >= 0) ? BigDecimal.valueOf(valueAsLong) :
                        BIGINT_MAX_VALUE.add(BigDecimal.valueOf(1 + valueAsLong));
                }

                return ConvertorHelper.commonToCommon.convert(Decimal.fromBigDecimal(result), destClass);
            } else if (Number.class.isInstance(src) && Number.class.isAssignableFrom(destClass)) {
                return ConvertorHelper.commonToCommon.convert(src, destClass);
            } else if (Boolean.class.isInstance(src) && Number.class.isAssignableFrom(destClass)) {
                Integer bool = (Boolean) src ? 1 : 0;
                return ConvertorHelper.commonToCommon.convert(bool, destClass);
            }

            throw new ConvertorException("Unsupported convert: [" + src.getClass().getName() + ","
                + destClass.getName() + "]");
        }

    }

}
