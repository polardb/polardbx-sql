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

import com.alibaba.polardbx.common.charset.CharsetName;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;


public class StringAndObjectConvertor {

    public static class ObjectToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            return src != null ? src.toString() : null;
        }
    }

    public static class StringToBytes extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src) && destClass.equals(byte[].class)) {
                return src != null ? ((String) src).getBytes() : null;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class BytesToString extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (byte[].class.isInstance(src) && destClass.equals(String.class)) {
                try {
                    return new String((byte[]) src, CharsetName.BINARY.toJavaCharset());
                } catch (Throwable e) {
                    throw new ConvertorException(e);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class BigDecimalToString extends AbastactConvertor {
        @Override
        public Object convert(Object src, Class destClass) {
            if (src instanceof BigDecimal) {
                return ((BigDecimal) src).toPlainString();
            }
            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class RealToString extends AbastactConvertor {
        @Override
        public Object convert(Object src, Class destClass) {
            if (src instanceof Float) {
                float x = (Float) src;

                if (x == (long) x) {
                    return Long.toString((long) x);
                } else {
                    return Float.toString(x);
                }
            } else if (src instanceof Double) {
                double x = (Double) src;
                if (x == (long) x) {
                    return Long.toString((long) x);
                } else {
                    return Double.toString(x);
                }
            } else {
                throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
            }
        }
    }
}
