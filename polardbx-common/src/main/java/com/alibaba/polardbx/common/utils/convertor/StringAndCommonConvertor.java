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

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;


public class StringAndCommonConvertor {


    public static class StringToCommon extends AbastactConvertor {

        protected static final Set TRUE_STRINGS = new HashSet<>(Arrays.asList("true", "on", "yes", "y"));
        protected static final Set FALSE_STRINGS =
            new HashSet<>(Arrays.asList("false", "null", "nul", "nil", "off", "no", "n"));

        protected Boolean booleanConvert(Object value) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }

            if (value instanceof Number) {
                return (Math.abs(((Number) value).doubleValue()) < Float.MIN_VALUE) ? Boolean.FALSE : Boolean.TRUE;
            }

            if (value instanceof String) {
                String strValue = ((String) value).trim();
                try {
                    return (Integer.parseInt(strValue) == 0) ? Boolean.FALSE : Boolean.TRUE;
                } catch (NumberFormatException e) {
                    strValue = strValue.toLowerCase();

                    if (TRUE_STRINGS.contains(strValue)) {
                        return Boolean.TRUE;
                    }

                    if (FALSE_STRINGS.contains(strValue)) {
                        return Boolean.FALSE;
                    }

                }
            }

            throw new ConvertorException("Unsupported convert: [" + String.class + "," + Boolean.class.getName() + "]");
        }

        protected Character charConvert(Object value) {
            if (value instanceof Character) {
                return (Character) value;
            }

            if (value instanceof Number) {
                return new Character((char) ((Number) value).intValue());
            }

            if (value instanceof String) {
                String strValue = ((String) value).trim();

                try {
                    return new Character((char) Integer.parseInt(strValue));
                } catch (NumberFormatException e) {
                }
            }

            throw new ConvertorException("Unsupported convert: [" + String.class + "," + Character.class.getName()
                + "]");
        }

        @Override
        public Object convert(Object src, Class destClass) {
            if (String.class.isInstance(src)) {
                String str = (String) src;
                if (destClass == Double.class || destClass == double.class) {
                    return Double.valueOf(str);
                }

                if (destClass == Float.class || destClass == float.class) {
                    return Float.valueOf(str);
                }

                if (destClass == Boolean.class || destClass == boolean.class) {
                    return booleanConvert(str);
                }

                if (destClass == Integer.class || destClass == int.class) {
                    return Integer.valueOf(str);
                }

                if (destClass == Short.class || destClass == short.class) {
                    return Short.valueOf(str);
                }

                if (destClass == Long.class || destClass == long.class) {
                    return Long.valueOf(str);
                }

                if (destClass == Byte.class || destClass == byte.class) {
                    return Byte.valueOf(str);
                }

                if (destClass == Character.class || destClass == char.class) {
                    return charConvert(str);
                }

                if (destClass == Decimal.class) {
                    return Decimal.fromString(str);
                }

                if (destClass == BigInteger.class) {
                    return new BigInteger(str);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

}
