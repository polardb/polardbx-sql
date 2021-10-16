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
import com.alibaba.polardbx.common.datatype.UInt64;
import com.alibaba.polardbx.common.utils.time.old.DateUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class CommonAndCommonConvertor {

    public static class CommonToCommon extends AbastactConvertor {

        private static final Integer ZERO = 0;
        private static final Integer ONE = 1;

        private long fromNumberToLongByRoundHalfUp(Number value) {
            if (value instanceof Float || value instanceof Double) {
                BigDecimal bd = new BigDecimal(value.toString());
                bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
                return bd.longValue();
            } else if (value instanceof Decimal) {
                BigDecimal bd = ((Decimal) value).toBigDecimal();
                bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
                return bd.longValue();
            } else {
                return value.longValue();
            }
        }

        private Object toCommon(Class srcClass, Class targetClass, Number value) {

            if (targetClass.equals(value.getClass())) {
                return value;
            }

            if (targetClass == Integer.class || targetClass == int.class) {
                long longValue = fromNumberToLongByRoundHalfUp(value);
                if (longValue > Integer.MAX_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too large for " + targetClass.getName());
                }
                if (longValue < Integer.MIN_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too small " + targetClass.getName());
                }
                return (int) longValue;
            }

            if (targetClass == Long.class || targetClass == long.class) {
                return fromNumberToLongByRoundHalfUp(value);
            }

            if (targetClass == Boolean.class || targetClass == boolean.class) {
                long longValue = fromNumberToLongByRoundHalfUp(value);
                return longValue > 0;
            }

            if (targetClass == Byte.class || targetClass == byte.class) {
                long longValue = fromNumberToLongByRoundHalfUp(value);
                if (longValue > Byte.MAX_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too large for " + targetClass.getName());
                }
                if (longValue < Byte.MIN_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too small " + targetClass.getName());
                }
                return (byte) longValue;
            }

            if (targetClass == Double.class || targetClass == double.class) {
                return value.doubleValue();
            }

            if (targetClass == Decimal.class) {
                if (value instanceof Float || value instanceof Double) {
                    return Decimal.fromString(value.toString());
                } else if (value instanceof BigInteger) {
                    return Decimal.fromBigDecimal(new BigDecimal((BigInteger) value));
                } else if (value instanceof UInt64) {
                    return ((UInt64) value).toDecimal();
                } else {
                    return Decimal.fromLong(value.longValue());
                }
            }

            if (targetClass == BigInteger.class) {
                if (value instanceof Decimal) {
                    BigDecimal bd = ((Decimal) value).toBigDecimal();
                    bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
                    return bd.toBigInteger();
                } else if (value instanceof Float || value instanceof Double) {
                    BigDecimal bd = new BigDecimal(value.toString());
                    bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
                    return bd.toBigInteger();
                } else {
                    return BigInteger.valueOf(value.longValue());
                }
            }

            if (targetClass == Short.class || targetClass == short.class) {
                long longValue = fromNumberToLongByRoundHalfUp(value);
                if (longValue > Short.MAX_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too large for " + targetClass.getName());
                }
                if (longValue < Short.MIN_VALUE) {
                    throw new ConvertorException(
                        srcClass.getName() + " value '" + value + "' is too small " + targetClass.getName());
                }
                return (short) longValue;
            }

            if (targetClass == Float.class || targetClass == float.class) {
                return value.floatValue();
            }

            if (targetClass == Character.class || targetClass == char.class) {
                long longValue = fromNumberToLongByRoundHalfUp(value);

                return (char) longValue;
            }

            throw new ConvertorException(
                "Unsupported convert: [" + srcClass.getName() + "," + targetClass.getName() + "]");
        }

        private Object toCommon(Class srcClass, Class targetClass, Decimal value) {
            if (targetClass == srcClass) {
                return value;
            }

            if (targetClass == Double.class || targetClass == double.class || targetClass == Float.class
                || targetClass == float.class) {

                return toCommon(srcClass, targetClass, value.doubleValue());
            }

            if (value.precision() - value.scale() > 100) {
                throw new ConvertorException("Decimal overflow: [" + value.toString() + "]");
            }

            if (targetClass == BigInteger.class) {
                return value.toBigDecimal().setScale(0, BigDecimal.ROUND_HALF_UP).toBigInteger();
            }

            BigDecimal bd = value.toBigDecimal();
            bd = bd.setScale(0, BigDecimal.ROUND_HALF_UP);
            return toCommon(srcClass, targetClass, bd.longValue());
        }

        private Object toCommon(Class srcClass, Class targetClass, BigDecimal value) {
            if (targetClass == srcClass) {
                return value;
            }

            if (targetClass == Double.class || targetClass == double.class || targetClass == Float.class
                || targetClass == float.class) {

                return toCommon(srcClass, targetClass, value.doubleValue());
            }

            if (targetClass == Decimal.class) {
                return Decimal.fromBigDecimal(value);
            }

            if (value.precision() - value.scale() > 100) {
                throw new ConvertorException("BigDecimal overflow: [" + value.toString() + "]");
            }

            if (targetClass == BigInteger.class) {
                return value.setScale(0, BigDecimal.ROUND_HALF_UP).toBigInteger();
            }

            BigDecimal rounded = value.setScale(0, BigDecimal.ROUND_HALF_UP);
            return toCommon(srcClass, targetClass, rounded.longValue());
        }

        private Object toCommon(Class srcClass, Class targetClass, BigInteger value) {
            if (targetClass == srcClass) {
                return value;
            }

            if (targetClass == Decimal.class) {
                return Decimal.fromBigDecimal(new BigDecimal(value));
            }

            return toCommon(srcClass, targetClass, value.longValue());
        }

        @Override
        public Object convert(Object src, Class targetClass) {
            Class srcClass = src.getClass();
            if (srcClass == Integer.class || srcClass == int.class) {
                return toCommon(srcClass, targetClass, (Integer) src);
            }
            if (srcClass == Long.class || srcClass == long.class) {
                return toCommon(srcClass, targetClass, (Long) src);
            }
            if (srcClass == Boolean.class || srcClass == boolean.class) {
                Boolean boolValue = (Boolean) src;
                return toCommon(srcClass, targetClass, boolValue ? ONE : ZERO);
            }
            if (srcClass == Byte.class || srcClass == byte.class) {
                return toCommon(Double.class, targetClass, (Byte) src);
            }
            if (srcClass == Byte[].class || srcClass == byte[].class) {
                return toCommon(BigInteger.class, targetClass, new BigInteger((byte[]) src));
            }
            if (srcClass == Double.class || srcClass == double.class) {
                return toCommon(srcClass, targetClass, (Double) src);
            }
            if (srcClass == Decimal.class) {
                return toCommon(srcClass, targetClass, (Decimal) src);
            }
            if (srcClass == BigInteger.class) {
                return toCommon(srcClass, targetClass, (BigInteger) src);
            }
            if (srcClass == Float.class || srcClass == float.class) {
                return toCommon(srcClass, targetClass, (Float) src);
            }
            if (srcClass == Short.class || srcClass == short.class) {
                return toCommon(srcClass, targetClass, (Short) src);
            }
            if (srcClass == Character.class || srcClass == char.class) {
                Character charvalue = (Character) src;
                return toCommon(srcClass, targetClass, Integer.valueOf(charvalue));
            }
            if (srcClass == Date.class) {
                String myDate = DateUtils.replaceAll(((Date) src).toString(), "-", "");
                return toCommon(srcClass, targetClass, Integer.parseInt(myDate));
            }
            if (srcClass == Time.class) {
                StringAndDateConvertor.SqlTimeToString sqlTimeToString = new StringAndDateConvertor.SqlTimeToString();
                String myTime = (String) sqlTimeToString.convert(src, Integer.class);

                myTime = DateUtils.replaceAll(myTime, ":", "");
                int nanoIndex = myTime.indexOf('.');
                if (nanoIndex != -1) {
                    myTime = myTime.substring(0, nanoIndex);
                }
                return toCommon(srcClass, targetClass, Integer.parseInt(myTime));
            }
            if (srcClass == Timestamp.class) {
                String myTimestamp = src.toString();
                myTimestamp = DateUtils.replaceAll(myTimestamp, "-", "");
                myTimestamp = DateUtils.replaceAll(myTimestamp, ":", "");
                myTimestamp = DateUtils.replaceAll(myTimestamp, " ", "");
                if (targetClass == BigDecimal.class) {

                    return new BigDecimal(myTimestamp);
                } else if (targetClass == Decimal.class) {
                    return Decimal.fromString(myTimestamp);
                } else {
                    int nanoIndex = myTimestamp.indexOf('.');
                    if (nanoIndex != -1) {
                        myTimestamp = myTimestamp.substring(0, nanoIndex);
                    }

                    return toCommon(srcClass, targetClass, Long.valueOf(myTimestamp));
                }
            }

            if (srcClass == BigDecimal.class) {
                return toCommon(srcClass, targetClass, (BigDecimal) src);
            }
            throw new ConvertorException("Unsupported convert: [" + src + "," + targetClass.getName() + "]");
        }
    }
}
