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

package com.alibaba.polardbx.common.model.sqljep;

import com.alibaba.polardbx.common.exception.NotSupportException;
import org.apache.commons.lang.time.DateFormatUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Date;

public class Comparative implements Comparable, Cloneable {

    public static final int GreaterThan = 1;
    public static final int GreaterThanOrEqual = 2;
    public static final int Equivalent = 3;
    public static final int NotEquivalent = 4;
    public static final int LessThan = 5;
    public static final int LessThanOrEqual = 6;

    private Object value;
    private int comparison;
    // raw index != -1 meaning this comparative is refered to one IN expr.
    private int rawIndex = -1;
    private int skIndex = -1;

    protected Comparative() {
    }

    public Comparative(int function, Object value) {
        this.comparison = function;
        this.value = value;
    }

    public static int reverseComparison(int function) {
        return 7 - function;
    }

    public static int exchangeComparison(int function) {
        if (function == GreaterThan) {
            return LessThan;
        } else if (function == GreaterThanOrEqual) {
            return LessThanOrEqual;
        } else if (function == LessThan) {
            return GreaterThan;
        }

        if (function == LessThanOrEqual) {
            return GreaterThanOrEqual;
        } else {
            return function;
        }
    }

    public Object getValue() {
        return value;
    }

    public void setComparison(int function) {
        this.comparison = function;
    }

    public static String getComparisonName(int function) {
        if (function == Equivalent) {
            return "=";
        } else if (function == GreaterThan) {
            return ">";
        } else if (function == GreaterThanOrEqual) {
            return ">=";
        } else if (function == LessThanOrEqual) {
            return "<=";
        } else if (function == LessThan) {
            return "<";
        } else if (function == NotEquivalent) {
            return "<>";
        } else {
            return null;
        }
    }

    public static int getComparisonByCompleteString(String completeStr) {
        if (completeStr != null) {
            String ident = completeStr.toLowerCase();
            if (ident.contains(">=")) {
                return GreaterThanOrEqual;
            } else if (ident.contains("<=")) {
                return LessThanOrEqual;
            } else if (ident.contains("!=")) {
                return NotEquivalent;
            } else if (ident.contains("<>")) {
                return NotEquivalent;
            } else if (ident.contains("=")) {
                return Equivalent;
            } else if (ident.contains(">")) {
                return GreaterThan;
            } else if (ident.contains("<")) {
                return LessThan;
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    public int getComparison() {
        return comparison;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public int compareTo(Object o) {
        if (o instanceof Comparative) {
            Comparative other = (Comparative) o;
            if (this.getValue() instanceof Comparable && other.getValue() instanceof Comparable) {
                return ((Comparable) this.getValue()).compareTo(other.getValue());
            } else {
                throw new NotSupportException("not comparable");
            }
        } else if (o instanceof Comparable) {
            if (this.getValue() instanceof Comparable) {
                return ((Comparable) this.getValue()).compareTo(o);
            } else {
                throw new NotSupportException("not comparable");
            }
        }

        return -1;
    }

    public String toString() {
        if (value != null) {
            StringBuilder sb = new StringBuilder();
            sb.append("(").append(getComparisonName(comparison));
            sb.append(value.toString()).append(")");
            return sb.toString();
        } else {
            return null;
        }
    }

    public Comparative clone() {
        Comparative c = new Comparative(this.comparison, this.value);
        c.setRawIndex(rawIndex, skIndex);
        return c;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + comparison;
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Comparative other = (Comparative) obj;
        if (comparison != other.comparison) {
            return false;
        }
        if (value == null) {
            if (other.value != null) {
                return false;
            }
        } else if (!value.equals(other.value)) {
            return false;
        }

        return true;
    }

    public int compareToIgnoreType(String value) {
        if (value == null) {
            return -2;
        }
        try {
            final Class<?> aClass = this.getValue().getClass();
            if (aClass == Long.class) {
                final Long aLong = Long.valueOf(value);
                if (aLong.equals(getValue())) {
                    return 0;
                } else if (aLong > (Long) getValue()) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (aClass == Integer.class) {
                final Integer aInt = Integer.valueOf(value);
                if (aInt.equals(getValue())) {
                    return 0;
                } else if (aInt > (Integer) getValue()) {
                    return 1;
                } else {
                    return -1;
                }
            } else if (aClass == BigInteger.class) {
                return new BigInteger(value).compareTo((BigInteger) getValue());
            } else if (aClass == BigDecimal.class) {
                return new BigDecimal(value).compareTo((BigDecimal) getValue());
            } else if (aClass == Timestamp.class) {
                String formatString = "yyyy-MM-dd";
                if (value.length() == 4) {
                    formatString = "yyyy";
                } else if (value.length() == 7) {
                    formatString = "yyyy-MM";
                }
                String format = DateFormatUtils.format((Date) getValue(), formatString);
                return value.compareTo(format);
            } else if (aClass == String.class) {
                if (value.equals(getValue())) {
                    return 0;
                } else {
                    throw new RuntimeException("not supported type[1]:" + aClass.getName());
                }
            } else {
                throw new RuntimeException("not supported type[2]:" + aClass.getName());
            }
        } catch (Exception e) {
            return -3;
        }

    }

    public int getRawIndex() {
        return rawIndex;
    }

    public void setRawIndex(int rawIndex, int skIndex) {
        this.rawIndex = rawIndex;
        this.skIndex = skIndex;
    }

    public int getSkIndex() {
        return skIndex;
    }

    public void childrenAccept(ComparativeVisitor visitor) {
    }
}
