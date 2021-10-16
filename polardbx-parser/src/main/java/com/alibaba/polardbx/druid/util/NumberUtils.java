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

package com.alibaba.polardbx.druid.util;

import static com.alibaba.polardbx.druid.util.MySqlUtils.ASCII;
import static com.alibaba.polardbx.druid.util.MySqlUtils.UTF8;

public class NumberUtils {
    public static float parseFloat(byte[] str, final int off, int len) {
        if (str == null) {
            throw new NumberFormatException();
        }

        if (len == 0 || off + len > str.length) {
            throw new NumberFormatException(new String(str, UTF8));
        }

        final int end = off + len - 1;
        int i = off;

        byte ch = str[i];
        boolean negative = ch == '-';
        if (ch == '-') {
            negative = true;
            ch = str[++i];
        } else if (ch == '+') {
            ch = str[++i];
        }

        long intVal;
        if (ch == '.') {
            intVal = 0;
        } else if (ch < '0' || ch > '9') {
            throw new NumberFormatException(new String(str, off, len, UTF8));
        } else {
            intVal = ch - '0';
            for (; i < end;) {
                ch = str[++i];
                if (ch >= '0' && ch <= '9') {
                    intVal = intVal * 10 + (ch - '0');
                    continue;
                } else {
                    break;
                }
            }
        }

        boolean exp = false;
        long power = 1;
        boolean small = (ch == '.');
        if (small) {
            if (i < end) {
                ch = str[++i];
                if (ch >= '0' && ch <= '9') {
                    intVal = intVal * 10 + (ch - '0');
                    power = 10;
                    for (; i < end; ) {
                        ch = str[++i];
                        if (ch >= '0' && ch <= '9') {
                            intVal = intVal * 10 + (ch - '0');
                            power *= 10;
                            continue;
                        } else {
                            break;
                        }
                    }
                } else {
                    throw new NumberFormatException(new String(str, off, len, UTF8));
                }
            } else if (len == 1) {
                throw new NumberFormatException(new String(str, off, len, UTF8));
            }
        } else if(ch == 'e' || ch == 'E') {
            exp = true;
        } else if (ch < '0' || ch > '9') {
            throw new NumberFormatException(new String(str, off, len, UTF8));
        }

        double value;
        if ((!exp) && len < 17 && i == end) {
            value = ((double) intVal) / power;
            if (negative) {
                value = -value;
            }
        } else {
            String text = new String(str, off, len, ASCII);
            value = Double.parseDouble(text);
        }

        return (float) value;
    }

    public static double parseDouble(byte[] str, final int off, int len) {
        if (str == null) {
            throw new NumberFormatException();
        }

        if (len == 0 || off + len > str.length) {
            throw new NumberFormatException(new String(str, UTF8));
        }

        final int end = off + len - 1;
        int i = off;

        byte ch = str[i];
        boolean negative = ch == '-';
        if (ch == '-') {
            negative = true;
            ch = str[++i];
        } else if (ch == '+') {
            ch = str[++i];
        }

        long intVal;
        if (ch == '.') {
            intVal = 0;
        } else if (ch < '0' || ch > '9') {
            throw new NumberFormatException(new String(str, off, len, UTF8));
        } else {
            intVal = ch - '0';
            for (; i < end;) {
                ch = str[++i];
                if (ch >= '0' && ch <= '9') {
                    intVal = intVal * 10 + (ch - '0');
                    continue;
                } else {
                    break;
                }
            }
        }

        boolean exp = false;
        long power = 1;
        boolean small = (ch == '.');
        if (small) {
            if (i < end) {
                ch = str[++i];
                if (ch >= '0' && ch <= '9') {
                    intVal = intVal * 10 + (ch - '0');
                    power = 10;
                    for (; i < end; ) {
                        ch = str[++i];
                        if (ch >= '0' && ch <= '9') {
                            intVal = intVal * 10 + (ch - '0');
                            power *= 10;
                            continue;
                        } else {
                            break;
                        }
                    }
                } else {
                    throw new NumberFormatException(new String(str, off, len, UTF8));
                }
            } else if (len == 1) {
                throw new NumberFormatException(new String(str, off, len, UTF8));
            }
        } else if(ch == 'e' || ch == 'E') {
            exp = true;
        } else if (ch < '0' || ch > '9') {
            throw new NumberFormatException(new String(str, off, len, UTF8));
        }

        double value;
        if ((!exp) && len < 17 && i == end) {
            value = ((double) intVal) / power;
            if (negative) {
                value = -value;
            }
        } else {
            String text = new String(str, off, len, ASCII);
            value = Double.parseDouble(text);
        }

        return value;
    }
}
