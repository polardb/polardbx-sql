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

import com.alibaba.polardbx.common.jdbc.ZeroDate;
import com.alibaba.polardbx.common.jdbc.ZeroTime;
import com.alibaba.polardbx.common.jdbc.ZeroTimestamp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;


public class LongAndDateConvertor {

    public static class LongToDateConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (Long.class.isInstance(src)) {
                Long time = (Long) src;

                if (destClass.equals(java.util.Date.class) || destClass.equals(ZeroDate.class)) {
                    if (time == 0l) {
                        return ZeroDate.instance;
                    }
                    return new java.util.Date(time);
                }


                if (destClass.equals(java.sql.Date.class) || destClass.equals(ZeroDate.class)) {
                    if (time == 0l) {
                        return ZeroDate.instance;
                    }
                    return new java.sql.Date(time);
                }

                if (destClass.equals(java.sql.Time.class) || destClass.equals(ZeroTime.class)) {
                    if (time == 0l) {
                        return ZeroTime.instance;
                    }
                    return new java.sql.Time(time);
                }

                if (destClass.equals(java.sql.Timestamp.class) || destClass.equals(ZeroTimestamp.class)) {
                    if (time == 0l) {
                        return ZeroTimestamp.instance;
                    }
                    try {
                        String timeStr = String.valueOf(time);
                        if (timeStr.length() > 14) {
                            return null;
                        }
                        java.util.Date date = new SimpleDateFormat("yyyyMMddHHmmss").parse(timeStr);
                        return new java.sql.Timestamp(date.getTime());
                    } catch (ParseException ignore) {
                    }
                    return new java.sql.Timestamp(time);
                }

            }
            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

    public static class DateToLongConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {

            if (Date.class.isInstance(src)) {
                Date date = (Date) src;
                return date.getTime();
            }

            if (src instanceof LocalDateTime) {
                LocalDateTime localDateTime = (LocalDateTime) src;
                return java.sql.Timestamp.valueOf(
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
                        .format(localDateTime)).getTime();
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }

}
