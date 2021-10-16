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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import org.apache.commons.lang.BooleanUtils;

import java.io.Closeable;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class GeneralUtil {

    static Pattern pattern = Pattern.compile("\\d+$");

    public static boolean isEmpty(Map map) {
        return null == map || map.isEmpty();
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isEmpty(Object[] collection) {
        return collection == null || collection.length == 0;
    }

    public static boolean isNotEmpty(Collection collection) {
        return collection != null && !collection.isEmpty();
    }

    public static <E> Collection<E> emptyIfNull(Collection<E> coll) {
        return coll == null ? Collections.emptyList() : coll;
    }

    public static String getTab(int count) {
        StringBuffer tab = new StringBuffer();
        for (int i = 0; i < count; i++) {
            tab.append("    ");
        }
        return tab.toString();
    }

    public static String getPropertyString(Map<String, Object> extraCmd, String key) {
        if (extraCmd == null) {
            return null;
        }

        if (key == null) {
            return null;
        }
        Object obj = extraCmd.get(key);
        if (obj != null) {
            return obj.toString().trim();
        } else {
            return null;
        }
    }

    public static String getPropertyString(Map<String, Object> extraCmd, String key, String defaultValue) {
        String value = getPropertyString(extraCmd, key);

        if (value == null) {
            value = defaultValue;
        }

        return value;

    }

    public static boolean getPropertyBoolean(Map<String, Object> extraCmd, String key, boolean defaultValue) {
        String value = getPropertyString(extraCmd, key);
        if (value == null) {
            return defaultValue;
        } else {
            return BooleanUtils.toBoolean(value);
        }
    }

    public static long getPropertyLong(Map<String, Object> extraCmd, String key, long defaultValue) {
        String value = getPropertyString(extraCmd, key);
        if (value == null) {
            return defaultValue;
        } else {
            return Long.valueOf(value);
        }
    }

    public static void checkInterrupted() {
        if (Thread.interrupted()) {
            throw GeneralUtil.nestedException(new InterruptedException());
        }
    }

    public static void printlnToStringBuilder(StringBuilder sb, String v) {
        sb.append(v).append("\n");
    }

    public static void printAFieldToStringBuilder(StringBuilder sb, String field, Object v, String inden) {
        if (v == null || v.toString().equals("") || v.toString().equals("[]")) {
            return;
        }

        printlnToStringBuilder(sb, inden + field + ":" + v);
    }

    public static StackTraceElement split = new StackTraceElement("------- one sql exceptions-----", "", "", 0);

    public static RuntimeException mergeException(List<Throwable> exceptions) {

        Throwable first = exceptions.get(0);
        List<StackTraceElement> stes = new ArrayList<StackTraceElement>(30 * exceptions.size());

        boolean hasSplit = false;
        for (StackTraceElement ste : first.getStackTrace()) {
            stes.add(ste);
            if (ste == split) {
                hasSplit = true;
            }
        }
        if (!hasSplit) {
            stes.add(split);
        }
        Throwable current = first;
        for (int i = 1, n = exceptions.size(); i < n; i++) {

            current = exceptions.get(i);

            hasSplit = false;
            for (StackTraceElement ste : current.getStackTrace()) {
                stes.add(ste);
                if (ste == split) {
                    hasSplit = true;
                }
            }
            if (!hasSplit) {
                stes.add(split);
            }
        }

        first.setStackTrace(stes.toArray(new StackTraceElement[stes.size()]));
        return new TddlNestableRuntimeException(first);
    }

    public static InputStream getInputStream(String fileName) {
        if (fileName.charAt(0) == '/') {
            fileName = fileName.substring(1);
        }

        ClassLoader classLoader = GeneralUtil.class.getClassLoader();
        InputStream stream = null;
        if (classLoader != null) {
            stream = classLoader.getResourceAsStream(fileName);
        }

        if (stream == null) {
            stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        }

        return stream;
    }

    public static boolean isNotEmpty(Map collection) {
        return collection != null && collection.size() != 0;

    }

    public static String printBytes(byte[] bs) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        sb.append("(");
        for (byte b : bs) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }

            sb.append("0x").append(Integer.toHexString(b & 0xff));
        }

        sb.append(")");
        return sb.toString();
    }

    public static RuntimeException nestedException(Throwable e) {
        if (e instanceof TddlRuntimeException) {
            return (TddlRuntimeException) e;
        }

        if (e instanceof TddlNestableRuntimeException) {
            return (TddlNestableRuntimeException) e;
        }

        return new TddlNestableRuntimeException(e);
    }

    public static RuntimeException nestedException(String msg, Throwable cause) {
        throw new TddlNestableRuntimeException(msg, cause);
    }

    public static RuntimeException nestedException(String msg) {
        throw new TddlNestableRuntimeException(msg);
    }

    public static void check(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw nestedException(message.get());
        }
    }

    public static boolean sameSize(List<?> expected, List<?> actual) {
        if (GeneralUtil.isEmpty(expected) && GeneralUtil.isEmpty(actual)) {
            return true;
        }

        if (GeneralUtil.isEmpty(expected) || GeneralUtil.isEmpty(actual)) {
            return false;
        }

        if (expected.size() != actual.size()) {
            return false;
        }

        return true;
    }

    public static long unixTimeStamp() {
        return System.currentTimeMillis() / 1000;
    }

    public static boolean shouldRunAtThatTime(long now, String startTime, String endTime, Timestamp nextRunningTime) {
        String[] startTimeSplit = startTime.split(":");
        String[] endTimeSplit = endTime.split(":");
        assert startTimeSplit.length == 2;
        assert endTimeSplit.length == 2;

        int startTimeHour = Integer.valueOf(startTimeSplit[0]);
        int startTimeMin = Integer.valueOf(startTimeSplit[1]);
        int endTimeHour = Integer.valueOf(endTimeSplit[0]);
        int endTimeMin = Integer.valueOf(endTimeSplit[1]);

        assert startTimeHour >= 0 && startTimeHour < 24;
        assert startTimeMin >= 0 && startTimeMin < 60;
        assert endTimeHour >= 0 && endTimeHour < 24;
        assert endTimeMin >= 0 && endTimeMin < 60;

        long startTimeInMs = (startTimeHour * 60 + startTimeMin) * 60 * 1000;
        long endTimeInMs = (endTimeHour * 60 + endTimeMin) * 60 * 1000;

        long todayMs = now - GeneralUtil.startOfToday(now);
        if (endTimeInMs >= startTimeInMs) {
            if (todayMs > startTimeInMs && todayMs < endTimeInMs) {
                nextRunningTime.setTime(now);
                return true;
            } else {
                nextRunningTime.setTime(now - todayMs + startTimeInMs);
                return false;
            }
        } else {
            if (todayMs > startTimeInMs || todayMs < endTimeInMs) {
                nextRunningTime.setTime(now);
                return true;
            } else {
                nextRunningTime.setTime(now - todayMs + startTimeInMs);
                return false;
            }
        }
    }

    public static long startOfToday(long now) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(now);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }

    public static <T> T coalesce(T... objects) {
        for (T o : objects) {
            if (o != null) {
                return o;
            }
        }
        return null;
    }

    public static void close(Connection x) {
        if (x == null) {
            return;
        }
        try {
            x.close();
        } catch (Exception e) {
            throw nestedException(e);
        }
    }

    public static void close(Statement x) {
        if (x == null) {
            return;
        }
        try {
            x.close();
        } catch (Exception e) {
            throw nestedException(e);
        }
    }

    public static void close(ResultSet x) {
        if (x == null) {
            return;
        }
        try {
            x.close();
        } catch (Exception e) {
            throw nestedException(e);
        }
    }

    public static void close(Closeable x) {
        if (x == null) {
            return;
        }

        try {
            x.close();
        } catch (Exception e) {
            throw nestedException(e);
        }
    }
}
