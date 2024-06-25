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
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
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
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class GeneralUtil {

    private static String lsnErrorMessage = "Variable 'read_lsn' can't be set to the value of";
    private static String followDelayMessage =
        "The follow exists delay, please use 'show storage' command to check latency";

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

    public static <E> void addAllIfNotEmpty(Collection<E> from, Collection<E> to) {
        if (isNotEmpty(from)) {
            to.addAll(from);
        }
    }

    public static <K, V> void addAllIfNotEmpty(Map<K, V> from, Map<K, V> to) {
        if (isNotEmpty(from)) {
            to.putAll(from);
        }
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

    public static int getPropertyInt(Map<String, Object> extraCmd, String key, int defaultValue) {
        String value = getPropertyString(extraCmd, key);
        if (value == null) {
            return defaultValue;
        } else {
            return Integer.valueOf(value);
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

        if (e.getMessage() != null && e.getMessage().contains(lsnErrorMessage)) {
            return new TddlNestableRuntimeException(followDelayMessage);
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

    public static long stringToMs(String time) {
        String[] timeSplit = time.split(":");
        assert timeSplit.length == 2;
        int timeHour = Integer.parseInt(timeSplit[0]);
        int timeMin = Integer.parseInt(timeSplit[1]);
        assert timeHour >= 0 && timeHour < 24;
        assert timeMin >= 0 && timeMin < 60;
        return (timeHour * 60 + timeMin) * 60 * 1000;
    }

    public static boolean shouldRunAtThatTime(long now, String startTime, String endTime, Timestamp nextRunningTime) {
        long startTimeInMs = stringToMs(startTime);
        long endTimeInMs = stringToMs(endTime);

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

    public static int findStartOfStatement(String sql) {
        int statementStartPos = 0;

        if (startsWithIgnoreCaseAndWs(sql, "/*", 0)) {
            statementStartPos = sql.indexOf("*/");

            if (statementStartPos == -1) {
                statementStartPos = 0;
            } else {
                statementStartPos += 2;
            }
        } else if (startsWithIgnoreCaseAndWs(sql, "--", 0) || startsWithIgnoreCaseAndWs(sql, "#", 0)) {
            statementStartPos = sql.indexOf('\n');

            if (statementStartPos == -1) {
                statementStartPos = sql.indexOf('\r');

                if (statementStartPos == -1) {
                    statementStartPos = 0;
                }
            }
        }

        return statementStartPos;
    }

    /**
     * Useful function.
     */

    public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor) {
        return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
    }

    public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos) {
        if (searchIn == null) {
            return searchFor == null;
        }

        int inLength = searchIn.length();

        for (; beginPos < inLength; beginPos++) {
            if (!Character.isWhitespace(searchIn.charAt(beginPos))) {
                break;
            }
        }

        return startsWithIgnoreCase(searchIn, beginPos, searchFor);
    }

    public final static String UNION_KW = "\nUNION ALL\n";
    public final static String ORDERBY_KW = " ORDER BY ";
    public final static String LIMIT_KW = " LIMIT ";
    public final static String UNION_ALIAS = "__DRDS_ALIAS_T_";

    /**
     * Convert string value to boolean value.
     * TRUE/ON/1 will be converted to true.
     * FALSE/OFF/0 will be converted to false.
     * For other cases, return null.
     */
    public static Boolean convertStringToBoolean(String val) {
        if ("true".equalsIgnoreCase(val) || "on".equalsIgnoreCase(val) || "1".equalsIgnoreCase(val)) {
            return true;
        }
        if ("false".equalsIgnoreCase(val) || "off".equalsIgnoreCase(val) || "0".equalsIgnoreCase(val)) {
            return false;
        }
        return null;
    }

    public static Map<Integer, ParameterContext> listToMap(List<ParameterContext> parameterContexts) {
        if (parameterContexts == null || parameterContexts.size() == 0) {
            return Collections.emptyMap();
        }
        Map<Integer, ParameterContext> map = Maps.newHashMap();
        for (int i = 0; i < parameterContexts.size(); i++) {
            map.put(i, parameterContexts.get(i));
        }
        return map;
    }

    public static List<ParameterContext> mapToList(Map<Integer, ParameterContext> param) {
        if (param == null) {
            return Collections.emptyList();
        }
        List<ParameterContext> p = new ArrayList<>(param.size());
        for (int i = 1; i <= param.size(); i++) {
            if (param.get(i) == null) {
                p.add(null);
            } else {
                p.add(param.get(i));
            }
        }
        return p;
    }

    public static List<ParameterContext> prepareParam(List<ParameterContext> paramList) {
        List<ParameterContext> r = Lists.newArrayList();
        if (paramList == null || paramList.isEmpty()) {
            return paramList;
        }
        boolean hasRawString = false;
        for (ParameterContext parameterContext : paramList) {
            if (parameterContext != null && parameterContext.getValue() instanceof RawString) {
                hasRawString = true;
                break;
            }
        }
        if (!hasRawString) {
            return paramList;
        }
        int indexCurrent = 1;
        for (ParameterContext parameterContext : paramList) {
            if (parameterContext.getValue() instanceof RawString) {
                for (Object o : ((RawString) parameterContext.getValue()).getObjList()) {
                    if (o instanceof List) {
                        for (Object sub : (List<?>) o) {
                            r.add(new ParameterContext(parameterContext.getParameterMethod(),
                                new Object[] {indexCurrent++, sub}));
                        }
                    } else {
                        r.add(new ParameterContext(parameterContext.getParameterMethod(),
                            new Object[] {indexCurrent++, o}));
                    }
                }
            } else {
                if (parameterContext.getArgs()[0] == null || (int) parameterContext.getArgs()[0] != indexCurrent) {
                    Object[] args = parameterContext.getArgs().clone();
                    args[0] = indexCurrent;
                    r.add(new ParameterContext(parameterContext.getParameterMethod(), args));
                } else {
                    r.add(parameterContext);
                }
                indexCurrent++;
            }
        }
        return r;
    }

    /**
     * decode statistic trace info, like :
     * Catalog:tpch_100g,lineitem,l_shipdate,null_1998-09-02
     * Action:getRangeCount
     * StatisticValue:693554944
     * <p>
     * after decode:
     * key: Catalog:tpch_100g,lineitem,l_shipdate,null_1998-09-02 Action:getRangeCount
     * value:693554944
     *
     * @param statisticTraceInfo statistic trace info, return by explain cost_trace
     * @return normalize info for statistic trace info
     */
    public static Map<String, String> decode(String statisticTraceInfo) throws IOException {
        if (StringUtils.isEmpty(statisticTraceInfo)) {
            return Collections.emptyMap();
        }
        Map<String, String> statisticTraceMap = Maps.newHashMap();
        BufferedReader lineReader = new BufferedReader(new StringReader(statisticTraceInfo));
        String line;
        while ((line = lineReader.readLine()) != null) {
            // find Key
            line = line.trim();
            if (line.startsWith("Catalog:")) {
                String actionLine = lineReader.readLine().trim();
                if (!actionLine.startsWith("Action:")) {
                    continue;
                }
                line = removeIdxSuffix(line);
                String key = line + "\n" + actionLine;

                String statisticResultLine = lineReader.readLine().trim();
                if (statisticResultLine.length() > "StatisticValue:".length()) {
                    statisticResultLine = statisticResultLine.substring("StatisticValue:".length());
                }
                statisticTraceMap.put(key.toLowerCase(), statisticResultLine);
            }
        }
        return statisticTraceMap;
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

    /**
     * remove the suffix of gsi name
     */
    public static String removeIdxSuffix(String source) {
        if (source.contains("_$")) {
            int targetIndex = source.indexOf("_$");
            int secondIndex = source.indexOf(",", targetIndex);
            if (secondIndex != -1) {
                return source.substring(0, targetIndex) + source.substring(secondIndex);
            } else {
                return source.substring(0, targetIndex);
            }
        } else {
            return source;
        }
    }

    /**
     * return target length random string
     */
    public static String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char ch = (char) (ThreadLocalRandom.current().nextInt('x' - 'a') + 'a');
            sb.append(ch);
        }
        return sb.toString();
    }
}
