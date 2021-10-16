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

package com.alibaba.polardbx.rule.impl.groovy;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorException;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.rule.enumerator.utils.CrossMonthDate;
import com.alibaba.polardbx.rule.enumerator.utils.CrossYearDate;
import com.alibaba.polardbx.rule.meta.ShardFunctionMeta;
import com.alibaba.polardbx.rule.meta.StringNum;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

/**
 * 可直接用于groovy规则中的便捷方法
 *
 * @author shenxun
 * @author linxuan
 */
public class GroovyStaticMethod {

    private final static long[] pow10 = {
        1, 10, 100, 1000, 10000, 100000, 1000000, 10000000,
        100000000, 1000000000, 10000000000L, 100000000000L,
        1000000000000L, 10000000000000L, 100000000000000L,
        1000000000000000L, 10000000000000000L,
        100000000000000000L, 1000000000000000000L};

    private final static Convertor commonConvertor = ConvertorHelper.commonToCommon;
    private final static ThreadLocal threaLocal = new ThreadLocal();
    private final static SimpleDateFormat yyyyMM = new SimpleDateFormat("yyyyMM");
    private final static SimpleDateFormat yyyyMMdd = new SimpleDateFormat("yyyyMMdd");
    static BigInteger _33 = new BigInteger("33");
    static BigInteger _l = new BigInteger("18446744073709551615");
    static BigInteger _31 = new BigInteger("31");
    static int BKDR_Seed = 31;
    private final static Date NULL_DATE = new Date(0);

    /**
     * @return 返回4位年份
     */
    public static int yyyy(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.YEAR);
    }

    public static int yyyy(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        return cal.get(Calendar.YEAR);
    }

    /**
     * @return 返回2位年份（年份的后两位）
     */
    public static int yy(Date date) {
        if (date == null) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.YEAR) % 100;
    }

    public static int yy(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        return cal.get(Calendar.YEAR) % 100;
    }

    /**
     * @return 返回月份数字，注意：从1开始：1-12（返回 Calendar.MONTH对应的值加1）
     */
    public static int month(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.MONTH) + 1;
    }

    public static int month(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        return cal.get(Calendar.MONTH) + 1;
    }

    /**
     * @return 返回2位的月份字串，从01开始：01-12（Calendar.MONTH对应的值加1）
     */
    public static String mm(Date date) {
        Calendar cal = getCalendar(date);
        int m = cal.get(Calendar.MONTH) + 1;
        return m < 10 ? "0" + m : String.valueOf(m);
    }

    public static String mm(Calendar cal) {
        int m = cal.get(Calendar.MONTH) + 1;
        return m < 10 ? "0" + m : String.valueOf(m);
    }

    /**
     * @return 返回 Calendar.DAY_OF_WEEK 对应的值
     */
    public static int week(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.DAY_OF_WEEK);
    }

    public static int week(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        return cal.get(Calendar.DAY_OF_WEEK);
    }

    /**
     * 旧规则默认的dayofweek : 如果offset = 0;那么为默认 SUNDAY=1; MONDAY=2; TUESDAY=3;
     * WEDNESDAY=4; THURSDAY=5; FRIDAY=6; SATURDAY=7;
     */
    public static int dayofweek(Date date, int offset) {
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.DAY_OF_WEEK) + offset;
    }

    public static int dayofweek(Calendar cal, int offset) {
        return cal.get(Calendar.DAY_OF_WEEK) + offset;
    }

    /**
     * 计算输入的日期是一年中第几个星期,
     */
    public static int weekofyear(Date date, int offset) {
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.WEEK_OF_YEAR) + offset;
    }

    /**
     * 旧规则的dayofweek.因为旧规则计算结果为数组下标，尤其表规则结果是库内表数组下标，必须从0开始， 因此必须让day of week从0
     * 开始。通过直接offset = -1 解决：星期日=0,星期一=1,...星期六=6
     */
    public static int dayofweek(Date date) {
        return dayofweek(date, -1);
    }

    public static int dayofweek(Calendar cal) {
        return dayofweek(cal, -1);
    }

    /**
     * @return 返回4位年份和2位月份的字串，月份从01开始：01-12
     */
    public static String yyyymm(Date date) {
        Calendar cal = getCalendar(date);
        return yyyy(cal) + mm(cal);
    }

    public static String yyyymm(Calendar cal) {
        return yyyy(cal) + mm(cal);
    }

    public static String yyyymmdd(Date date) {
        Calendar cal = getCalendar(date);
        return yyyy(cal) + mm(cal) + dd(cal);
    }

    public static String yyyymmdd(Calendar cal) {
        return yyyy(cal) + mm(cal) + dd(cal);
    }

    public static String yyyymmdd(long millis) {
        Calendar cal = (Calendar) threaLocal.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            threaLocal.set(cal);
        }
        cal.setTimeInMillis(millis);
        return yyyymmdd(cal);
    }

    /**
     * @return 返回 4位年份_2位月份 的字串，月份从01开始：01-12
     */
    public static String yyyy_mm(Date date) {
        Calendar cal = getCalendar(date);
        return yyyy(cal) + "_" + mm(cal);
    }

    public static String yyyy_mm(Calendar cal) {
        return yyyy(cal) + "_" + mm(cal);
    }

    /**
     * @return 返回2位年份和2位月份的字串，月份从01开始：01-12
     */
    public static String yymm(Date date) {
        Calendar cal = getCalendar(date);
        return yy(cal) + mm(cal);
    }

    public static String yymm(Calendar cal) {
        return yy(cal) + mm(cal);
    }

    /**
     * @return 返回 2位年份_2位月份 的字串，月份从01开始：01-12
     */
    public static String yy_mm(Date date) {
        Calendar cal = getCalendar(date);
        return yy(cal) + "_" + mm(cal);
    }

    public static String yy_mm(Calendar cal) {
        return yy(cal) + "_" + mm(cal);
    }

    /**
     * @return 返回 Calendar.DATE 对应的值。每月的1号值为1, 2号值为2...
     */
    public static int date(Date date) {
        Calendar cal = getCalendar(date);
        return cal.get(Calendar.DATE);
    }

    public static int date(Calendar cal) {
        return cal.get(Calendar.DATE);
    }

    /**
     * @return 返回2位的天数，从01开始：01-31（Calendar.DATE对应的值）
     */
    public static String dd(Date date) {
        Calendar cal = getCalendar(date);
        int d = cal.get(Calendar.DATE);
        return d < 10 ? "0" + d : String.valueOf(d);
    }

    public static String dd(Calendar cal) {
        int d = cal.get(Calendar.DATE);
        return d < 10 ? "0" + d : String.valueOf(d);
    }

    public static String mmdd(Date date) {
        Calendar cal = getCalendar(date);
        return mm(cal) + dd(cal);
    }

    public static String mmdd(Calendar cal) {
        return mm(cal) + dd(cal);
    }

    @SuppressWarnings("unused")
    private static Calendar getCalendar(Calendar c) {
        return c;
    }

    private static Calendar getCalendar(Date date) {
        Calendar cal = (Calendar) threaLocal.get();
        if (cal == null) {
            cal = Calendar.getInstance();
            threaLocal.set(cal);
        }
        cal.setTime(date);
        return cal;
    }

    /**
     * @param bit 补齐后的长度
     * @param table 数值
     * @return 返回前面补0达到bit长度的字符串。如果table长度大于bit，则返回table的原始值
     */
    public static String placeHolder(int bit, long table) {
        if (bit > 18) {
            throw new IllegalArgumentException("截取的位数不能大于18位");
        }
        if (table == 0) {
            // bugfix 被0除
            return String.valueOf(pow10[bit]).substring(1);
        }
        if (table >= pow10[bit - 1]) {
            // 当数值的width >= 要求的补齐位数时，应该直接返回原始数值
            return String.valueOf(table);
        }
        long max = pow10[bit];
        long placedNumber = max + table;
        return String.valueOf(placedNumber).substring(1);
    }

    @SuppressWarnings("unused")
    private static long getModRight(long targetID, int size, int bitNumber) {
        if (bitNumber < size) {
            throw new IllegalArgumentException("输入的位数比要求的size还小");
        }
        return (size == 0 ? 0 : targetID / pow10[bitNumber - size]);
    }

    /**
     * 从左开始，取指定多的位数。默认是一个long形长度的数据，也就是bitNumber= 19
     *
     * @param targetID 目标id，也就是等待被decode的数据
     * @param st 从哪儿开始取，如果想取最左边的一位那么可以输入st = 0;ed =1;
     * @param ed 取到哪儿，如果想取最左边的两位，那么可以输入st = 0;ed = 2;
     */
    // public static long leftBetween(long targetID,int st,int ed){
    // int sizeAll = st + ed - 1;
    // if(sizeAll >= 19||sizeAll <= 0){
    // throw new IllegalArgumentException("截取19位请直接使用元数据。");
    // }
    // if(targetID / pow10[sizeAll] < 1){
    // throw new IllegalArgumentException(targetID+",小于"+(st+ed)+"位，不能进行计算");
    // }
    // long end = getModRight(targetID, ed,19);
    // return end % pow10[(ed-st)];
    // }
    public static int quarter(Date date) {
        Calendar cal = getCalendar(date);
        int month = cal.get(Calendar.MONTH);
        return quarter(month);
    }

    public static int quarter(long month) {
        return quarter((int) month);
    }

    public static int halfayear(long month) {
        return halfayear((int) month);
    }

    public static int quarter(int month) {
        if (month > 11 || month < 0) {
            throw new IllegalArgumentException("month range is 1~12");
        }
        return month / 3 + 1;
    }

    public static int halfayear(Date date) {
        Calendar cal = getCalendar(date);
        int month = cal.get(Calendar.MONTH);
        return halfayear(month);
    }

    public static int halfayear(int month) {
        if (month > 11 || month < 0) {
            throw new IllegalArgumentException("month range is 1~12,current value is " + month);
        }
        return month / 6 + 1;
    }

    /**
     * 从右开始，取指定多的位数。 假如参数是1234567.那么rightBetwen(1234567,2,3) 返回的数据是 345
     * rightBetween(10000234,2,2) 返回的数据是2 rightBetween(10000234,3,2) 返回的数据是0
     *
     * @param targetID 目标id，也就是等待被decode的数据
     * @param closeFrom 从哪儿开始取，如果想取最右边的一位那么可以输入st = 0;ed =1;
     * @param openTo 取到哪儿，如果想取最右边的两位，那么可以输入st = 0;ed = 2;
     * @throws IllegalArgumentException 如果st+ed -1 >= 19,这时候对long来说不需要截取。
     * 如果targetId小于st+ed，
     */
    public static long rightCut(long targetID, int closeFrom, int openTo) {
        int sizeAll = closeFrom + openTo - 1;
        if (sizeAll >= 19 || sizeAll < 0) {
            throw new IllegalArgumentException("截取19位请直接使用元数据。");
        }

        long right = targetID / pow10[(closeFrom)];
        right = right % pow10[openTo];

        return right;
    }

    public static long right(long targetID, int size) {
        if (size >= 19 || size < 0) {
            throw new IllegalArgumentException("截取19位请直接使用元数据。");
        }
        return targetID % pow10[size];
    }

    public static void validate(long targetID, int size) {
        if (targetID / pow10[size - 1] < 1) {
            throw new IllegalArgumentException(targetID + ",小于" + (size) + "位，不能进行计算");
        }
    }

    public static String right(String right, int rightLength) {
        int length = right.length();
        int start = length - rightLength;
        return right.substring(start < 0 ? 0 : start);
    }

    public static Long longValue(Object o) {
        if (o == null) {
            return null;
        }

        try {
            return (Long) commonConvertor.convert(o, Long.class);
        } catch (ConvertorException e) {
            Convertor convertor = ConvertorHelper.getInstance().getConvertor(o.getClass(), Long.class);
            if (convertor != null) {
                return (Long) convertor.convert(o, Long.class);
            } else {
                throw new ConvertorException(
                    "Unsupported convert: [" + o.getClass().getName() + "," + Long.class.getName() + "]");
            }
        }
    }

    public static long wangwangHash(String str, int n) {
        BigInteger h = BigInteger.ZERO;

        for (int i = 0; i < str.length(); i++) {
            String charx = String.valueOf((int) str.charAt(i));
            BigInteger temp2 = h.multiply(_33);
            BigInteger temp1 = temp2.add(new BigInteger(charx));
            h = h.add(temp1);

            if (h.compareTo(_l) > 0) {
                h = h.and(_l);
            }
        }

        if (n == -1) {
            n = str.length();
        }

        for (int i = n - 1; i > 0; i--) {
            BigInteger temp = new BigInteger(String.valueOf(i * (i + 1)));
            BigInteger k = h.mod(temp);
            BigInteger temp1 = new BigInteger(String.valueOf(i * i));
            BigInteger temp2 = new BigInteger(String.valueOf(i * i - 1));
            if (k.compareTo(temp1) >= 0) {
                return i;
            }
            if (k.compareTo(temp2) >= 0) {
                return i - 1;
            }
        }

        return 0;
    }

    public static long junxingHash(String str) {
        byte[] bytes = null;
        try {
            bytes = str.getBytes("latin1");
        } catch (UnsupportedEncodingException e) {
            throw GeneralUtil.nestedException(e);
        }

        int hash = 0;
        for (int i = 0; i < bytes.length; i++) {
            hash = hash * 31 + bytes[i];
        }

        return hash;
    }

    public static long wangwangPushHash(String str, int n, int tableCount) {
        if (n != -1) {
            str = TStringUtil.substring(str, 0, n);
        }
        BigInteger h = BigInteger.ZERO;
        for (int i = 0; i < str.length(); i++) {
            String charx = String.valueOf((int) str.charAt(i));
            BigInteger temp2 = h.multiply(_31);
            BigInteger temp1 = temp2.add(new BigInteger(charx));
            h = temp1;
            if (h.compareTo(_l) > 0) {
                h = h.and(_l);
            }
        }
        return h.mod(BigInteger.valueOf(tableCount)).intValue();
    }

    public static long wangwangBKDRHash(String str, int n, int tableCount) {

        int hash = 0;
        byte[] bytes = null;
        try {
            bytes = str.getBytes("latin1");
        } catch (UnsupportedEncodingException e) {
            throw GeneralUtil.nestedException(e);
        }

        for (int i = 0; i < bytes.length; i++) {
            int c = bytes[i] & 0xff;
            int temp2 = hash * BKDR_Seed;
            int temp1 = temp2 + c;
            hash = temp1;

        }
        hash = hash & Integer.MAX_VALUE; //ensure positive
        int idx = hash % tableCount;
        return idx;
    }

    /**
     * 新版本cobar的hash算法 用来支持旧版tddl中的算法
     */
    public static long cobarHash(String s, int start, int end) {
        if (start < 0) {
            start = 0;
        }
        if (end > s.length()) {
            end = s.length();
        }
        long h = 0;
        for (int i = start; i < end; ++i) {
            h = (h << 5) - h + s.charAt(i);
        }
        return h;
    }

    /**
     * 旧版本cobar的hash算法 用来支持旧版tddl中的算法
     */
    public static final long cobarOldHash(String s, int len) {
        long h = 0;
        int sLen = s.length();
        for (int i = 0; (i < len && i < sLen); i++) {
            h = (h << 5) - h + s.charAt(i);
        }
        return h;
    }

    public static int mm_i(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        int m = cal.get(Calendar.MONTH) + 1;
        return m;
    }

    public static int mm_i(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        int m = cal.get(Calendar.MONTH) + 1;
        return m;
    }

    public static int dd_i(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        int d = cal.get(Calendar.DATE);

        if (date instanceof CrossMonthDate) {
            d += ((CrossMonthDate) date).getOffset();
        }

        return d;
    }

    public static int dd_i(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        int d = cal.get(Calendar.DATE);
        return d;
    }

    public static int yyyy_i(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        int d = cal.get(Calendar.YEAR);
        return d;
    }

    public static int mmdd_i(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);

        int dayOfYear = cal.get(Calendar.DAY_OF_YEAR);

        if (date instanceof CrossYearDate) {
            long offset = ((CrossYearDate) date).getOffset();
            dayOfYear += offset;
        }

        return dayOfYear;
    }

    public static int mmdd_i(Calendar cal) {
        if (cal == null) {
            return 0;
        }
        return cal.get(Calendar.DAY_OF_YEAR);
    }

    public static Integer yyyymm_i(Date date) {
        if (date == NULL_DATE) {
            return 0;
        }
        Calendar cal = getCalendar(date);
        return yyyy_i(cal) * 12 + mm_i(cal);
    }

    public static Integer yyyymm_noloop(Date date) {
        return Integer.valueOf(yyyyMM.format(date));
    }

    public static Integer yyyydd_noloop(Date date) {
        return Integer.valueOf(yyyyMMdd.format(date));
    }

    public static Integer yyyyweek_noloop(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.setMinimalDaysInFirstWeek(1);
        cal.setTime(date);
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        int mouth = cal.get(Calendar.MONTH) + 1; // WEEK_OF_YEAR计算年月从0开始
        int year = cal.get(Calendar.YEAR);
        // JDK think 2015-12-31 as 2016 1th week
        // JDK会把下一年的1月1号和在同一个周上的本年度12月末的一些日期算为下一年的第一周，如2015-12-28 至 2015-12-31
        // 分别为周一到周四，2016-01-01为周五，那么JDK会将2015-12-28 至 2015-12-31 算进2016的第一个周里
        // 一年有53个周，如果月份是12月，且求出来的周数是第一周，说明该日期实质上是这一年的第53周，也是下一年的第一周，为了拓扑连贯性，将本年度的这些日期计算到下一年的第一个周里，故year自增1
        if (mouth == 12 && week == 1) {
            ++year;
        }
        return Integer.valueOf(String.valueOf(year) + String.valueOf(week));
    }

    /**
     * yyyymm_i_opt是yyyymm_i的优化型Hash函数
     * <p>
     * <p>
     * <pre>
     *
     * 1. 当 allDbCount > 1 && allInstCount > 1时，
     *      它能实现随着date的月份的线上递增，其hash值的能平均落到DRDS之下的多个MySQL实例上，
     *      ，通常分库分表键相同并且分库和分表函数都使用yyyymm_i_opt函数时，就会是这种情况
     *
     * 2. 注意，当allDbCount == -1 && allInstCount == -1时，
     *      yyyymm_i_opt就会直接退化为 “自动带取余的yyyymm_i函数”，例如
     *      yyyymm_i_opt(dateCol, 8, 0 ,0) 实质等价于  yyyymm_i(dateCol) % 8
     *      ，通常 分库分表函数不一致 并且 分库或分表函数使用的是yyyymm_i_opt函数时，就会是这种情况
     * </pre>
     */
    public static Long yyyymm_i_opt(Date date, int allTbCount, int allDbCount, int allInstCount) {

        // 所有的分表数目
        int tbNum = allTbCount;

        // 所有的分库数目
        int dbNum = allDbCount;

        // 所有的MySQL数目
        int instNum = allInstCount;

        // 获取date所对应的tb的hash值
        long tbHashVal = yyyymm_i(date).longValue() % tbNum;

        return partitionOptimize(dbNum, instNum, tbHashVal);
    }

    /**
     * 根据date计算由年份与一年中的周数算出的hash值
     */
    public static Long yyyyweek_i(Date date) {
        if (date == NULL_DATE) {
            return 0L;
        }
        Calendar cal = getCalendar(date);
        int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
        int yearOfDate = cal.get(Calendar.YEAR);

        /**
         * DRDS的跨年周的定义
         * 1. 一年中的第一个周从是周日开始算起（即SUNNY）
         * 2. 一年中的第一个周要求的至少天数是1
         * （更直白一点，当一个周跨年了，那这个周就直接算在新一年的第1周）
         *
         */

        /**
         * 这里要判断一下跨年周的问题
         *
         * <pre>
         *
         * 例如， 2012-12-31 是 周一
         *
         * 但是，一周的从周日开始计算的，这个周( 2012-12-30 周日 ~ 2013-01-05 周六)
         * 因为有超过5天跨了2013年，所以这个日期所对应的这个周 就要算为 2013年的第1周
         *
         * 所以 2012-12-31 这一天其实是2013年的第一个周，
         *
         * 所以， yyyyweek_i(2012-12-31)的 结果应该是 2013 * 54 +　1
         * </pre>
         */
        // 计算一下7天前的日期
        Calendar cal7DaysBefore = (Calendar) cal.clone();
        cal7DaysBefore.add(Calendar.WEEK_OF_YEAR, -1);

        // 获取一下7天前的年份
        int yearOfWeekOf7DaysBefore = cal7DaysBefore.get(Calendar.YEAR);
        // 获取一下7天前的周数
        int weekOfYearOf7DaysBefore = cal7DaysBefore.get(Calendar.WEEK_OF_YEAR);

        // 如果7天前的周数比当前的还大，说明出现跨年周了，年数加1
        int yearOfWeek = yearOfWeekOf7DaysBefore;
        if (weekOfYear < weekOfYearOf7DaysBefore) {
            ++yearOfWeek;
        } else {
            /**
             * 这里是一个路由计算的BUG, 但不能修改了，只能将错就错
             *
             * 虽然 7天前日期所对应的周与当前日期对应的周是同属于一个年份，
             * 但是7天前的日期所对应的周可能刚好跨年周，这个日期的年份有可能会比当前的日期少一年。
             * 例如
             *  2018-12-31 其实是2019年的第1周
             *  所以，对于 2019-01-06 （它是2019年第2周），它的7天前的日期（2018-12-31）的年份是2018年
             *  ，而当前日期的年份2019刚好少1（这样会导致对于 2019-01-06 这个日期，它计算哈希值使用的
             *  年份是2018年，而不是2019年，比实际正确的值少了54）.
             *
             *  它下边的哈希函数的计算"yearOfWeek * 54 + weekOfYear"中的yearOfWeek，实际上应该是用
             *  2019年来计算，而不是2018年，即  yearOfWeek = yearOfDate;
             *  但是考虑到路由算法的兼容性(改了后用户升级后可能会有数据查不到)，所以不能修改。
             */
            //yearOfWeek = yearOfDate;

        }

        // 平年是52星期余1天；闰年是52星期余2天, 所以乘以 54, 保证不重合
        long hashVal = yearOfWeek * 54 + weekOfYear;

        if (date instanceof CrossYearDate) {
            long offset = ((CrossYearDate) date).getOffset();
            hashVal += offset;
            if (hashVal < 0) {
                hashVal += 54;
            }
        }

        return hashVal;
    }

    public static Long yyyyweek_i_opt(Date date, int allTbCount, int allDbCount, int allInstCount) {

        // 所有的分表数目
        int tbNum = allTbCount;

        // 所有的分库数目
        int dbNum = allDbCount;

        // 所有的MySQL数目
        int instNum = allInstCount;

        // 获取date所对应的tb的hash值
        long tbHashVal = yyyyweek_i(date).longValue() % tbNum;

        return partitionOptimize(dbNum, instNum, tbHashVal);
    }

    /**
     * 根据日期date，结合年份与一年中的天数计算其hash值
     */
    public static Long yyyydd_i(Date date) {
        if (date == NULL_DATE) {
            return 0L;
        }
        Calendar cal = getCalendar(date);

        long yearOfDay = cal.get(Calendar.YEAR);
        long datOfYear = cal.get(Calendar.DAY_OF_YEAR);

        // 一年最多366天
        long tbHashVal = yearOfDay * 366 + datOfYear;

        if (date instanceof CrossYearDate) {
            long offset = ((CrossYearDate) date).getOffset();
            tbHashVal += offset;
        }

        return tbHashVal;
    }

    public static Long yyyydd_i_opt(Date date, int allTbCount, int allDbCount, int allInstCount) {

        // 所有的分表数目
        int tbNum = allTbCount;

        // 所有的分库数目
        int dbNum = allDbCount;

        // 所有的MySQL数目
        int instNum = allInstCount;

        // 获取date所对应的tb的hash值
        long tbHashVal = yyyydd_i(date).longValue() % tbNum;

        return partitionOptimize(dbNum, instNum, tbHashVal);
    }

    /**
     * 根据 拆分键的值经过处理得到的哈希值 ，进行分库分表的数据分片的下标计算
     *
     * @param shardKeyHashIdx 拆分键的值经过处理得到的哈希值
     * @param dbCount 分库数目
     * @param tbCountEachDb 各个分库的物理表数目
     * @param shardType 标识是否为进行分库计算还是分表计算，0表示分库，1表示分表
     * @param isTbStandAlone 标识物理表名的下标是否为全局唯一（即各分库之间互不相同）,
     * true为允许各分库的分表重复，false为各分库的分表名全局唯一
     */
    protected static long hashPartitionIndex(long shardKeyHashIdx, int dbCount, int tbCountEachDb, int shardType,
                                             boolean isTbStandAlone) {

        shardKeyHashIdx = Math.abs(shardKeyHashIdx);
        if (shardType == ShardFunctionMeta.SHARD_TYPE_DB) {
            long dbIdx = shardKeyHashIdx % dbCount;
            // double ABS, 防止下标出现负数
            dbIdx = Math.abs(dbIdx);
            return dbIdx;
        } else if (shardType == ShardFunctionMeta.SHARD_TYPE_TB) {

            if (!isTbStandAlone) {
                /**
                 * <pre>
                 *
                 *  HASH算法如下:
                 *      shardKeyHashIdx必须是一个正整数
                 *
                 *      首先将 dbCount（总分库数目）看成是 dbCount 个槽位，将 shardKeyHashIdx的值 看成是 shardKeyHashIdx 个球；
                 *
                 *      1.求 shardKeyHashIdx 对应的分库下标：
                 *
                 *          将 shardKeyHashIdx 个球， 依次循环放入 1，..., dbCount个槽位中，最后一个球对应的所在的槽位就是分库下标。
                 *
                 *          这等价于数学公式:
                 *              dbIdx = shardKeyHashIdx % dbCount
                 *
                 *      2.求 shardKeyHashIdx 对应的分表下标：
                 *
                 *          将 shardKeyHashIdx 个球， 依次循环 放入 1，..., dbCount个槽位后，取各个槽位的球数的最小值,
                 *          即 N = shardKeyHashIdx / dbCount  ，
                 *
                 *          这时假设每个分库的各个分表数tbCountEachDb，也可认为是tbCountEachDb个子槽位, 然后槽内的再将N个球再依次循环放入 1，2，..., tbCountEachDb
                 *          子槽位中，那么最后一个球所在的子槽位（用 <dbIdx,tbIdxInDb>, dbIdx是第dbIdx个槽，tbIdxInDb是第tbIdxInDb个子槽 ），就是分表下标:
                 *             tbIdxInDb = N % tbCountEachDb
                 *          ，而这个分表下标的全局唯一标识是
                 *
                 *             finalTbIdx =  dbIdx * tbCountEachDb + tbIdxInDb
                 * </pre>
                 */
                long tbIdxInDb = (shardKeyHashIdx / dbCount) % tbCountEachDb;
                long dbIdx = shardKeyHashIdx % dbCount;
                long finalTbIdx = dbIdx * tbCountEachDb + tbIdxInDb;

                // double ABS, 防止下标出现负数
                finalTbIdx = Math.abs(finalTbIdx);
                return finalTbIdx;
            } else {
                long tbIdxInDb = shardKeyHashIdx % tbCountEachDb;

                // double ABS, 防止下标出现负数
                tbIdxInDb = Math.abs(tbIdxInDb);
                return tbIdxInDb;
            }

        } else {
            // 要抛错error
            return -1;
        }

    }

    /**
     * 有符号二进制右移
     *
     * @param shardKeyVal 拆分键的值
     * @param shiftBitCount 向右移位的二进制位数
     * @param dbCount 分库数目
     * @param tbCountEachDb 各个分库的物理表数目
     * @param shardType 标识是否为进行分库计算还是分表计算，0表示分库，1表示分表
     * @param isTbStandAlone 标识物理表名的下标是否为全局唯一（即各分库之间互不相同）
     */
    public static Long bin_right_shift(long shardKeyVal, int shiftBitCount, int dbCount, int tbCountEachDb,
                                       int shardType, boolean isTbStandAlone) {
        long finalHashVal = shardKeyVal;
        finalHashVal = finalHashVal >> shiftBitCount;

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;

    }

    /**
     * 无符号二进制右移
     *
     * @param shardKeyVal 拆分键的值
     * @param shiftBitCount 向右移位的二进制位数
     * @param dbCount 分库数目
     * @param tbCountEachDb 各个分库的物理表数目
     * @param shardType 标识是否为进行分库计算还是分表计算，0表示分库，1表示分表
     * @param isTbStandAlone 标识物理表名的下标是否为全局唯一（即各分库之间互不相同）
     */
    public static Long bin_right_shift_u(long shardKeyVal, int shiftBitCount, int dbCount, int tbCountEachDb,
                                         int shardType, boolean isTbStandAlone) {
        long finalHashVal = shardKeyVal;
        finalHashVal = finalHashVal >>> shiftBitCount;

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;

    }

    /**
     * 范围HASH
     *
     * @param shardKeyVal 拆分键的值
     * @param dbCount 分库数目
     * @param tbCountEachDb 各个分库的物理表数目
     * @param shardType 标识是否为进行分库计算还是分表计算，0表示分库，1表示分表
     * @param isTbStandAlone 标识物理表名的下标是否为全局唯一（即各分库之间互不相同）
     */
    public static Long range_hash(Number shardKeyVal, int rangeCount, int dbCount, int tbCountEachDb, int shardType,
                                  boolean isTbStandAlone) {
        String str = shardKeyVal + "";
        long finalHashVal = Long
            .valueOf(str.substring((str.length() - rangeCount) >= 0 ? (str.length() - rangeCount) : 0, str.length()));

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;

    }

    /**
     * 范围HASH
     *
     * @param shardKeyVal 拆分键的值
     * @param dbCount 分库数目
     * @param tbCountEachDb 各个分库的物理表数目
     * @param shardType 标识是否为进行分库计算还是分表计算，0表示分库，1表示分表
     * @param isTbStandAlone 标识物理表名的下标是否为全局唯一（即各分库之间互不相同）
     */
    public static Long range_hash(String shardKeyVal, int rangeCount, int dbCount, int tbCountEachDb, int shardType,
                                  boolean isTbStandAlone) {
        long finalHashVal = shardKeyVal
            .substring((shardKeyVal.length() - rangeCount) >= 0 ? (shardKeyVal.length() - rangeCount) : 0,
                shardKeyVal.length())
            .hashCode();

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;

    }

    public static Long uni_hash(Number shardKeyVal, int dbCount, int tbCountEachDb, int shardType,
                                boolean isTbStandAlone) {
        long finalHashVal = shardKeyVal.hashCode();

        // In 5.3.2-1667887 to 5.3.3-1687582, BigInteger is always converted to Long, which is a bug.
        // But to be compatible with these versions, we need a JVM parameter to enable it, with "-DconvertBigIntegerToLong=true".
        // Typically, we don't suggest it to be enabled.
        String convertBigIntegerToLong = System.getProperty("convertBigIntegerToLong");
        if (StringUtils.isNotEmpty(convertBigIntegerToLong) && BooleanUtils.toBoolean(convertBigIntegerToLong)) {
            if (shardKeyVal instanceof BigInteger) {
                long longValOfShardKeyVal = ((BigInteger) shardKeyVal).longValue();
                Long longValObjOfShardKeyVal = new Long(longValOfShardKeyVal);
                finalHashVal = longValObjOfShardKeyVal.hashCode();
            }
        }

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;
    }

    public static Long uni_hash(String shardKeyVal, int dbCount, int tbCountEachDb, int shardType,
                                boolean isTbStandAlone) {
        long finalHashVal = shardKeyVal.hashCode();
        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;
    }

    /**
     * <pre>
     *
     *      dbpartition/tbpartition by STR_HASH( `name` )
     *      dbpartition/tbpartition by STR_HASH( `name`, begin, end )
     *      dbpartition/tbpartition by STR_HASH( `name`, begin, end, typeId)
     *      dbpartition/tbpartition by STR_HASH( `name`, begin, end, typeId, seed)
     *
     *          typeId               action
     *            str（默认）     被截取的子字符串当字符串处理
     *            int           被截取的子字符串当整数处理, 如 'ABCD1234'
     *
     *          begin       end            action(第1个字符从0开始标记)
     *            -1        -1                  不做任何截取(默认)
     *            -1       m(>0)                截取字符串倒数最多m个字符, [len-m, len)
     *            m(>0)     -1                  从字符串开头截取最多m个字符, [0, m)
     *            j(>0)     k(>j>0)             在字符串中截取 [j,k) 之间的字符
     *
     *          seed  BKDR随机种子, 默认是31，如果HASH不均衡，可以调整为 131/1313/13131/131313 ...等值
     * </pre>
     */
    public static Long str_hash(Object shardKeyObj, int beginIndex, int endIndex, int typeId, int seed, int dbCount,
                                int tbCountEachDb, int shardType, boolean isTbStandAlone) {
        long finalHashVal = 0;
        String shardKeyVal = null;

        /**
         * 标识当前的shardKey的值是否是来自枚举值
         */
        boolean shardValFromEnum = false;
        if (shardKeyObj instanceof String) {
            shardKeyVal = (String) shardKeyObj;
        } else if (shardKeyObj instanceof StringNum) {
            // 此类型，说明数值的来源是字符串的自枚举（如初始化拓扑），对于自枚举的类型，该HASH函数直接当作数值处理
            // 从而能保证无论字符串截取出什么的数值，都能将所有分库分表枚举全
            shardValFromEnum = true;
        }

        if (!shardValFromEnum) {
            if (shardKeyVal != null) {
                String subStr = build_substr(shardKeyVal, beginIndex, endIndex);
                if (typeId == 0) {
                    if (subStr == null) {
                        finalHashVal = 0;
                    }
                    finalHashVal = bkdr_hash(subStr, seed);
                } else if (typeId == 1) {
                    if (subStr == null) {
                        finalHashVal = 0;
                    }
                    if (subStr.isEmpty()) {
                        finalHashVal = 0;
                    } else {
                        BigInteger intVal = new BigInteger(subStr);
                        finalHashVal = intVal.longValue();

                    }
                } else {
                    throw new TddlNestableRuntimeException(
                        "invalid typeId for str_hash funcion, typeId must be 0(string)/1(int)");
                }
            }
        } else {
            finalHashVal = ((StringNum) shardKeyObj).number;
        }

        long partitionIdx = hashPartitionIndex(finalHashVal, dbCount, tbCountEachDb, shardType, isTbStandAlone);
        return partitionIdx;
    }

    protected static String build_substr(String shardKeyVal, int beginIndex, int endIndex) {

        String targetStr = shardKeyVal;

        if (beginIndex < 0 && endIndex < 0) {
            if (beginIndex == -1 && endIndex == -1) {
                return targetStr;
            } else {
                throw new TddlNestableRuntimeException("invalid params of str_hash funcion");
            }

        } else if (beginIndex > 0 && endIndex < 0) {
            //
            int len = shardKeyVal.length();
            if (len < beginIndex) {
                return targetStr;
            }
            targetStr = shardKeyVal.substring(0, beginIndex);
        } else if (endIndex > 0 && beginIndex < 0) {
            int len = shardKeyVal.length();
            if (len < endIndex) {
                return targetStr;
            }
            int targetIdx = len - endIndex;
            targetStr = shardKeyVal.substring(targetIdx);
        } else {
            if (endIndex < beginIndex) {
                throw new TddlNestableRuntimeException("invalid params of str_hash funcion");
            }
            int len = shardKeyVal.length();
            if (len <= beginIndex) {
                // beginIndex 比len还长
                return "";
            } else if (len < endIndex) {
                targetStr = shardKeyVal.substring(beginIndex);
            } else {
                targetStr = shardKeyVal.substring(beginIndex, endIndex);
            }
        }
        return targetStr;
    }

    // bkdrSeed = 31 // or 131 1313 13131 1313131
    public static int bkdrSeed = 31;

    public static int bkdr_hash(String str, int seedVal) {
        int hash = 0;

        int seed = bkdrSeed;
        if (seedVal > 0) {
            seed = seedVal;
        }

        for (int i = 0; i != str.length(); ++i) {
            hash = seed * hash + str.charAt(i);
        }
        return hash;
    }

    private static Long partitionOptimize(int dbNum, int instNum, long tbHashVal) {
        if (dbNum < 1 || instNum < 1) {
            return tbHashVal;
        }

        // 求出每个MySQL实例上的分库数目
        long dbNumOfInst = dbNum / instNum;

        // 根据分表的hash值和分库数目再求出分表hash值的取余值，这个值可以理解为分表hash值所对应的db位移值
        long tableHashValAfterModDbNum = tbHashVal % dbNum;

        // 根据db位移值求出这个db所在MySQL实例的索引用， 即实例索引号
        long instIdx = tableHashValAfterModDbNum % instNum;

        // 根据db位移值求出这个db在其所在MySQL实例中各个分库中所处的位置号，即实例的库序号
        long dbIdxOfInst = tableHashValAfterModDbNum / instNum;

        // 最终根据 实例索引号 与 实例的库序号 求出 分表hash值 所对应实际分库hash值，即库索引
        long dbHashVal = dbIdxOfInst + instIdx * dbNumOfInst;

        return dbHashVal;
    }

    public static Object defaultIfNull(Object object, Object defaultValue) {
        return object != null ? object : defaultValue;
    }

    /**
     * <pre>
     * preProcessShardKeyValue的功能
     *
     * 作用：用于拆分键的键值进入hash函数进行计算时的预期处理
     *
     * 预处理工作：
     * 1. 处理null值，如果是null, 默认返回0L, 直接替代原来的 defaultIfNull 函数；
     * 2. 根据EnumType做必要的转型转换，如，STR_HASH只接收字符串，所以null值默认要返回"",
     *    或时间类型的枚举类型，那应该要返回0000-00-00 00:00:00.000的时间；
     * 3. 根据一些額处参数otherParams，做一些扩展。
     *
     * 能用于替代 defaultIfNull 函数，同时还其它
     *
     * </pre>
     */
    public static Object preProcessShardKeyValue(Map shardKeyValMap, String targetShardKeyName, String enumType,
                                                 Map otherParams) {

        Object shardVal = shardKeyValMap.get(targetShardKeyName);
        if (shardVal == null) {

            // 处理null值的情况
            if (AtomIncreaseType.STRNUM.toString().equalsIgnoreCase(enumType)) {
                return "";
            } else if (AtomIncreaseType.ATOM_ENUM_TYPE_STR_NAME_FOR_TIME.contains(enumType.toUpperCase())) {
                return NULL_DATE;
            }

            return 0L;
        } else {

            Object realShardVal = shardVal;
            if (shardVal instanceof StringNum) {
                if (!AtomIncreaseType.STRNUM.toString().equalsIgnoreCase(enumType)) {
                    realShardVal = ((StringNum) shardVal).number;
                }
            }

            return realShardVal;
        }

    }

}
