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

package com.alibaba.polardbx.qatest.data;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 用来生成每一列数据的类，按照用户定义的规则生成
 */
public class ColumnDataRandomGenerateRule {

    public List<Integer> integer_testValue = new ArrayList<Integer>();
    public AtomicLong pk = new AtomicLong();
    public final Random random = new Random();

    public long getPk() {
        return pk.getAndIncrement();
    }

    public void setPk(long pkId) {
        pk.set(pkId);
    }

    public void resetPk() {
        pk.set(0);
    }

    public Integer integer_testOneValue() {
        return 10;
    }

    public Integer integer_testRandom() {
        return random.nextInt();
    }

    public void clearInteger_testValue() {
        integer_testValue = new ArrayList<Integer>();
    }

    public Integer integer_testDifference() {
        Integer value = (Math.abs(new Random().nextInt()) % 1000) * 100;
        int i = 0;
        while (integer_testValue.contains(value)) {
            value = Math.abs(new Random().nextInt()) % 1000;
            if (i++ > 20) {
                throw new RuntimeException(" integer_test column get difference value fail !");
            }
        }
        integer_testValue.add(value);

        return value;
    }

    public Integer bigint_testOneValue() {
        return 12;
    }

    public Integer bigint_testRandom() {
        return random.nextInt();
    }

    public Integer smallint_testOneValue() {
        return 5;
    }

    public Integer smallint_testRandom() {
        return random.nextInt(32700);
    }

    public Integer mediumint_testOneValue() {
        return 9;
    }

    public Integer mediumint_testRandom() {
        return random.nextInt(8388607);
    }

    public Integer tinyint_testOneValue() {
        return 12;
    }

    public Integer tinyint_testRandom() {
        return random.nextInt(128);
    }

    public Integer tinyint_1bit_testOneValue() {
        return 18;
    }

    public Integer tinyint_1bit_testRandom() {
        return random.nextInt(128);
    }

    public Boolean bit_testOneValue() {
        return false;
    }

    public Boolean bit_testRandom() {
        return random.nextInt() % 2 == 0;
    }

    static Byte[] blobS = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};

    public Byte blob_testOneValue() {
        return blobS[2];
    }

    public Byte blob_testRandom() {
        return blobS[random.nextInt(128) % blobS.length];
    }

    static BigDecimal[] bigDecimalS = {
        new BigDecimal(10), new BigDecimal(100),
        new BigDecimal(1000), new BigDecimal(10000),
        new BigDecimal(100000), new BigDecimal(1000000),
        new BigDecimal(10000000), new BigDecimal(100000000),
        new BigDecimal(100000000), new BigDecimal(1000000000)};

    public BigDecimal decimal_testOneValue() {
        return bigDecimalS[2];
    }

    public BigDecimal decimal_testRandom() {
        return new BigDecimal(random.nextDouble());
    }

    static Double[] doubleS = {
        10.2145d, 21.258d, 35.1478d, 38.4879d, 40.47845d,
        48.1478d, 50.48745d, 55.1478d, 58.1245d, 80.4578d, 90.4587447d,
        180.457845d, 200.48787d, 250.4874d, 301.457d, 800.147d,
        1110.4747d, 1414.14747d, 1825.47484d, 2000.23232d};

    public Double double_testOneValue() {
        return doubleS[2];
    }

    public Double double_testRandom() {
        return random.nextDouble();
    }

    static float[] floatS = {
        4.55f, 4.56f, 4.32f, 4.23f, 5.34f, 6.78f, 4.51f, 7.34f,
        5.78f, 0.45f, 7.89f, 2.34f, 4.12f, 45.23f};

    public Float float_testOneValue() {
        return floatS[2];
    }

    public Float float_testRandom() {
        return random.nextFloat();
    }

    static String[] StringS = {
        "hello1234", "he343243", "word23", "feed32feed",
        "nihaore", "afdaewer", "hellorew", "abdfeed", "cdefeed",
        "adaabcwer", "afsabcabcd", "sfdeiekd", "einoejk", "kisfe",
        "safdwe", "zhuoXUE", "zhuoxue_yll", "zhuoxue%yll", ""};

    static String[] StringCHNS = {
        "中文带有数字1", "纯中文", "中文带有特殊字符*#￥%", "中文带有字母abc",
        "中文带有很多类型1a&", "中文带有下划线_", "1中文带有数字", "%*@@中文带有特殊字符", "bcd中文带有字母",
        "#ab1中文带有很多类型", "中文带 有空格", ""};

    public String varchar_testOneValue() {
        return StringS[3];
    }

    public String varchar_testTwoValue() {
        return StringS[4];
    }

    public String varchar_testRandom(String isNullValue) {
        if (pk.intValue() % 10 == 0) {
            return null;
        }
        return varchar_testRandom();
    }

    public String varchar_testRandom() {
        return random.nextBoolean() ? StringS[pk.intValue() % StringS.length] : UUID.randomUUID().toString();
    }

    static String[] charS = {
        "hello1234", "he343243", "word23", "feed32feed",
        "nihaore", "afdaewer", "hellorew", "abdfeed", "cdefeed",
        "adaabcwer", "afsabcabcd", "sfdeiekd", "einoejk", "kisfe",
        "safdwe", ""};

    public String char_testOneValue() {
        return charS[2];
    }

    public String char_testRandom() {
        return random.nextBoolean() ? StringS[pk.intValue() % StringS.length] : UUID.randomUUID().toString();
    }

    static String[] dateS = {
        "2012-12-13", "2013-04-05",
        "2015-11-23", "2010-02-22", "2015-12-02", "2014-05-26",
        "2011-12-23", "2003-04-05", "2013-02-05", "2013-09-02",
        "2017-03-22", "2011-06-22", "2013-03-22", "2014-02-12"};

    public String date_testRandom() {
        Date d = new Date(random.nextInt(1000000000) * 1000);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(d);
    }

    public String date_testOneValue() {
        return dateS[2];
    }

    public String date_testEndValue() {
        return dateS[dateS.length - 1];
    }

    public String date_testStartValue() {
        return dateS[0];
    }

    static String[] timeS = {
        "12:23:00", "11:23:45", "06:34:12", "08:02:45",
        "18:35:23", "15:23:34", "20:12:12", "12:12:12", "12:23:34",
        "12:27:32", "14:47:28", "07:47:28", "09:12:28", "09:17:28"};

    public String time_testRandom() {
        return timeS[pk.intValue() % timeS.length];
    }

    public String time_testOneValue() {
        return timeS[2];
    }

    static String[] timeStampS = {
        "2012-12-13 12:23:00", "2014-02-12 11:23:45",
        "2013-04-05 06:34:12", "2015-11-23 08:02:45",
        "2010-02-22 18:35:23", "2014-05-26 20:12:12", "2011-12-23 12:12:12",
        "2003-04-05 12:23:34", "2013-02-05 12:27:32",
        "2013-09-02 14:47:28", "2035-03-22 07:47:28",
        "2011-06-22 09:12:28", "2013-03-22 09:17:28", "2015-12-02 15:23:34",};

    public String timestamp_testRandom() {
        return timeStampS[pk.intValue() % timeStampS.length];
    }

    public String timestamp_testOneValue() {
        return timeStampS[2];
    }

    public String timestampStartValue() {
        return timeStampS[0];
    }

    public String timestampEndValue() {
        return timeStampS[timeStampS.length - 1];
    }

    public String datetime_testOneValue() {
        return timeStampS[2];
    }

    public String datetime_testRandom() {
        return timeStampS[pk.intValue() % timeStampS.length];
    }

    static Integer[] yearS = {
        2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019,
        2011, 2009};

    public Integer year_testOneValue() {
        return yearS[2];
    }

    public Integer year_testRandom() {
        return yearS[pk.intValue() % yearS.length];
    }

    public String binary_testRandom() {
        return charS[pk.intValue() % charS.length];
    }

    public String var_binary_testRandom() {
        return StringS[pk.intValue() % charS.length];
    }

    public String text_testRandom() {
        return StringS[pk.intValue() % charS.length];
    }
}
