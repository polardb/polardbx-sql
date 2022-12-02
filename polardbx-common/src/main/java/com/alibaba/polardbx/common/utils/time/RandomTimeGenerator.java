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

package com.alibaba.polardbx.common.utils.time;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.utils.time.parser.NumericTimeParser;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomTimeGenerator {
    public static final Random R = new Random();

    private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static boolean isZero() {
        return R.nextInt(10) % 4 == 5;
    }

    private static double randomDoubleInRange(long rangeMin, long rangeMax) {
        Preconditions.checkArgument(rangeMin <= rangeMax);
        while (true) {
            double d = rangeMin + (rangeMax - rangeMin) * R.nextDouble();

            DecimalFormat df = new DecimalFormat("#.000");
            String s = df.format(d);
            double ret = Double.valueOf(s);

            if (new BigDecimal(ret).toString().equals(s)) {
                return ret;
            }
        }
    }

    private static long randomIntegerInRange(long rangeMin, long rangeMax) {
        Preconditions.checkArgument(rangeMin <= rangeMax);
        return rangeMin + (int) (Math.random() * ((rangeMax - rangeMin) + 1));
    }

    public static List<Object> generateYear(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return R.nextInt(10000);
                    case 1:
                        return -R.nextInt();
                    default:
                        return R.nextInt();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateDay(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return R.nextInt(366);
                    case 1:
                        return -R.nextInt();
                    default:
                        return R.nextInt();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateHour(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return R.nextInt(MySQLTimeTypeUtil.TIME_MAX_HOUR);
                    case 1:
                        return -R.nextInt();
                    default:
                        return R.nextInt();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateMinute(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return R.nextInt(59);
                    case 1:
                        return -R.nextInt();
                    default:
                        return R.nextInt();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateSecond(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 5;
                    switch (r) {
                    case 0:
                        return R.nextInt(59);
                    case 1:
                        return -R.nextInt();
                    case 2:
                        return R.nextInt();
                    default:
                        return randomDoubleInRange(0L, 60L);
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateInvalidParam(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 4;
                    switch (r) {
                    case 0:
                        return null;
                    case 1:
                        return R.nextDouble();
                    default:
                        int len = R.nextInt(10);
                        StringBuilder builder = new StringBuilder();
                        for (int j = 0; j < len; j++) {
                            char ch = CHARS.charAt(R.nextInt(CHARS.length()));
                            builder.append(ch);
                        }
                        return builder.toString();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generatePeriod(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 2;
                    int y, m;
                    switch (r) {
                    case 0:
                        y = 1970 + R.nextInt(60);
                        m = R.nextInt(12) + 1;
                        return Long.valueOf(y + "" + m);

                    case 1:
                    default:
                        y = R.nextInt(9999);
                        m = R.nextInt(12) + 1;
                        return Long.valueOf(y + "" + m);
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateValidDatetimeNumeric(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 6;
                    switch (r) {
                    case 0:
                        return randomDoubleInRange(1, (70 - 1) * 10000L + 1231L);
                    case 1:
                        return randomDoubleInRange(70 * 10000L + 101L, 991231L);
                    case 2:
                        return randomDoubleInRange(10000101L, 99991231L);
                    case 3:
                        return randomDoubleInRange(101000000L, (70 - 1) * 10000000000L + 1231235959L);
                    case 4:
                        return randomDoubleInRange(70 * 10000000000L + 101000000L, 991231235959L);
                    default:
                        return randomDoubleInRange(1, 10000101000000L);
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateValidTimeNumeric(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                    case 1:
                        return randomDoubleInRange(-NumericTimeParser.TIME_MAX_VALUE, NumericTimeParser.TIME_MAX_VALUE);
                    default:
                        return randomDoubleInRange(1, 10000000000L);
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateValidTimestampString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return generateStandardTimestamp();
                    case 1:
                        return generateNoDelimiterStandardTimestamp();
                    case 2:
                    default:
                        return generateNonStandardTimestamp();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    /**
     * Generate the valid datetime string to MySQL server.
     */
    public static List<Object> generateValidDatetimeString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return generateNonStandardDateTime();
                    case 1:
                        return generateNoDelimiterStandardDatetime();
                    case 2:
                    default:
                        return generateStandardDatetime();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    /**
     * Generate the nullable datetime string to MySQL server.
     */
    public static List<Object> generateNullableDatetimeString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 4;
                    switch (r) {
                    case 0:
                        return generateNonStandardDateTime();
                    case 1:
                        return generateNoDelimiterStandardDatetime();
                    case 2:
                        return generateStandardDatetime();
                    case 3:
                    default:
                        return null;
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateValidTimeString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 3;
                    switch (r) {
                    case 0:
                        return generateNonStandardTime();
                    case 1:
                        return generateNoDelimiterStandardTime();
                    case 2:
                    default:
                        return generateStandardTime();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateDatetimeString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 4;
                    switch (r) {
                    case 0:
                        return generateRandomDatetime();
                    case 1:
                        return generateTruncatedDatetime();
                    case 2:
                        return generateNonStandardDateTime();
                    case 3:
                    default:
                        return generateStandardDatetime();
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static List<Object> generateTimeString(int n) {
        return IntStream.range(0, n)
            .mapToObj(
                i -> {
                    final int r = Math.abs(R.nextInt()) % 4;
                    if (R.nextInt() % 3 == 0) {
                        switch (r) {
                        case 0:
                            return generateRandomDatetime();
                        case 1:
                            return generateTruncatedDatetime();
                        case 2:
                            return generateNonStandardDateTime();
                        case 3:
                        default:
                            return generateStandardDatetime();
                        }
                    } else {
                        switch (r) {
                        case 0:
                            return generateRandomTime();
                        case 1:
                            return generateTruncatedTime();
                        case 2:
                            return generateNonStandardTime();
                        case 3:
                        default:
                            return generateStandardTime();
                        }
                    }
                }
            )
            .collect(Collectors.toList());
    }

    public static String generateStandardTime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();

        hour = Optional.of(R.nextInt(24))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        builder.append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();

    }

    public static String generateNoDelimiterStandardTime() {
        String time = generateStandardTime();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < time.length(); i++) {
            if (time.charAt(i) == ':') {
                continue;
            }
            builder.append(time.charAt(i));
        }
        return builder.toString();
    }

    public static String generateNonStandardTime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();

        hour = Optional.of(R.nextInt(24))
            .map(String::valueOf)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        builder.append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateTruncatedTime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();

        hour = Optional.of(R.nextInt(24))
            .map(String::valueOf)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        return Optional.ofNullable(builder)
            .map(b -> b.append(hour))
            .filter(b -> R.nextInt() % 5 != 0)
            .map(b -> b.append(':'))
            .map(b -> b.append(min))
            .filter(b -> R.nextInt() % 4 != 0)
            .map(b -> b.append(':'))
            .map(b -> b.append(sec))
            .filter(b -> R.nextInt() % 2 != 0)
            .map(b -> b.append('.'))
            .map(b -> {
                int fraction = Math.abs(R.nextInt()) % 6 + 1;
                for (int i = 0; i < fraction; i++) {
                    if (i == fraction - 1) {

                        b.append(R.nextInt(9) + 1);
                    } else {
                        b.append(R.nextInt(10));
                    }
                }
                return b;
            })
            .map(StringBuilder::toString)
            .orElseGet(
                () -> builder.toString()
            );
    }

    public static String generateRandomTime() {
        StringBuilder builder = new StringBuilder();

        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(':');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(':');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateStandardDatetime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();
        year = isZero()
            ? "0000"
            : Optional.of(R.nextInt(9999))
            .map(Math::abs)
            .map(i -> i == 1582 - 1 ? 2000 : i + 1)
            .map(
                i -> {
                    if (i < 10) {
                        return "000" + i;
                    } else if (i < 100) {
                        return "00" + i;
                    } else if (i < 1000) {
                        return "0" + i;
                    } else {
                        return "" + i;
                    }
                }
            )
            .get();

        mon = isZero()
            ? "00"
            : Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        day = isZero()
            ? "00"
            : (
            mon.equals("00")
                ? Optional.of(R.nextInt(31))
                .map(Math::abs)
                .map(i -> i + 1)
                .map(i -> i >= 10 ? "" + i : "0" + i)
                .get()
                : Optional.ofNullable(MySQLTimeTypeUtil.DAYS_IN_MONTH[Integer.valueOf(mon) - 1])
                .map(d -> R.nextInt(d))
                .map(Math::abs)
                .map(i -> i + 1)
                .map(i -> i >= 10 ? "" + i : "0" + i)
                .get());

        hour = Optional.of(R.nextInt(24))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        builder.append(year)
            .append('-')
            .append(mon)
            .append('-')
            .append(day)
            .append(' ')
            .append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateNoDelimiterStandardDatetime() {
        String datetime = generateStandardDatetime();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < datetime.length(); i++) {
            char ch = datetime.charAt(i);
            if (ch == ':' || ch == ' ' || ch == '-') {
                continue;
            }
            builder.append(datetime.charAt(i));
        }
        return builder.toString();
    }

    public static String generateNonStandardDateTime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();
        year = isZero()
            ? "0"
            : Optional.of(R.nextInt(9999))
            .map(Math::abs)
            .map(i -> i == 1582 - 1 ? 2000 : i + 1)
            .map(
                String::valueOf
            )
            .get();

        mon = isZero()
            ? "00"
            : Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        day = isZero()
            ? "00"
            : Optional.of(R.nextInt(MySQLTimeTypeUtil.DAYS_IN_MONTH[Integer.valueOf(mon) - 1]))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        hour = Optional.of(R.nextInt(24))
            .map(String::valueOf)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        builder.append(year)
            .append('-')
            .append(mon)
            .append('-')
            .append(day)
            .append(' ')
            .append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateTruncatedDatetime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();
        year = isZero()
            ? "0"
            : Optional.of(R.nextInt(9999))
            .map(Math::abs)
            .map(i -> i == 1582 - 1 ? 2000 : i + 1)
            .map(
                String::valueOf
            )
            .get();

        mon = isZero()
            ? "00"
            : Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        day = isZero()
            ? "00"
            : Optional.of(R.nextInt(31))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        hour = Optional.of(R.nextInt(24))
            .map(String::valueOf)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        return Optional.ofNullable(builder)
            .map(b -> b.append(year))
            .filter(b -> R.nextInt() % 10 != 0)
            .map(b -> b.append('-'))
            .map(b -> b.append(mon))
            .filter(b -> R.nextInt() % 9 != 0)
            .map(b -> b.append('-'))
            .map(b -> b.append(day))
            .filter(b -> R.nextInt() % 7 != 0)
            .map(b -> b.append(' '))
            .map(b -> b.append(hour))
            .filter(b -> R.nextInt() % 5 != 0)
            .map(b -> b.append(':'))
            .map(b -> b.append(min))
            .filter(b -> R.nextInt() % 4 != 0)
            .map(b -> b.append(':'))
            .map(b -> b.append(sec))
            .filter(b -> R.nextInt() % 2 != 0)
            .map(b -> b.append('.'))
            .map(b -> {
                int fraction = Math.abs(R.nextInt()) % 6 + 1;
                for (int i = 0; i < fraction; i++) {
                    if (i == fraction - 1) {

                        b.append(R.nextInt(9) + 1);
                    } else {
                        b.append(R.nextInt(10));
                    }
                }
                return b;
            })
            .map(StringBuilder::toString)
            .orElseGet(
                () -> builder.toString()
            );
    }

    public static String generateRandomDatetime() {
        StringBuilder builder = new StringBuilder();
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append('-');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append('-');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(' ');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(':');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));
        builder.append(':');
        builder.append(R.nextInt(10));
        builder.append(R.nextInt(10));

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateStandardTimestamp() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();

        year = isZero()
            ? "0000"
            : Optional.of(R.nextInt(66))
            .map(Math::abs)
            .map(i -> i + 1971)
            .map(
                i -> {
                    if (i < 10) {
                        return "000" + i;
                    } else if (i < 100) {
                        return "00" + i;
                    } else if (i < 1000) {
                        return "0" + i;
                    } else {
                        return "" + i;
                    }
                }
            )
            .get();

        mon = isZero()
            ? "00"
            : Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        day = isZero()
            ? "00"
            : (
            mon.equals("00")
                ? Optional.of(R.nextInt(31))
                .map(Math::abs)
                .map(i -> i + 1)
                .map(i -> i >= 10 ? "" + i : "0" + i)
                .get()
                : Optional.ofNullable(MySQLTimeTypeUtil.DAYS_IN_MONTH[Integer.valueOf(mon) - 1])
                .map(d -> R.nextInt(d))
                .map(Math::abs)
                .map(i -> i + 1)
                .map(i -> i >= 10 ? "" + i : "0" + i)
                .get());

        hour = Optional.of(R.nextInt(24))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        builder.append(year)
            .append('-')
            .append(mon)
            .append('-')
            .append(day)
            .append(' ')
            .append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }

    public static String generateNoDelimiterStandardTimestamp() {
        String timestamp = generateStandardTimestamp();
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < timestamp.length(); i++) {
            char ch = timestamp.charAt(i);
            if (ch == ':' || ch == ' ' || ch == '-') {
                continue;
            }
            builder.append(timestamp.charAt(i));
        }
        return builder.toString();
    }

    public static String generateNonStandardTimestamp() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();

        year = isZero()
            ? "0"
            : Optional.of(R.nextInt(66))
            .map(Math::abs)
            .map(i -> i + 1971)
            .map(
                String::valueOf
            )
            .get();

        mon = isZero()
            ? "00"
            : Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        day = isZero()
            ? "00"
            : Optional.of(R.nextInt(MySQLTimeTypeUtil.DAYS_IN_MONTH[Integer.valueOf(mon) - 1]))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(String::valueOf)
            .get();

        hour = Optional.of(R.nextInt(24))
            .map(String::valueOf)
            .get();

        min = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        sec = Optional.of(R.nextInt(60))
            .map(String::valueOf)
            .get();

        builder.append(year)
            .append('-')
            .append(mon)
            .append('-')
            .append(day)
            .append(' ')
            .append(hour)
            .append(':')
            .append(min)
            .append(':')
            .append(sec);

        int fraction = Math.abs(R.nextInt()) % 6 + 1;

        builder.append('.');
        for (int i = 0; i < fraction; i++) {
            if (i == fraction - 1) {

                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString();
    }
}
