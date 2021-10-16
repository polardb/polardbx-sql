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

import java.util.Optional;
import java.util.Random;

public class TimeTestBase {
    public static final Random R = new Random();

    public static byte[] generateStandardDatetime() {
        String year, mon, day, hour, min, sec;

        StringBuilder builder = new StringBuilder();
        year = Optional.of(R.nextInt(9999))
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

        mon = Optional.of(R.nextInt(12))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        day = Optional.ofNullable(MySQLTimeTypeUtil.DAYS_IN_MONTH[Integer.valueOf(mon) - 1])
            .map(d -> R.nextInt(d))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        hour = Optional.of(R.nextInt(23))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        min = Optional.of(R.nextInt(59))
            .map(Math::abs)
            .map(i -> i + 1)
            .map(i -> i >= 10 ? "" + i : "0" + i)
            .get();

        sec = Optional.of(R.nextInt(59))
            .map(Math::abs)
            .map(i -> i + 1)
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
                // avoid to end up with '0'
                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString().getBytes();
    }

    /**
     * generate datetime like 'yyyy-mm-dd hh:mm:ss[.fffffffff]'
     */
    public static byte[] generateRandomDatetime() {
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
                // avoid to end up with '0'
                builder.append(R.nextInt(9) + 1);
            } else {
                builder.append(R.nextInt(10));
            }
        }

        return builder.toString().getBytes();
    }
}
