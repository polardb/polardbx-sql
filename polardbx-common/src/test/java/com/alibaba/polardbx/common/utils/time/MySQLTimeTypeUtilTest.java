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

import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.stream.IntStream;

@Ignore
public class MySQLTimeTypeUtilTest extends TimeTestBase {
    // Timestamp, actual = 1582-10-21 18:30:19.264681, original = 1582-10-11 18:30:19.264681
    // Timestamp, actual = 1582-10-18 09:54:56.351, original = 1582-10-08 09:54:56.351
    // Timestamp, actual = 1582-10-23 10:10:59.154, original = 1582-10-13 10:10:59.154
    @Test
    public void test1582() {
        System.out.println(Timestamp.valueOf("1582-10-11 18:30:19.264681"));
        // 1582-10-21 18:30:19.264681
        System.out.println(Timestamp.valueOf("1582-10-08 09:54:56.351"));
        // 1582-10-18 09:54:56.351
        System.out.println(Timestamp.valueOf("1582-10-13 10:10:59.154"));
        // 1582-10-23 10:10:59.154
    }

    @Test
    public void testInvalidTime() {
        String datetime = "0000-00-00 01:01:01.003";
        Calendar calendar = new GregorianCalendar();
        Timestamp ts1 = MySQLTimeTypeUtil.bytesToDatetime(datetime.getBytes(), Types.TIMESTAMP, calendar, false, true);
        Assert.assertTrue(ts1.getClass() == OriginalTimestamp.class);
        Assert.assertTrue(ts1.toString().equals("0000-00-00 01:01:01.003"));

        Timestamp ts2 = MySQLTimeTypeUtil.bytesToDatetime(datetime.getBytes(), Types.TIMESTAMP, calendar, true, true);
        Assert.assertTrue(ts2.getClass() == Timestamp.class);
        Assert.assertTrue(ts2.toString().equals("0002-11-30 01:01:01.003"));
    }

    @Test
    public void testRandomTime() {
        Calendar calendar = new GregorianCalendar();
        IntStream.range(0, 1 << 20)
            // we have 1/3 prob to get random invalid timestamp
            .mapToObj(i -> R.nextInt() % 3 == 0 ? generateRandomDatetime() :
                generateStandardDatetime())
            .forEach(
                bs -> {
                    Timestamp t1 = MySQLTimeTypeUtil.bytesToDatetime(bs, Types.TIMESTAMP, calendar, false, true);
                    Timestamp t2 = MySQLTimeTypeUtil.bytesToDatetime(bs, Types.TIMESTAMP, calendar, true, true);
                    boolean eq = t1.toString().equals(t2.toString());
                    String errorMessage =
                        "original = " + new String(bs) + ", t1 = " + t1.toString() + ", t2 = " + t2.toString();
                    if (!(t1 instanceof OriginalTimestamp)) {
                        // If timestamp is not modified, they are equal to each other.
                        Assert.assertTrue(errorMessage, eq);
                    } else {
                        // If timestamp is modified, they are not equal to each other.
                        Assert.assertTrue(errorMessage, !eq);
                    }
                    // anyway, we must ensure the consistency of timestamp and original bytes
                    Assert.assertTrue(
                        t1.getClass().getSimpleName() + ", actual = " + t1.toString() + ", original = " + new String(
                            bs),
                        Arrays.equals(t1.toString().getBytes(), bs));
                }
            );
    }

    @Test
    public void testSingle() {
        String original = "0000-00-00 01:01:01";
        Calendar calendar = new GregorianCalendar();
        byte[] bs = original.getBytes();
        Timestamp t1 = MySQLTimeTypeUtil.bytesToDatetime(bs, Types.TIMESTAMP, calendar, false, true);
        Timestamp t2 = MySQLTimeTypeUtil.bytesToDatetime(bs, Types.TIMESTAMP, calendar, true, true);
        boolean eq = t1.toString().equals(t2.toString());
        if (!(t1 instanceof OriginalTimestamp)) {
            // If timestamp is not modified, they are equal to each other.
            Assert.assertTrue(eq);
        } else {
            // If timestamp is modified, they are not equal to each other.
            Assert.assertTrue(!eq);
        }
        // anyway, we must ensure the consistency of timestamp and original bytes
        Assert.assertTrue(
            t1.getClass().getSimpleName() + ", actual = " + t1.toString() + ", original = " + new String(bs),
            Arrays.equals(t1.toString().getBytes(), bs));
        System.out.println(t2.toString());
    }

    @Test
    public void testTimestamp() {
        String original = "0000-11-05 11:15:38.49513";
        Timestamp ts = Timestamp.valueOf(original);
    }
}
