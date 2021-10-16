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

package com.alibaba.polardbx.rule.enumerator;

import static com.alibaba.polardbx.rule.TestUtils.Equivalent;
import static com.alibaba.polardbx.rule.TestUtils.GreaterThan;
import static com.alibaba.polardbx.rule.TestUtils.LessThan;
import static com.alibaba.polardbx.rule.TestUtils.LessThanOrEqual;
import static com.alibaba.polardbx.rule.TestUtils.gand;
import static com.alibaba.polardbx.rule.TestUtils.gcomp;
import static org.junit.Assert.assertEquals;

import java.util.Date;
import java.util.Set;

import org.junit.Test;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.TestUtils;

public class DatePartDiscontinousRangeEnumeratorUnitTest {

    /*
     * T:测试在有自增和没有自增的情况下 对于close interval的处理， 在有自增和range的情况下测试 x = ? or (x > ?
     * and x < ?) 测试开区间 ，测试x>5 and x>10,测试x >= 3 and x < 5取值是否正确 测试x>3 and
     * x<5取值是否正确。 测试x >=3 and x=3的时候返回是否正确。
     */
    Comparative btc                        = null;
    Enumerator  e                          = new RuleEnumeratorImpl();
    // @Before
    // public void setUp() throws Exception{
    // e.setNeedMergeValueInCloseInterval(true);
    // }
    boolean     needMergeValueInCloseRange = true;

    @Test
    public void test_没有自增的closeinterval() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), GreaterThan), gcomp(getDate(109, 02, 5), LessThanOrEqual));
        try {
            e.getEnumeratedValue(btc, null, null, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            assertEquals("当原子增参数或叠加参数为空时，不支持在sql中使用范围选择，如id>? and id<?", e.getMessage());
        }

    }

    @Test
    public void test_带有自增的closeInterval() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), GreaterThan), gcomp(getDate(109, 02, 5), LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 16, 64, needMergeValueInCloseRange);
        // 还在一个日期里，实际上是毫秒数+1了，变为表的时候是不会显示毫秒数的
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 3), getDate(109, 02, 05) }, s);
    }

    // --------------------------------------------------以下是一些对两个and节点上挂两个参数一些情况的单元测试。
    // 因为从公共逻辑测试中已经测试了> 在处理中会转变为>= 而< 在处理中会转为<= 因此只需要测试> = <
    // 在and,两个节点的情况下的可能性即可。
    @Test
    public void test_带有自增的closeInterval_1() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), GreaterThan), gcomp(getDate(109, 02, 5), LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 64, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 3), getDate(109, 02, 4), getDate(109, 02, 5) }, s);
    }

    @Test
    public void test_开区间() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), LessThanOrEqual), gcomp(getDate(109, 02, 5), GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个大于一个null在一个and中() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), LessThanOrEqual), null);
        try {
            e.getEnumeratedValue(btc, 64, 1, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            assertEquals("input value is not a comparative: null", e.getMessage());
        }
    }

    @Test
    public void test_一个小于等于() throws Exception {
        btc = gcomp(getDate(109, 02, 3), LessThanOrEqual);
        Set<Object> s = e.getEnumeratedValue(btc, 2, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 3), getDate(109, 02, 2) }, s);
    }

    @Test
    public void test_一个小于() throws Exception {
        btc = gcomp(getDate(109, 02, 3), LessThan);
        Set<Object> s = e.getEnumeratedValue(btc, 2, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getSubD(getDate(109, 02, 3)), getSubD(getDate(109, 02, 2)) }, s);
    }

    @Test
    public void test_两个小于等于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), LessThanOrEqual), gcomp(getDate(109, 02, 5), LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 2, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 3), getDate(109, 02, 2) }, s);
    }

    @Test
    public void test_一个小于一个等于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), LessThanOrEqual), gcomp(getDate(109, 02, 5), Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个等于一个小于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), Equivalent), gcomp(getDate(109, 02, 5), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 3) }, s);
    }

    @Test
    public void test_一个等于一个大于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), Equivalent), gcomp(getDate(109, 02, 5), GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个大于一个等于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), GreaterThan), gcomp(getDate(109, 02, 5), Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { getDate(109, 02, 5) }, s);
    }

    @Test
    public void test_两个等于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), LessThanOrEqual), gcomp(getDate(109, 02, 5), Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_两个大于不是大于等于() throws Exception {
        btc = gand(gcomp(getDate(109, 02, 3), GreaterThan), gcomp(getDate(109, 02, 5), GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 2, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(109, 02, 5), getDate(109, 02, 6) }, s);
    }

    /** -------------------------场景:10y2m5d ~10y2m7d.日期在关节点前中后，符号< <= > >= */

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7
    public void test1() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,6号中一个时间的
    public void test3() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6),
                getSubD(getDate(110, 02, 6, 12, 11, 10)) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6),
                getSubD(getDate(110, 02, 6, 12, 11, 10)) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,6号中一个时间的
    public void test4() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6),
                getSubD(getDate(110, 02, 6, 23, 59, 59)) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6),
                getSubD(getDate(110, 02, 6, 23, 59, 59)) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test5() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getSubD(getDate(110, 02, 7, 23, 59, 59)) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getSubD(getDate(110, 02, 7, 23, 59, 59)) }, s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test6() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getSubD(getDate(110, 02, 7, 00, 00, 01)) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getSubD(getDate(110, 02, 7, 00, 00, 01)) }, s);
    }

    /*----------第二轮 gmt> 10,2,5 变为gmt > 10,2,5 +1 ---------*/

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test7() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan), gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                new Date(1267891199999l) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                new Date(1267891199999l) }, s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test8() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan), gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 9) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 9) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test9() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan), gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test10() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan), gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test11() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan), gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7) },
            s);
    }

    /*---------------第三轮 gmt > 10,2,5 -一段时键 ----------------------*/

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test12() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5, 0, 0, 1).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                new Date(1267891199999l) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                new Date(1267891199999l) }, s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test13() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 9) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 9) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test14() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test15() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test16() throws Exception {
        // 会有两次比3月7号的被枚举出来，但实际上7号内的两个时间是不同的，一个0,0,0 ，一个23,59,59。目前暂时没有考虑去重的问题
        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7) },
            s);
    }

    /*---------------------第四种测试-------------------gmt >= 10,2,5 gmt < 7-------*/

    @Test
    // @T gmt>= 10,2,5 gmt< 10,2,7
    public void test17() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual), gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 gmt< 10,2,6号中一个时间的
    public void test18() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 9) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 9) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 gmt< 10,2,6号中一个时间的
    public void test19() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 gmt< 10,2,7号中一个时间的
    public void test20() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 gmt< 10,2,7号中一个时间的
    public void test21() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 00) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 00) },
            s);
    }

    /*----------第5轮 gmt>= 10,2,5 变为gmt > 10,2,5 +1 ---------*/

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,7
    public void test22() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test23() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 9) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 9) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test24() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test25() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test26() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 0) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 0) },
            s);
    }

    /*---------------第6轮 gmt > 10,2,5 -一段时键 ----------------------*/

    @Test
    // @T gmt>=10,2,5 +一段时间 gmt< 10,2,7
    public void test27() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                new Date(1267891199999l) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                new Date(1267891199999l) }, s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test28() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 12, 11, 9) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 12, 11, 9) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test29() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test30() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 23, 59, 58) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 23, 59, 58) },
            s);
    }

    @Test
    // @T gmt>= 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test31() throws Exception {
        // 会有两次比3月7号的被枚举出来，但实际上7号内的两个时间是不同的，一个0,0,0 ，一个23,59,59。目前暂时没有考虑去重的问题
        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7) },
            s);
    }

    /** ------------------第七轮 gmt > 110 02 05 ,gmt <= 110,02,07------------- */

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7
    public void test32() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan), gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,6号中一个时间的
    public void test33() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,6号中一个时间的
    public void test34() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test35() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test36() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), GreaterThan),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 01) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 01) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test37() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan),
            gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 2, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 2, 7) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test38() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 10) },
            s);

    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test39() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test40() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test41() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), GreaterThan),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test42() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test43() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test44() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test45() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test46() throws Exception {
        // 会有两次比3月7号的被枚举出来，但实际上7号内的两个时间是不同的，一个0,0,0 ，一个23,59,59。目前暂时没有考虑去重的问题
        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), GreaterThan),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 0, 0, 1) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 0, 0, 1) },
            s);
    }

    /** ------------------第八轮 gmt >= 110 02 05 ,gmt <= 110,02,07------------- */

    @Test
    // @T gmt>= 10,2,5 gmt<= 10,2,7
    public void test47() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7) }, s);
    }

    @Test
    // @T gmt>= 10,2,5 gmt<= 10,2,6号中一个时间的
    public void test48() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 12, 11, 10) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,6号中一个时间的
    public void test49() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test50() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 gmt< 10,2,7号中一个时间的
    public void test51() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 01) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5), getDate(110, 02, 6), getDate(110, 02, 7),
                getDate(110, 02, 7, 00, 00, 01) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test52() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test53() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 12, 11, 10) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test54() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test55() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1), getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test56() throws Exception {

        btc = gand(gcomp(getDate(110, 02, 5, 0, 0, 1), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 5, 0, 0, 1), getDate(110, 02, 6, 0, 0, 1),
                getDate(110, 02, 7, 0, 0, 1) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7
    public void test57() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的,getDate(1267891199999l)
    public void test58() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 12, 11, 10), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 12, 11, 10) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);

        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 12, 11, 10) },
            s);

    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,6号中一个时间的
    public void test59() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 6, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test60() throws Exception {

        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 23, 59, 59), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 23, 59, 59) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 23, 59, 59) },
            s);
    }

    @Test
    // @T gmt> 10,2,5 +一段时间 gmt< 10,2,7号中一个时间的
    public void test61() throws Exception {
        // 会有两次比3月7号的被枚举出来，但实际上7号内的两个时间是不同的，一个0,0,0 ，一个23,59,59。目前暂时没有考虑去重的问题
        btc = gand(gcomp(new Date(getDate(110, 02, 5).getTime() - 1l), TestUtils.GreaterThanOrEqual),
            gcomp(getDate(110, 02, 7, 00, 00, 01), TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 00, 0, 1) },
            s);

        s = e.getEnumeratedValue(btc, 7, 1, needMergeValueInCloseRange);
        TestUtils.testSetDate(new Date[] { getDate(110, 02, 4, 23, 59, 59), getDate(110, 02, 5, 23, 59, 59),
                getDate(110, 02, 6, 23, 59, 59), getDate(110, 02, 7, 00, 0, 1) },
            s);

    }

    private Date getSubD(Date date) {
        return new Date(date.getTime() - 1l);
    }

    @SuppressWarnings("unused")
    private Date getAddD(Date date) {
        return new Date(date.getTime() + 1l);
    }

    @SuppressWarnings("deprecation")
    private Date getDate(int year, int month, int date) {
        return new Date(year, month, date);
    }

    @SuppressWarnings("deprecation")
    public Date getDate(int year, int month, int date, int hrs, int min, int sec) {
        return new Date(year, month, date, hrs, min, sec);
    }
}
