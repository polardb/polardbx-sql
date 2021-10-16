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

import static org.junit.Assert.assertEquals;

import java.util.Set;

import com.alibaba.polardbx.rule.TestUtils;
import org.junit.Test;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

public class LongPartDiscontinousRangeEnumeratorUnitTest {

    /*
     * T:测试在有自增和没有自增的情况下 对于close interval的处理， 在有自增和range的情况下测试 x = ? or (x > ?
     * and x < ?) 测试开区间 ，测试x>5 and x>10,测试x >= 3 and x < 5取值是否正确 测试x>3 and
     * x<5取值是否正确。 测试x >=3 and x=3的时候返回是否正确。
     */
    Comparative btc                        = null;

    Enumerator  e                          = new RuleEnumeratorImpl();
    boolean     needMergeValueInCloseRange = true;

    @Test
    public void test_没有自增的closeinterval() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.LessThanOrEqual));
        try {
            e.getEnumeratedValue(btc, null, null, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            assertEquals("当原子增参数或叠加参数为空时，不支持在sql中使用范围选择，如id>? and id<?", e.getMessage());
        }

    }

    @Test
    public void test_带有自增的closeInterval() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3l, TestUtils.GreaterThan), TestUtils.gcomp(5l, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 16, 64, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 4l, 5l }, s);
    }

    // --------------------------------------------------以下是一些对两个and节点上挂两个参数一些情况的单元测试。
    // 因为从公共逻辑测试中已经测试了> 在处理中会转变为>= 而< 在处理中会转为<= 因此只需要测试> = <
    // 在and,两个节点的情况下的可能性即可。
    @Test
    public void test_带有自增的closeInterval_1() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3l, TestUtils.GreaterThan), TestUtils.gcomp(5l, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 64, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 4l, 5l }, s);
    }

    @Test
    public void test_开区间() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.LessThanOrEqual), TestUtils.gcomp(5, TestUtils.GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个大于一个null在一个and中() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.LessThanOrEqual), null);
        try {
            e.getEnumeratedValue(btc, 64, 1, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            assertEquals("input value is not a comparative: null", e.getMessage());
        }
    }

    @Test
    public void test_一个小于() throws Exception {
        btc = TestUtils.gcomp(3l, TestUtils.LessThanOrEqual);
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        // TestUtils.testSet(new Object[]{3l,2l,1l,0l,-1l},s );//默认忽略负数
        TestUtils.testSet(new Object[] { 3l, 2l, 1l, 0l, }, s);
    }

    @Test
    public void test_两个小于等于() throws Exception {
        btc = TestUtils
            .gand(TestUtils.gcomp(3l, TestUtils.LessThanOrEqual), TestUtils.gcomp(5l, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        // TestUtils.testSet(new Object[]{3l,2l,1l,0l,-1l},s );//默认忽略负数
        TestUtils.testSet(new Object[] { 3l, 2l, 1l, 0l }, s);
    }

    @Test
    public void test_一个小于一个等于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.LessThanOrEqual), TestUtils.gcomp(5, TestUtils.Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个等于一个小于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.Equivalent), TestUtils.gcomp(5, TestUtils.LessThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 3 }, s);
    }

    @Test
    public void test_一个等于一个大于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.Equivalent), TestUtils.gcomp(5, TestUtils.GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_一个大于一个等于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 5 }, s);
    }

    @Test
    public void test_两个等于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.LessThanOrEqual), TestUtils.gcomp(5, TestUtils.Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] {}, s);
    }

    @Test
    public void test_两个大于不是大于等于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3l, TestUtils.GreaterThan), TestUtils.gcomp(5l, TestUtils.GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 6l, 7l, 8l, 9l, 10l }, s);
    }

}
