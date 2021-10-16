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
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import org.junit.Assert;
import org.junit.Test;

import com.alibaba.polardbx.common.model.sqljep.Comparative;

public class IntegerPartDiscontinousRangeEnumeratorUnitTest {

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
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 16, 64, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 4, 5 }, s);
    }

    // --------------------------------------------------以下是一些对两个and节点上挂两个参数一些情况的单元测试。
    // 因为从公共逻辑测试中已经测试了> 在处理中会转变为>= 而< 在处理中会转为<= 因此只需要测试> = <
    // 在and,两个节点的情况下的可能性即可。
    @Test
    public void test_带有自增的closeInterval_1() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 64, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 4, 5 }, s);
    }

    @Test
    public void test_足够大的一个范围选择函数_远远大于一个y的变动周期() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(1000, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 4, 5, 6, 7, 8 }, s);
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
        btc = TestUtils.gcomp(3, TestUtils.LessThanOrEqual);
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        // TestUtils.testSet(new Object[]{3,2,1,0,-1},s );
        TestUtils.testSet(new Object[] { 3, 2, 1, 0 }, s); // 默认忽略负数
    }

    @Test
    public void test_两个小于等于() throws Exception {
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.LessThanOrEqual), TestUtils.gcomp(5, TestUtils.LessThanOrEqual));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        // TestUtils.testSet(new Object[]{3,2,1,0,-1},s );
        TestUtils.testSet(new Object[] { 3, 2, 1, 0 }, s); // 默认忽略负数
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
        btc = TestUtils.gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 6, 7, 8, 9, 10 }, s);
    }

    // -------------------------------------or和and共同作用完成的功能。
    // T:主要目标是测试or+and范围的时候的枚举系统的表现。
    @Test
    public void test_X等于2OrX大于3AndX小于5() throws Exception {
        btc = TestUtils.gor(TestUtils.gcomp(1, TestUtils.Equivalent), TestUtils
            .gand(TestUtils.gcomp(3, TestUtils.GreaterThan), TestUtils.gcomp(5, TestUtils.LessThanOrEqual)));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 1, 4, 5 }, s);
    }

    @Test
    public void test_x在两个范围内同时两个范围之间是or关系() throws Exception {
        btc = TestUtils
            .gor(TestUtils.gand(TestUtils.gcomp(1, TestUtils.GreaterThan), TestUtils.gcomp(3, TestUtils.LessThanOrEqual)),
            TestUtils.gand(TestUtils.gcomp(5, TestUtils.GreaterThan), TestUtils.gcomp(7, TestUtils.LessThanOrEqual)));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 2, 3, 6, 7 }, s);
    }

    @Test
    public void test_x在一个是无界的而另一个是有界的_他们之间是or关系() throws Exception {
        // 这个条件下，系统只需要返回一组能够代表整个定义域内所有引起y变化的x的对应值的描点即可。
        // 因此不需要返回7这个数据。
        btc = TestUtils.gor(TestUtils.gcomp(1, TestUtils.GreaterThan), TestUtils
            .gand(TestUtils.gcomp(5, TestUtils.GreaterThan), TestUtils.gcomp(7, TestUtils.LessThanOrEqual)));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 2, 3, 4, 5, 6 }, s);
    }

    @Test
    public void test_x一边是无界的而另一边是一个and开区间_他们之间是or关系() throws Exception {

        btc = TestUtils.gor(TestUtils.gcomp(1, TestUtils.GreaterThan), TestUtils
            .gand(TestUtils.gcomp(5, TestUtils.LessThan), TestUtils.gcomp(7, TestUtils.GreaterThan)));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 2, 3, 4, 5, 6 }, s);
    }

    @Test
    public void test_复杂范围关系() throws Exception {
        btc = TestUtils
            .gor(TestUtils.gand(TestUtils.gcomp(1, TestUtils.GreaterThan), TestUtils.gcomp(3, TestUtils.LessThanOrEqual)),
            TestUtils.gor(
                TestUtils.gand(TestUtils.gcomp(8, TestUtils.GreaterThan), TestUtils.gcomp(10, TestUtils.LessThanOrEqual)),
                TestUtils.gand(TestUtils.gcomp(5, TestUtils.GreaterThan), TestUtils.gcomp(7, TestUtils.LessThan))));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 3, 2, 6, 9, 10 }, s);
    }

    // and左右条件中，至少有一个是 or 条件
    // id = 1 and ( id = 1 or id is null )
    @Test
    public void test_CompactiveAndWithBaseList_1() throws Exception {
        btc = TestUtils
            .gand(TestUtils.gor(TestUtils.gcomp(1, TestUtils.Equivalent), TestUtils.gcomp(null, TestUtils.Equivalent)), TestUtils
                .gcomp(1, TestUtils.Equivalent));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 1 }, s);
    }

    // and左右条件中，有一个是 or 条件, 有一个是开区间
    // id > 1 and ( id = 3 or id=2 )
    @Test
    public void test_CompactiveAndWithBaseList_2() throws Exception {
        btc = TestUtils
            .gand(TestUtils.gor(TestUtils.gcomp(3, TestUtils.Equivalent), TestUtils.gcomp(2, TestUtils.Equivalent)), TestUtils
                .gcomp(1, TestUtils.GreaterThan));
        Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
        TestUtils.testSet(new Object[] { 2, 3, 4, 5, 6 }, s);
    }

    // and左右条件中，有一个是 or 条件, 有一个是开区间
    // id = 1 and ( id = 3 or id is null )
    @Test
    public void test_CompactiveAndWithBaseList_3() throws Exception {
        btc = TestUtils
            .gand(TestUtils.gor(TestUtils.gcomp(3, TestUtils.Equivalent), TestUtils.gcomp(null, TestUtils.Equivalent)), TestUtils
                .gcomp(1, TestUtils.Equivalent));

        try {
            Set<Object> s = e.getEnumeratedValue(btc, 5, 1, needMergeValueInCloseRange);
            System.out.println(s);
        } catch (Exception e) {
            if (e instanceof TddlRuleException) {
                String msg = ((TddlRuleException) e).getMessage();
                Assert.assertTrue(msg.contains("Route : ComparativeAND leads to an empty enumeration set"));
            }
        }
    }

    // @Test
    // public void test_性能测试() throws Exception{
    // Comparative par = null;
    // for(int i = 0 ;i< 1000;i++){
    // par = gor(par,gcomp(i,Equivalent));
    // }
    // btc = par;
    // long time = System.currentTimeMillis();
    // for(int i = 0;i< 10000;i++){
    // e.getEnumeratedValue(btc, 5, 1);
    // }
    // System.out.println(System.currentTimeMillis()-time);
    // // TestUtils.testSet(new Object[]{3,2,6,9,10}, s);
    // }
    // 2000ms运行10000次，虽然比那边好点不过有限
    //

}
