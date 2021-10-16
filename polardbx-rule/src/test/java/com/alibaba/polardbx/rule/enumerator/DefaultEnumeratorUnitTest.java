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
import static com.alibaba.polardbx.rule.TestUtils.NotEquivalent;
import static com.alibaba.polardbx.rule.TestUtils.gand;
import static com.alibaba.polardbx.rule.TestUtils.gcomp;
import static com.alibaba.polardbx.rule.TestUtils.gor;
import static com.alibaba.polardbx.rule.TestUtils.testSet;

import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.exception.TddlRuleException;

/**
 * 对默认的枚举器做测试
 * 
 * @author shenxun
 */
public class DefaultEnumeratorUnitTest {

    Enumerator  dE                         = new RuleEnumeratorImpl();

    boolean     needMergeValueInCloseRange = true;
    Comparative beTestComparative          = null;

    // Test:getEnumeratedValue 测试等于 Or comparable null > <
    @Test
    public void testGetEnumeratedValue_等于() throws Exception {
        beTestComparative = gcomp(1, Equivalent);
        Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1 }, s);
    }

    @Test
    public void testGetEnumeratedValue_or() throws Exception {
        beTestComparative = gor(gor(gcomp(1, Equivalent), gcomp(2, Equivalent)), gcomp(3, Equivalent));
        Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1, 2, 3 }, s);
    }

    @Test
    public void testGetEnumberatedValue_comparable() throws Exception {
        Comparable c = 1;
        Set<Object> s = dE.getEnumeratedValue(c, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { 1 }, s);
    }

    @Test
    public void testGetEnumberatedValue_null() throws Exception {
        Set<Object> s = dE.getEnumeratedValue(null, null, null, needMergeValueInCloseRange);
        testSet(new Object[] { null }, s);
    }

    @Test
    public void testGetEnumberatedValue_GreaterThan() throws Exception {
        beTestComparative = gcomp(1, GreaterThan);
        try {
            Set<Object> s = dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
            System.out.println(s.size());
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals("在没有提供叠加次数的前提下，不能够根据当前范围条件选出对应的定义域的枚举值，sql中不要出现> < >= <=", e.getMessage());
        }
    }

    @Test
    public void testCloseInterval() throws Exception {
        beTestComparative = gand(gcomp(1, GreaterThan), gcomp(4, LessThan));
        try {
            dE.getEnumeratedValue(beTestComparative, null, null, needMergeValueInCloseRange);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("当原子增参数或叠加参数为空时，不支持在sql中使用范围选择，如id>? and id<?", e.getMessage());
        }
    }

    @Test
    public void testGetEnumeratedValue_不等于() throws Exception {
        beTestComparative = gand(gcomp(1, GreaterThan), gcomp(5, NotEquivalent));
        try {
            dE.getEnumeratedValue(beTestComparative, 10, 1, needMergeValueInCloseRange);
        } catch (TddlRuleException e) {
            // 暂时不支持
        }
    }
}
