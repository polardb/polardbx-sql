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

package com.alibaba.polardbx.optimizer.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Date;
import java.util.List;

/**
 * @author busu
 * date: 2020/10/29 5:31 下午
 */
public class SqlKeywordMatchUtilsTest {

    @Test
    public void test1() {
        String sql = " select * from sqlkeywordtest";
        Parameters params = new Parameters(Maps.newHashMap());
        List<String> keywords = Lists.newArrayList();
        SqlKeywordMatchUtils.MatchResult result =
            SqlKeywordMatchUtils
                .matchKeywords(sql, params, keywords, keywords.stream().mapToInt(String::length).sum());
        Assert.assertTrue(result.matched);
        Assert.assertFalse(result.matchParam);
    }

    @Test
    public void test2() {
        String sql = " select * from sqlkeywordtest";
        Parameters params = new Parameters(Maps.newHashMap());
        List<String> keywords = Lists.newArrayList("select", "from");
        SqlKeywordMatchUtils.MatchResult result =
            SqlKeywordMatchUtils
                .matchKeywords(sql, params, keywords, keywords.stream().mapToInt(String::length).sum());
        Assert.assertTrue(result.matched);
        Assert.assertFalse(result.matchParam);
    }

    @Test
    public void test3() {
        String sql = "select * from busu where id = ?";
        Parameters params = new Parameters(Maps.newHashMap());
        ParameterContext parameterContext =
            new ParameterContext(ParameterMethod.setString, new Object[] {1, "dingfeng"});
        params.getCurrentParameter().put(1, parameterContext);
        List<String> keywords = Lists.newArrayList("select", "busU", "dingfeng");
        SqlKeywordMatchUtils.MatchResult result =
            SqlKeywordMatchUtils
                .matchKeywords(sql, params, keywords, keywords.stream().mapToInt(String::length).sum());
        Assert.assertTrue(result.matched);
        Assert.assertTrue(result.matchParam);
    }

    @Test
    public void test4() {
        String sql = "select b.kk, b.mm from busu b join hhh h on b.oo=h.bb where b.ii = ? and b.pp =?";
        Parameters params = new Parameters(Maps.newHashMap());
        ParameterContext parameterContext1 =
            new ParameterContext(ParameterMethod.setDate1, new Object[] {1, Date.valueOf("2020-11-11")});
        params.getCurrentParameter().put(1, parameterContext1);

        ParameterContext parameterContext2 =
            new ParameterContext(ParameterMethod.setDate1, new Object[] {2, Date.valueOf("2020-11-12")});
        params.getCurrentParameter().put(2, parameterContext2);

        List<String> keywords = Lists.newArrayList("select", "busu", "join", "2020-11-11", "2020-11-12");
        SqlKeywordMatchUtils.MatchResult result =
            SqlKeywordMatchUtils.matchKeywords(sql, params, keywords, keywords.stream().mapToInt(String::length).sum());
        Assert.assertTrue(result.matched);
        Assert.assertTrue(result.matchParam);

    }

    @Test
    public void fetchKeywordTest1() {
        String sql = "select * from `busu` where id = '1' and name = 'dingfeng' ";
        List<String> expectList = Lists
            .newArrayList("select", "*", "from", "`busu`", "where", "id", "=", "'1'", "and", "name", "=", "'dingfeng'");
        List<String> actualList = SqlKeywordMatchUtils.fetchWords(sql);
        Assert.assertEquals(expectList, actualList);
    }

}
