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

package com.alibaba.polardbx.common.utils;

import java.util.Arrays;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 * 各种不同实现的比较测试
 *
 * @author linxuan
 */
public class TStringUtilTest {

    @Test
    public void testGetBetween() {
        Assert.assertEquals(TStringUtil.getBetween("wx[ b ]yz", "[", "]"), "b");
        Assert.assertEquals(TStringUtil.getBetween(null, "a", "a"), null);
        Assert.assertEquals(TStringUtil.getBetween("abc", null, "a"), null);
        Assert.assertEquals(TStringUtil.getBetween("abc", "a", null), null);
        Assert.assertEquals(TStringUtil.getBetween("", "", ""), "");
        Assert.assertEquals(TStringUtil.getBetween("", "", "]"), null);
        Assert.assertEquals(TStringUtil.getBetween("", "[", "]"), null);
        Assert.assertEquals(TStringUtil.getBetween("yabcz", "", ""), "");
        Assert.assertEquals(TStringUtil.getBetween("yabcz", "y", "z"), "abc");
        Assert.assertEquals(TStringUtil.getBetween("yabczyabcz", "y", "z"), "abc");
    }

    @Test
    public void testRemoveBetween() {
        Assert.assertEquals(TStringUtil.removeBetween("abc[xxx]bc", "[", "]"), "abc bc");
    }

    @Test
    public void testTwoPartSplit() {
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc:bc", ":"), new String[] {"abc", "bc:bc"});
        Assert.assertArrayEquals(TStringUtil.twoPartSplit(null, "a"), new String[] {null});
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc", "d"), new String[] {"abc:bc"});
        Assert.assertArrayEquals(TStringUtil.twoPartSplit("abc:bc", ";"), new String[] {"abc:bc"});
    }

    @Test
    public void testRecursiveSplit() {
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc:bc", ":"), Arrays.asList("abc", "bc", "bc"));
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc", "d"), Arrays.asList("abc:bc"));
        Assert.assertEquals(TStringUtil.recursiveSplit("abc:bc", ";"), Arrays.asList("abc:bc"));
    }

    @Test
    public void testFillTabWithSpace() {
        String sql =
            "   select sum(rate)      from                                                                          feed_receive_0117                                                            t             where       RATED_UID=?     and RATER_UID=?     and suspended=0 and validscore=1      and rater_type=?     and trade_closingdate>=?     and trade_closingdate<?     and id<>?        and (IFNULL(IMPORT_FROM, 0)&8) = 0        #@#mysql_feel_01#@#EXECUTE_A_SQL_TIMEOUT#@#1#@#484#@#484#@#484";
        String assertSql =
            "select sum(rate) from feed_receive_0117 t where RATED_UID=? and RATER_UID=? and suspended=0 and validscore=1 and rater_type=? and trade_closingdate>=? and trade_closingdate<? and id<>? and (IFNULL(IMPORT_FROM, 0)&8) = 0 #@#mysql_feel_01#@#EXECUTE_A_SQL_TIMEOUT#@#1#@#484#@#484#@#484";
        String acutulSql = null;
        acutulSql = TStringUtil.fillTabWithSpace(sql);
        Assert.assertEquals(assertSql, acutulSql);
    }

    @Test
    public void testRemoveBetweenWithSplitor() {
        String sql = "/*+UNIT_VALID({valid_key:123})*/select * from table";
        sql = TStringUtil.removeBetween(sql, "/*+UNIT_VALID(", ")*/");
        Assert.assertEquals(" select * from table", sql);
    }

    @Test
    public void testReplaceWithIgnoreCase() {
        String sql = "avg(a.id)";
        sql = TStringUtil.replaceWithIgnoreCase(sql, "avg(", "count(");
        Assert.assertEquals("count(a.id)", sql);
        sql = TStringUtil.replaceWithIgnoreCase(sql, "AVG(", "count(");
        Assert.assertEquals("count(a.id)", sql);

        sql = "count(a.id),avg(a.id),sum(a.id)";
        sql = TStringUtil.replaceWithIgnoreCase(sql, "AVG(", "count(");
        Assert.assertEquals("count(a.id),count(a.id),sum(a.id)", sql);
    }

    @Test
    public void testIsParsableNumber() {
        Assert.assertTrue(TStringUtil.isParsableNumber("-1"));
        Assert.assertTrue(TStringUtil.isParsableNumber("-1.00"));
        Assert.assertTrue(TStringUtil.isParsableNumber("10.0000"));
        Assert.assertTrue(TStringUtil.isParsableNumber("23424"));
        Assert.assertFalse(TStringUtil.isParsableNumber("0x16"));
        Assert.assertFalse(TStringUtil.isParsableNumber("1L"));
        Assert.assertFalse(TStringUtil.isParsableNumber("2afdaf"));
    }

    @Test
    public void testContainsIgnoreCase() {
        Assert.assertFalse(TStringUtil.containsIgnoreCase(null, "abc"));
        Assert.assertFalse(TStringUtil.containsIgnoreCase("abc", null));
        Assert.assertTrue(TStringUtil.containsIgnoreCase("", ""));
        Assert.assertTrue(TStringUtil.containsIgnoreCase("abc", ""));
        Assert.assertTrue(TStringUtil.containsIgnoreCase("abc", "a"));
        Assert.assertFalse(TStringUtil.containsIgnoreCase("abc", "z"));
        Assert.assertTrue(TStringUtil.containsIgnoreCase("abc", "A"));
        Assert.assertFalse(TStringUtil.containsIgnoreCase("abc", "Z"));

        String sql = "/*+UNIT_VALID({valid_key:123})*/select * from table";
        Assert.assertTrue(TStringUtil.containsIgnoreCase(sql, "SELECT"));
        Assert.assertTrue(TStringUtil.containsIgnoreCase(sql, "valid"));
        Assert.assertTrue(TStringUtil.containsIgnoreCase(sql, "123"));
        Assert.assertFalse(TStringUtil.containsIgnoreCase(sql, "else"));
    }

    @Test
    public void testHex2Int() {
        Random random = new Random();
        for (int i = 0; i < 100; ++i) {
            int value = random.nextInt();
            String hex = Integer.toHexString(value);
            int hex2Int = TStringUtil.hex2Int(hex);
            Assert.assertTrue(hex2Int == value);
        }
    }

    public static void main(String[] args) {
        String sql = "/*+UNIT_VALID({valid_key:123})*/select * from table";

        final int COUNT = 10000000;

        for (int i = 0; i < 10; i++) {
            long time = System.currentTimeMillis();

            for (int j = 0; j < COUNT; j++) {
                TStringUtil.containsIgnoreCase(sql, "SELECT");
            }

            time = System.currentTimeMillis() - time;
            System.out.println("COST: " + (COUNT * 1000 / time));
        }
    }
}
