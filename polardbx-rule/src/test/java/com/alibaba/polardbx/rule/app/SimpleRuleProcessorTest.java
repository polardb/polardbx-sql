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

package com.alibaba.polardbx.rule.app;

import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.utils.SimpleRuleProcessor;
import com.alibaba.polardbx.rule.utils.SimpleRuleProcessor.SimpleRule;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;

/**
 * author: arnkore 2016-08-09
 */
@RunWith(Enclosed.class)
public class SimpleRuleProcessorTest {

    @RunWith(Parameterized.class)
    public static class ComponentParameterizedTest {

        @Parameterized.Parameters(name = "{index}:match={0}, column={1}")
        public static Collection<Object[]> initParamsData() {
            Object[][] objss = new Object[][] {
                {true, "user_id"}, {true, "product_id"}, {true, "中文测试"},
                {true, "你好abc123_-"}, {true, "89998"}, {true, "嗯哼"},

                {false, "abcやろ"}, {false, "user,"}, {false, "abcω"},
                {false, "中文."}, {false, "user_id+"}, {false, "╭(╯^╰)╮"}};
            return Arrays.asList(objss);
        }

        private boolean match;

        private String column;

        public ComponentParameterizedTest(boolean match, String column) {
            this.match = match;
            this.column = column;
        }

        private String formatExpr(String rawExpr) {
            return MessageFormat.format(rawExpr, column);
        }

        private void assertRule(SimpleRule rule, AtomIncreaseType type, int dbCount, int tbCount) {
            if (match) {
                Assert.assertNotNull(rule);
                Assert.assertEquals(column, rule.column);
                Assert.assertEquals(type, rule.type);
                Assert.assertEquals(tbCount, rule.tbCount);
                Assert.assertEquals(dbCount, rule.dbCount);
            } else {
                Assert.assertNull(rule);
            }
        }

        private void parseDb(String rawExpr, AtomIncreaseType type, int dbCount, int tbCount) {
            SimpleRule rule = SimpleRuleProcessor.parseDB(formatExpr(rawExpr));
            assertRule(rule, type, dbCount, tbCount);
        }

        private void parseTb(String rawExpr, AtomIncreaseType type, int dbCount, int tbCount) {
            SimpleRule rule = SimpleRuleProcessor.parseTB(formatExpr(rawExpr));
            assertRule(rule, type, dbCount, tbCount);
        }

        @Test
        public void testParse() {
            /**
             * Db rule parseDb
             */
            // Db number rule
            parseDb("(#{0},1,1024#.longValue() % 1024).intdiv(16)", AtomIncreaseType.NUMBER, 16, 1024);
            // Db number abs rule
            parseDb("(#{0},1,1024#.longValue().abs() % 1024).intdiv(16)", AtomIncreaseType.NUMBER_ABS, 16, 1024);
            // Db string rule
            parseDb("((#{0},1,1024#).hashCode().abs().longValue() % 1024).intdiv(16)",
                AtomIncreaseType.STRING,
                16,
                1024);
            parseDb("(#{0},1,48#.hashCode().abs().longValue() % 48).intdiv(1)", AtomIncreaseType.STRING, 1, 48);
            parseDb("(#{0},1,8#.hashCode().abs().longValue() % 8).intdiv(1)", AtomIncreaseType.STRING, 1, 8);
            // Db number special date rule
            Assert.assertEquals(match,
                SimpleRuleProcessor.isSpecialDateMethod(
                    MessageFormat.format("(mmdd_i(#{0},1_date,1024#).longValue() % 1024).intdiv(16)", column)));

            /**
             * tb rule parseDb
             */
            // Tb number rule
            parseTb("(#{0},1,1024#.longValue() % 1024)", AtomIncreaseType.NUMBER, 1, 1024);
            // Tb number abs rule
            parseTb("(#{0},1,8#.longValue().abs() % 8)", AtomIncreaseType.NUMBER_ABS, 1, 8);
            // Tb string rule
            parseTb("((#{0},1,1024#).hashCode().abs().longValue() %  1024) ", AtomIncreaseType.STRING, 1, 1024);
            parseTb(" ((#{0},1,36#).hashCode().abs().longValue()%36)", AtomIncreaseType.STRING, 1, 36);
            parseTb(" #{0},1,256#.hashCode().abs().longValue() %  256 ", AtomIncreaseType.STRING, 1, 256);
            // Tb number special date rule
            Assert.assertEquals(match,
                SimpleRuleProcessor
                    .isSpecialDateMethod(MessageFormat.format("mmdd_i(#{0},1_date,1024#).longValue() % 1024", column)));
        }
    }

    public static class ComponentSingleTest {

        @Test
        public void testNumberShard() {
            TableRule table = new TableRule();
            table.setDbRules("((#user_id,1,1024#).longValue() % 1024).intdiv(16)");
            table.setTbRules("((#user_id,1,1024#).longValue() % 1024)");
            table.setDbNamePattern("user_group_{0000}");
            table.setTbNamePattern("user_{0000}");
            table.init();

            TargetDB targetDB = SimpleRuleProcessor.shard(table, 1);
            Assert.assertEquals("user_group_0000", targetDB.getDbIndex());
            Assert.assertEquals("user_0001", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, 17);
            Assert.assertEquals("user_group_0001", targetDB.getDbIndex());
            Assert.assertEquals("user_0017", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, null);
            Assert.assertEquals("user_group_0000", targetDB.getDbIndex());
            Assert.assertEquals("user_0000", targetDB.getTableNameMap().keySet().iterator().next());
        }

        @Test
        public void testChineseNumberShard() {
            TableRule table = new TableRule();
            table.setDbRules("((#你好abc,1,1024#).longValue() % 1024).intdiv(16)");
            table.setTbRules("((#你好abc,1,1024#).longValue() % 1024)");
            table.setDbNamePattern("用户_group_{0000}");
            table.setTbNamePattern("用户_{0000}");
            table.init();

            TargetDB targetDB = SimpleRuleProcessor.shard(table, 1);
            Assert.assertEquals("用户_group_0000", targetDB.getDbIndex());
            Assert.assertEquals("用户_0001", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, 17);
            Assert.assertEquals("用户_group_0001", targetDB.getDbIndex());
            Assert.assertEquals("用户_0017", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, null);
            Assert.assertEquals("用户_group_0000", targetDB.getDbIndex());
            Assert.assertEquals("用户_0000", targetDB.getTableNameMap().keySet().iterator().next());
        }

        @Test
        public void testStringShard() {
            TableRule table = new TableRule();
            table.setDbRules("((#user_id,1,1024#).hashCode().abs().longValue() % 1024).intdiv(16)");
            table.setTbRules("((#user_id,1,1024#).hashCode().abs().longValue() % 1024)");
            table.setDbNamePattern("user_group_{0000}");
            table.setTbNamePattern("user_{0000}");
            table.init();

            TargetDB targetDB = SimpleRuleProcessor.shard(table, "1");
            Assert.assertEquals("user_group_0003", targetDB.getDbIndex());
            Assert.assertEquals("user_0049", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, "17");
            Assert.assertEquals("user_group_0034", targetDB.getDbIndex());
            Assert.assertEquals("user_0550", targetDB.getTableNameMap().keySet().iterator().next());

            // "0".hashCode() -> 48
            Assert.assertEquals(48, "0".hashCode());
            targetDB = SimpleRuleProcessor.shard(table, null);
            Assert.assertEquals("user_group_0003", targetDB.getDbIndex());
            Assert.assertEquals("user_0048", targetDB.getTableNameMap().keySet().iterator().next());
        }

        @Test
        public void testNegativeHashShard() {
            TableRule table = new TableRule();
            table.setDbRules("((#user_id,1,1023#).hashCode().abs().longValue() % 1023).intdiv(16)");
            table.setTbRules("((#user_id,1,1023#).hashCode().abs().longValue() % 1023)");
            table.setDbNamePattern("user_group_{0000}");
            table.setTbNamePattern("user_{0000}");
            table.init();

            // "08:5d:dd:50:30:12".hashCode() -> -2147483648
            TargetDB targetDB = SimpleRuleProcessor.shard(table, "08:5d:dd:50:30:12");

            Assert.assertEquals("user_group_0000", targetDB.getDbIndex());
            Assert.assertEquals("user_0002", targetDB.getTableNameMap().keySet().iterator().next());
        }

        @Test
        public void testChineseStringShard() {
            TableRule table = new TableRule();
            table.setDbRules("((#用户id,1,1024#).hashCode().abs().longValue() % 1024).intdiv(16)");
            table.setTbRules("((#用户id,1,1024#).hashCode().abs().longValue() % 1024)");
            table.setDbNamePattern("用户_group_{0000}");
            table.setTbNamePattern("用户_{0000}");
            table.init();

            TargetDB targetDB = SimpleRuleProcessor.shard(table, "1");
            Assert.assertEquals("用户_group_0003", targetDB.getDbIndex());
            Assert.assertEquals("用户_0049", targetDB.getTableNameMap().keySet().iterator().next());

            targetDB = SimpleRuleProcessor.shard(table, "17");
            Assert.assertEquals("用户_group_0034", targetDB.getDbIndex());
            Assert.assertEquals("用户_0550", targetDB.getTableNameMap().keySet().iterator().next());

            // "0".hashCode() -> 48
            Assert.assertEquals(48, "0".hashCode());
            targetDB = SimpleRuleProcessor.shard(table, null);
            Assert.assertEquals("用户_group_0003", targetDB.getDbIndex());
            Assert.assertEquals("用户_0048", targetDB.getTableNameMap().keySet().iterator().next());
        }
    }
}
