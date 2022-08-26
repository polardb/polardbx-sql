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

package com.alibaba.polardbx.qatest.dql.sharding.parser;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import org.junit.Test;

/**
 * Created by fangwu on 2017/4/20.
 */
public class HintParserTest {

    @Test
    public void testHintInQuote() {
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");

        String sql = "select * from  testtable where id='/*+TDDL( node='xxx' )*/'";
        Assert.assertTrue(HintParser.getInstance().getTddlHint(ByteString.from(sql)) == null);
        sql = "select * from  testtable where id=\"/*+TDDL( node='xxx' )*/\"";
        Assert.assertTrue(HintParser.getInstance().getTddlHint(ByteString.from(sql)) == null);
        sql = "insert into ljltest_03 values(12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' ";
        Assert.assertTrue(HintParser.getInstance().getTddlHint(ByteString.from(sql)) == null);
    }

    private String[][] testSql = {
        {
            "/*+TDDL( node='xxx' )*/ select * from testtable", " node='xxx' ",
            null, null},
        {
            "/*+TDDL( node='xxx' )*/ /*+TDDL_GROUP({ xxdf ='erlk',kjdf[] , {} })*/ select * from testtable ",
            " node='xxx' ", "xxdf ='erlk',kjdf[] , {}", null},
        {
            "/*+TDDL( node='TDDL5_01_GROUP' )*/select * from ljltest_03;", " node='TDDL5_01_GROUP' ",
            null, null},
        {
            "/*TDDL: node='TDDL5_01_GROUP' */insert into ljltest_03 values(12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' */');",
            null, null, " node='TDDL5_01_GROUP' "},
        {
            "/!TDDL: node='TDDL5_01_GROUP' */  insert into ljltest_03 values(12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' */');",
            null, null, " node='TDDL5_01_GROUP' "},
        {
            "/*TDDL: node='TDDL5_01_GROUP' */ insert into ljltest_03 values(12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' */');",
            null, null, " node='TDDL5_01_GROUP' "},
        {
            "/*TDDL: node='TDDL5_01_GROUP' */ insert into ljltest_03 values(12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' */');",
            null, null, " node='TDDL5_01_GROUP' "},
        {
            "/*TDDL: node='TDDL5_01_GROUP' */ /*+TDDL_GROUP({ xxdf ='erlk',kjdf[] , {} })*/ insert into ljltest_03 values (12, 'xx', '/*+TDDL: node=\\'TDDL5_06_GROUP\\' */');",
            null, "xxdf ='erlk',kjdf[] , {}", " node='TDDL5_01_GROUP' "},
    };

    @Test
    public void testRemoveTDDLHint() {
        for (String[] sqls : testSql) {
            String sql = sqls[0];
            String sql2 = null;
            String sql1 = null;
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
            String tddlHint = HintParser.getInstance().getTddlHint(ByteString.from(sql));
            if (tddlHint != null && !"".equalsIgnoreCase(tddlHint)) {
                sql1 = HintParser.getInstance().removeTDDLHint(ByteString.from(sql)).toString();
            }

            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
            tddlHint = HintParser.getInstance().getTddlHint(ByteString.from(sql));
            if (tddlHint != null && !"".equalsIgnoreCase(tddlHint)) {
                sql2 = HintParser.getInstance().removeTDDLHint(ByteString.from(sql)).toString();
            }
//            System.out.println(ByteString.from(sql));
//            System.out.println(sql1);
//            System.out.println(sql2);
            Assert.assertTrue(sql1 == sql2 || sql1.equalsIgnoreCase(sql2));
        }
    }

    @Test
    public void testRemoveGroupHint() {
        for (String[] sqls : testSql) {
            String sql = sqls[0];
//            System.out.println("orinal sql:" + sql);
            String sql2 = null;
            String sql1 = null;
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
            String tddlHint = HintParser.getInstance().getTddlGroupHint(ByteString.from(sql));
            if (tddlHint != null && !"".equalsIgnoreCase(tddlHint)) {
                sql1 = HintParser.getInstance().removeGroupHint(ByteString.from(sql)).toString();
            }
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
            tddlHint = HintParser.getInstance().getTddlGroupHint(ByteString.from(sql));
            if (tddlHint != null && !"".equalsIgnoreCase(tddlHint)) {
                sql2 = HintParser.getInstance().removeGroupHint(ByteString.from(sql)).toString();
            }
//            System.out.println(sql1);
//            System.out.println(sql2);
            Assert.assertTrue(sql1 == sql2 || sql1.equalsIgnoreCase(sql2));

        }
    }

    @Test
    public void testReplaceSimpleHint() {
        for (String[] sqls : testSql) {
            String sql = sqls[0];
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
            String hint = HintParser.getInstance().getTddlSimpleHint(ByteString.from(sql));
            String sql2 = null;
            String sql1 = null;
            if (!TStringUtil.isEmpty(hint)) {
                String newHint = "xxx";
                sql1 = HintParser.getInstance().exchangeSimpleHint(hint, newHint, ByteString.from(sql)).toString();
            }
            System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
            hint = HintParser.getInstance().getTddlSimpleHint(ByteString.from(sql));
            if (!TStringUtil.isEmpty(hint)) {
                String newHint = "xxx";
                sql2 = HintParser.getInstance().exchangeSimpleHint(hint, newHint, ByteString.from(sql)).toString();

            }
//            System.out.println(sql1);
//            System.out.println(sql2);
            Assert.assertTrue(sql1 == sql2 || sql1.equalsIgnoreCase(sql2));

        }
    }

    @Test
    public void testAllSql() {
        for (String[] sql : testSql) {
//            System.out.println("start:" + sql[0] + ", " + sql[1] + ", " + sql[2] + ", " + sql[3]);
            testTDDLHintSql(sql[0], sql[1]);
            testTDDLGroupHintSql(sql[0], sql[2]);
            testTDDLSimpleHintSql(sql[0], sql[3]);
        }
    }

    private void testTDDLSimpleHintSql(String sql, String hint) {
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
        String newHint = HintParser.getInstance().getTddlSimpleHint(ByteString.from(sql));

        if (hint == null) {
            Assert.assertTrue(hint == newHint);
        } else {
//            System.out.println("end:" + sql + " | " + hint + " | " + newHint);
            assertHintEquals(hint, newHint);
        }
    }

    private void testTDDLGroupHintSql(String sql, String hint) {
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
        String newHint = HintParser.getInstance().getTddlGroupHint(ByteString.from(sql));
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
        String oldHint = HintParser.getInstance().getTddlGroupHint(ByteString.from(sql));

        assertHintEquals(newHint, oldHint);
        if (hint == null) {
            Assert.assertTrue(hint == newHint);
        } else {
            assertHintEquals(hint, newHint);
        }
    }

    public void testTDDLHintSql(String sql, String hint) {
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "true");
        String newHint = HintParser.getInstance().getTddlHint(ByteString.from(sql));
        System.setProperty(ConnectionProperties.HINT_PARSER_FLAG, "false");
        String oldHint = HintParser.getInstance().getTddlHint(ByteString.from(sql));

        assertHintEquals(newHint, oldHint);
        if (hint == null) {
            Assert.assertTrue(hint == newHint);
        } else {
            if (!hint.equalsIgnoreCase(newHint)) {
//                System.out.println(sql + "," + hint + "," + newHint);
            }
            assertHintEquals(hint, newHint);
//            System.out.println(sql + "," + hint + "," + newHint);
        }

    }

    private void assertHintEquals(String newHint, String oldHint) {
        if (newHint == null && oldHint == null) {
            return;
        }
        if (newHint == null || oldHint == null) {
            Assert.fail();
        }
        if (!newHint.trim().equalsIgnoreCase(oldHint.trim())) {
            Assert.fail();
        }
    }
}
