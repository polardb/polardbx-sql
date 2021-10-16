/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;

import java.util.List;

public class Coronadb_hints_test_1_master_slave extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "/!TDDL:MASTER|SLAVE*/ select * from t";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*TDDL:MASTER|SLAVE*/\n" +
                "SELECT *\n" +
                "FROM t", stmt.toString());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
        assertEquals(0, visitor.getOrderByColumns().size());

        List<SQLCommentHint> hints = stmt.getHeadHintsDirect();

        TDDLHint hint = (TDDLHint) hints.get(0);
        assertEquals("MASTER", hint.getFunctions().get(0).getName());
        assertEquals("SLAVE", hint.getFunctions().get(1).getName());
    }

    public void test_1() throws Exception {
        String sql = "/!TDDL:MASTER()|SLAVE()*/ select * from t";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*TDDL:MASTER()|SLAVE()*/\n" +
                "SELECT *\n" +
                "FROM t", stmt.toString());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
        assertEquals(0, visitor.getOrderByColumns().size());

        List<SQLCommentHint> hints = stmt.getHeadHintsDirect();

        TDDLHint hint = (TDDLHint) hints.get(0);
        assertEquals("MASTER", hint.getFunctions().get(0).getName());
        assertEquals("SLAVE", hint.getFunctions().get(1).getName());
    }

    public void test_2() throws Exception {
        String sql = "/*+TDDL:MASTER*/select * from t";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL:MASTER*/\n" +
                "SELECT *\n" +
                "FROM t", stmt.toString());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
        assertEquals(0, visitor.getOrderByColumns().size());

        List<SQLCommentHint> hints = stmt.getHeadHintsDirect();

        TDDLHint hint = (TDDLHint) hints.get(0);
        assertEquals("MASTER", hint.getFunctions().get(0).getName());
    }

    public void test_3() throws Exception {
        String sql = "/*+TDDL:MASTER()*/select * from t";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL:MASTER()*/\n" +
                "SELECT *\n" +
                "FROM t", stmt.toString());

        MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
        stmt.accept(visitor);

        assertEquals(1, visitor.getTables().size());
        assertEquals(1, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());
        assertEquals(0, visitor.getOrderByColumns().size());

        List<SQLCommentHint> hints = stmt.getHeadHintsDirect();

        TDDLHint hint = (TDDLHint) hints.get(0);
        assertEquals("MASTER", hint.getFunctions().get(0).getName());
    }

    public void test_4() throws Exception {
        String sql = "/*+TDDL:MASTER and NODE IN('UPAY_1520935710844BPWAUPAY_PKSX_0007_RDS')*/update wallet set balance = balance + 2465, mtime = unix_timestamp(now())*1000 where id = '29b9a61a-a3cb-4294-a500-fc7bacd7ae3b'";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL:MASTER and NODE IN('UPAY_1520935710844BPWAUPAY_PKSX_0007_RDS')*/\n" +
                "UPDATE wallet\n" +
                "SET balance = balance + 2465, mtime = unix_timestamp(now()) * 1000\n" +
                "WHERE id = '29b9a61a-a3cb-4294-a500-fc7bacd7ae3b'", stmt.toString());
    }

    public void test_5() throws Exception {
        String sql = "/*+TDDL:master() and NODE IN('UPAY_1520935710844BPWAUPAY_PKSX_0007_RDS')*/update wallet set balance = balance + 2465, mtime = unix_timestamp(now())*1000 where id = '29b9a61a-a3cb-4294-a500-fc7bacd7ae3b'";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL:master() and NODE IN('UPAY_1520935710844BPWAUPAY_PKSX_0007_RDS')*/\n" +
                "UPDATE wallet\n" +
                "SET balance = balance + 2465, mtime = unix_timestamp(now()) * 1000\n" +
                "WHERE id = '29b9a61a-a3cb-4294-a500-fc7bacd7ae3b'", stmt.toString());
    }

    public void test_6() throws Exception {
        String sql = "/*+TDDL:socket_timeout=3000 and SLAVE*/select sum(ifnull(net_original,0)) as total_money,count(*) as num from `order` where status not in (0,3201,1300,1501) and ifnull(net_original,0)>0 and merchant_id = 'b29a44c0-87e9-4a9f-875b-533e87aea897' and ctime>= 1555689600000 and ctime < 1555775999000 and status != 0 and payway in (3) and ( sub_payway in (1,2) or (sub_payway = 3 and status not in (1300,1501)) or (sub_payway = 4 and status not in (1300,1501)) ) and ( operator = 'QRCODE:18072000299036955783' )";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*+TDDL:socket_timeout=3000 and SLAVE*/\n" +
                "SELECT sum(ifnull(net_original, 0)) AS total_money\n" +
                "\t, count(*) AS num\n" +
                "FROM `order`\n" +
                "WHERE status NOT IN (0, 3201, 1300, 1501)\n" +
                "\tAND ifnull(net_original, 0) > 0\n" +
                "\tAND merchant_id = 'b29a44c0-87e9-4a9f-875b-533e87aea897'\n" +
                "\tAND ctime >= 1555689600000\n" +
                "\tAND ctime < 1555775999000\n" +
                "\tAND status != 0\n" +
                "\tAND payway IN (3)\n" +
                "\tAND (sub_payway IN (1, 2)\n" +
                "\t\tOR (sub_payway = 3\n" +
                "\t\t\tAND status NOT IN (1300, 1501))\n" +
                "\t\tOR (sub_payway = 4\n" +
                "\t\t\tAND status NOT IN (1300, 1501)))\n" +
                "\tAND operator = 'QRCODE:18072000299036955783'", stmt.toString());
    }

    // /*TDDL:SLAVE AND MERGE_CONCURRENT=FALSE*/select id,member_id,password,telephone,nickname,wx_nickname,birth,out_platform,out_id,source,avatar,sex,teacher_id,teacher_bind_time,distinguish,open_id,is_new_open_id,mtag_id,platform,device_code,info,create_time,update_time from qukan.member_info where date(update_time)>='2019-04-18' and (1=1)
    public void test_7() throws Exception {
        String sql = "/*TDDL:SLAVE AND MERGE_CONCURRENT=FALSE*/select id,member_id,password,telephone,nickname,wx_nickname,birth,out_platform,out_id,source,avatar,sex,teacher_id,teacher_bind_time,distinguish,open_id,is_new_open_id,mtag_id,platform,device_code,info,create_time,update_time from qukan.member_info where date(update_time)>='2019-04-18' and (1=1)";

        MySqlStatementParser parser = new MySqlStatementParser(sql, SQLParserFeature.TDDLHint);
        List<SQLStatement> statementList = parser.parseStatementList();
        SQLStatement stmt = statementList.get(0);

        assertEquals(1, statementList.size());

        assertEquals("/*TDDL:SLAVE AND MERGE_CONCURRENT=FALSE*/\n" +
                "SELECT id, member_id, password, telephone, nickname\n" +
                "\t, wx_nickname, birth, out_platform, out_id, source\n" +
                "\t, avatar, sex, teacher_id, teacher_bind_time, distinguish\n" +
                "\t, open_id, is_new_open_id, mtag_id, platform, device_code\n" +
                "\t, info, create_time, update_time\n" +
                "FROM qukan.member_info\n" +
                "WHERE date(update_time) >= '2019-04-18'\n" +
                "\tAND 1 = 1", stmt.toString());
    }
}
