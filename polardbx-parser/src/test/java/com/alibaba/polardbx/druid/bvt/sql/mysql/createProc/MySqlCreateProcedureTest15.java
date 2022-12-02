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
package com.alibaba.polardbx.druid.bvt.sql.mysql.createProc;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.MysqlTest;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;

import java.util.List;

public class MySqlCreateProcedureTest15 extends MysqlTest {

    public void test_0() throws Exception {
        String sql = "CREATE DEFINER=`root`@`%` PROCEDURE `load_part_tab`()\n" +
            "BEGIN \n" +
            "DECLARE v INT DEFAULT 0; \n" +
            "WHILE v < 1 DO \n" +
            "INSERT INTO part_tab \n" +
            "VALUES (v,'testing partitions',ADDDATE('1995-01-01',(RAND(v)*36520) MOD 3652)); \n" +
            "SET v = v + 1; \n" +
            "END WHILE; \n" +
            "END";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLStatement stmt = statementList.get(0);
//    	print(statementList);
        assertEquals(1, statementList.size());

        System.out.println(SQLUtils.toMySqlString(stmt));

        assertEquals("CREATE DEFINER = 'root'@'%' PROCEDURE `load_part_tab` ()\n" +
            "CONTAINS SQL\n"
            + "SQL SECURITY DEFINER\n" +
            "BEGIN\n" +
            "\tDECLARE v INT DEFAULT 0;\n" +
            "\tWHILE v < 1 DO\n" +
            "\tINSERT INTO part_tab\n" +
            "\tVALUES (v, 'testing partitions', ADDDATE('1995-01-01', RAND(v) * 36520 % 3652));\n" +
            "\tSET v = v + 1;\n" +
            "\tEND WHILE;\n" +
            "END;", SQLUtils.toMySqlString(stmt));

        SchemaStatVisitor visitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);
        stmt.accept(visitor);

        System.out.println("Tables : " + visitor.getTables());
        System.out.println("fields : " + visitor.getColumns());
//        System.out.println("coditions : " + visitor.getConditions());
//        System.out.println("orderBy : " + visitor.getOrderByColumns());

        assertEquals(1, visitor.getTables().size());
        assertEquals(0, visitor.getColumns().size());
        assertEquals(0, visitor.getConditions().size());

        assertTrue(visitor.containsTable("part_tab"));
    }

    public void test_1() throws Exception {
        String sql = "CREATE PROCEDURE doiterate(p1 INT)\n" +
            "BEGIN\n" +
            "  label1: LOOP\n" +
            "    SET p1 = p1 + 1;\n" +
            "    IF p1 < 10 THEN\n" +
            "      ITERATE label1;\n" +
            "    END IF;\n" +
            "    LEAVE label1;\n" +
            "  END LOOP label1;\n" +
            "  SET @x = p1;\n" +
            "END;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLStatement stmt = statementList.get(0);
//    	print(statementList);
        assertEquals(1, statementList.size());

        System.out.println(SQLUtils.toMySqlString(stmt));

        assertEquals("CREATE PROCEDURE doiterate (\n" +
            "\tp1 INT\n" +
            ")\n" +
            "CONTAINS SQL\n"
            + "SQL SECURITY DEFINER\n" +
            "BEGIN\n" +
            "\tlabel1: LOOP \n" +
            "\t\tSET p1 = p1 + 1;\n" +
            "\t\tIF p1 < 10 THEN\n" +
            "\t\t\tITERATE label1;\n" +
            "\t\tEND IF;\n" +
            "\t\tLEAVE label1;\n" +
            "\tEND LOOP label1;\n" +
            "\tSET @x = p1;\n" +
            "END;", SQLUtils.toMySqlString(stmt));

    }

    public void test_2() throws Exception {
        String sql = "CREATE PROCEDURE p()  \n" +
            "BEGIN  \n" +
            "      \n" +
            "    declare c int;  \n" +
            "    declare n varchar(20);  \n" +
            "      \n" +
            "    declare total int default 0;  \n" +
            "      \n" +
            "    declare done int default false;  \n" +
            "      \n" +
            "    declare cur cursor for select name,count from store where name = 'iphone';  \n" +
            "      \n" +
            "    declare continue HANDLER for not found set done = true;  \n" +
            "      \n" +
            "    set total = 0;  \n" +
            "      \n" +
            "    open cur;  \n" +
            "      \n" +
            "    read_loop:loop  \n" +
            "      \n" +
            "    fetch cur into n,c;  \n" +
            "      \n" +
            "    if done then  \n" +
            "        leave read_loop;      \n" +
            "    end if;  \n" +
            "      \n" +
            "    set total = total + c;  \n" +
            "      \n" +
            "    end loop;  \n" +
            "      \n" +
            "    close cur;  \n" +
            "  \n" +
            "      \n" +
            "    select total;  \n" +
            "END;";

        List<SQLStatement> statementList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
        SQLStatement stmt = statementList.get(0);
//    	print(statementList);
        assertEquals(1, statementList.size());

        System.out.println(SQLUtils.toMySqlString(stmt));

        assertEquals("CREATE PROCEDURE p ()\n" +
            "CONTAINS SQL\n"
            + "SQL SECURITY DEFINER\n" +
            "BEGIN\n" +
            "\tDECLARE c int;\n" +
            "\tDECLARE n varchar(20);\n" +
            "\tDECLARE total int DEFAULT 0;\n" +
            "\tDECLARE done int DEFAULT false;\n" +
            "\tDECLARE cur CURSOR FOR\n" +
            "\t\tSELECT name, count\n" +
            "\t\tFROM store\n" +
            "\t\tWHERE name = 'iphone';\n" +
            "\tDECLARE CONTINUE HANDLER FOR NOT FOUND\n" +
            "\t\tSET done = true;\n" +
            "\tSET total = 0;\n" +
            "\tOPEN cur;\n" +
            "\tread_loop: LOOP \n" +
            "\t\tFETCH cur INTO n, c;\n" +
            "\t\tIF done THEN\n" +
            "\t\t\tLEAVE read_loop;\n" +
            "\t\tEND IF;\n" +
            "\t\tSET total = total + c;\n" +
            "\tEND LOOP read_loop;\n" +
            "\tCLOSE cur;\n" +
            "\tSELECT total;\n" +
            "END;", SQLUtils.toMySqlString(stmt));
    }

    public void test_fake() throws Exception {
        String sql =
            "UPDATE gx_brand_votelist SET itemid= \"3674133\",brandid= \"1527519\",groupid= \"0\",num= \"1001\",name= \"章丘市万源有色金属铸造有限公司\",introduce= \"<span style=\"color:#333333;font-family:\"background-color:#FFFFFF;\">铜铝铸件的加工、销售；金属材料、电线、电缆、铝锭、铅锭、铜锭、电解铜板、锌锭、稀有金属的销售。(依法须经批准的项目，经相关部门批准后方可开展经营活动)</span>\",video= \"\",votes=0,status= \"3\",zvote=0,zvote1=0,kouvote=0,truevote=0,yzm= \"2\",shuatime= \"4.0\",shuanum= \"1\",addtime= \"1591346748\",userid= \"1200039\",usertype= \"1\",is_ht= \"1\",is_apply=\"2\",ranking=0,username= \"rew4_2\",erweima= \"code/twocode/202006/05/159134697047696370.png\",editor= \"rew4_2\",edittime= \"1591346970\",shebei= \"0\",note= \"\",sid= \"a7552457d09eeea9\",top_ten= \"2\",email= \"13586786952@qq.com\",tel= \"13953123778\",qq= \"\",remark= \"\",web= \"https://www.tianyancha.com/company/1581157368\",xianzhi= \"0\",wxnum=0,backgift=0,giftvote=0,regCapital= \"9800.00\",actualCapital= \"0.00\",attention=0,tuiguang= \"1\",regyear= \"\",is_new= \"1\",is_sign= \"2\",gongsi_status=\"0\",share_count= \"0\",share_click= \"0\",company_status= \"0\",address= \"\",shenhe_pingtai_status= \"0\",video_url= \"\",anjian_num= \"0\" WHERE itemid = 3674133 AND brandid = 1527519";

        try {
            SQLStatement statement = SQLUtils.parseSingleStatement(sql, DbType.mysql);
        } catch (Exception e) {
            assertEquals(
                "syntax error, error in :'\"<span style=\"color:#333333;font-family:\"bac, pos 175, line 1, column 171, token IDENTIFIER color",
                e.getMessage());
        }
    }
}
