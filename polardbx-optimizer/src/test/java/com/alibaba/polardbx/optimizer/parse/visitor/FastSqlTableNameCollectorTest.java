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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author mengshi
 */
public class FastSqlTableNameCollectorTest {

    @Test
    public void test1() {
        String sql = "delete from d3.t1";
        Assert.assertEquals("[(d3, t1)]", getTables(sql));
    }

    @Test
    public void test2() {
        String sql = "delete from t1";
        Assert.assertEquals("[(null, t1)]", getTables(sql));
    }

    @Test
    public void test3() {
        String sql = "delete from t1 where id = (select * from d2.t2)";
        Assert.assertEquals("[(null, t1), (d2, t2)]", getTables(sql));
    }

    @Test
    public void test4() {
        String sql =
            "delete ignore a.*, b.* from `drds_polarx2_qatest_app`.update_delete_base_broadcast a, `drds_polarx2_qatest_app`.update_delete_base_two_one_db_one_tb b where a.pk = b.pk";
        Assert.assertEquals(
            "[(drds_polarx2_qatest_app, update_delete_base_broadcast), (drds_polarx2_qatest_app, update_delete_base_two_one_db_one_tb)]",
            getTables(sql));
    }

    @Test
    public void test5() {
        String sql =
            "select a as a from `db1`.`t` as tt,db2.t2,db3.`t3`,t4, (select * from t5) where id = (select * from d6.t6)";
        Assert.assertEquals("[(db1, t), (db2, t2), (db3, t3), (null, t4), (null, t5), (d6, t6)]", getTables(sql));
    }

    @Test
    public void test6() {
        String sql = "update t1 set a=1";
        Assert.assertEquals("[(null, t1)]", getTables(sql));
    }

    @Test
    public void test7() {
        String sql = "UPDATE student s JOIN class c ON s.class_id = c.id SET s.class_name='test11',c.stu_name='test11'";
        Assert.assertEquals("[(null, student), (null, class)]", getTables(sql));
    }

    @Test
    public void test8() {
        String sql =
            "delete from `drds_polarx2_qatest_app`.update_delete_base_multi_db_multi_tb.*, `drds_polarx2_qatest_app`.update_delete_base_two_multi_db_multi_tb.*, a.* using `drds_polarx2_qatest_app`.update_delete_base_multi_db_multi_tb, `drds_polarx2_qatest_app`.update_delete_base_two_multi_db_multi_tb, `drds_polarx2_qatest_app`.update_delete_base_three_broadcast as a ";
        Assert.assertEquals(
            "[(drds_polarx2_qatest_app, update_delete_base_multi_db_multi_tb), (drds_polarx2_qatest_app, update_delete_base_two_multi_db_multi_tb), (drds_polarx2_qatest_app, update_delete_base_three_broadcast)]",
            getTables(sql));
    }

    public String getTables(String sql) {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        FastSqlTableNameCollector visitor = new FastSqlTableNameCollector();
        stmt.accept(visitor);
        System.out.println(visitor.getTables());
        return visitor.getTables().toString();
    }
}
