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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author lingce.ldm 2018-01-15 12:33
 */
public class ReplaceTableNameTest {

    private static ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(
        "test_app", new ExecutionContext());
    private static FastsqlParser parser = new FastsqlParser();
    List<Pair<String, String>> simpleSqlTC = new ArrayList<Pair<String, String>>() {

        {
            add(new Pair<>(
                "select * from ta",
                "SELECT * FROM ? AS `ta`"));
            add(new Pair<>(
                "select id from ta where id = 5",
                "SELECT `id` FROM ? AS `ta` WHERE (`id` = 5)"));
            add(new Pair<>(
                "Select * from ta join tb on ta.id = tb.id",
                "SELECT * FROM ? AS `ta` INNER JOIN ? AS `tb` ON (`ta`.`id` = `tb`.`id`)"));
            add(new Pair<>(
                "Select * from ta a join tb on ta.id = tb.id",
                "SELECT * FROM ? AS `a` INNER JOIN ? AS `tb` ON (`ta`.`id` = `tb`.`id`)"));
            add(new Pair<>(
                "Select * from ta a join tb b on a.id = b.id",
                "SELECT * FROM ? AS `a` INNER JOIN ? AS `b` ON (`a`.`id` = `b`.`id`)"));
        }
    };

    List<Pair<String, String>> sqlWithFromSubQueryTC = new ArrayList<Pair<String, String>>() {

        {
            add(new Pair<>(
                "Select * from ta a join (select id from tb where id > 10) b on a.id = b.id where a.id = 1",
                "SELECT * FROM ? AS `a` INNER JOIN (SELECT `id` FROM ? AS `tb` WHERE (`id` > 10)) AS `b` ON (`a`.`id` = `b`.`id`) WHERE (`a`.`id` = 1)"));
            add(new Pair<>(
                "Select * from ta a join (select id from (select * from tc where tc.id>10) c where c.id > 10) b on a.id = b.id where a.id = 1",
                "SELECT * FROM ? AS `a` INNER JOIN (SELECT `id` FROM (SELECT * FROM ? AS `tc` WHERE (`tc`.`id` > 10)) AS `c` WHERE (`c`.`id` > 10)) AS `b` ON (`a`.`id` = `b`.`id`) WHERE (`a`.`id` = 1)"));
        }
    };

    List<Pair<String, String>> sqlWithWhereSubQueryTC = new ArrayList<Pair<String, String>>() {

        {
            add(new Pair<>(
                "Select * from ta where id in (select pk from tb where pk = 1)",
                "SELECT * FROM ? AS `ta` WHERE (`id` IN (SELECT `pk` FROM ? AS `tb` WHERE (`pk` = 1)))"));
            add(new Pair<>(
                "Select * from ta where id in (select pk from tb where pk in (select id from tc where tc.id > 10))",
                "SELECT * FROM ? AS `ta` WHERE (`id` IN (SELECT `pk` FROM ? AS `tb` WHERE (`pk` IN (SELECT `id` FROM ? AS `tc` WHERE (`tc`.`id` > 10)))))"));
        }
    };

    @Test
    public void simpleSqlReplaceTableName() {
        doTest(simpleSqlTC);
    }

    @Test
    public void sqlWithFromSubQueryReplaceTableName() {
        doTest(sqlWithFromSubQueryTC);
    }

    @Test
    public void sqlWithWhereSubQueryReplaceTableName() {
        doTest(sqlWithWhereSubQueryTC);
    }

    private SqlNode getAst(String sql) {
        SqlNodeList nodes = parser.parse(sql);
        return nodes.get(0);
    }

    private String getSqlNodeStr(SqlNode sqlNode) {
        return StringUtils.replace(sqlNode.toString(), "\n", " ");
    }

    private void doTest(List<Pair<String, String>> testCase) {
        // mock a schema name
        OptimizerContext mockOptimizerContext = new OptimizerContext("test");
        OptimizerContext.loadContext(mockOptimizerContext);

        for (Pair<String, String> c : testCase) {
            SqlNode ast = getAst(c.getKey());
            SqlNode result = ast.accept(visitor);
            Assert.assertEquals(c.getValue(), getSqlNodeStr(result));
        }
    }
}
