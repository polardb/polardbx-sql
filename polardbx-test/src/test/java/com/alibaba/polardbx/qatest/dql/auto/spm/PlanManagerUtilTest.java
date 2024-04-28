package com.alibaba.polardbx.qatest.dql.auto.spm;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class PlanManagerUtilTest {

    @Test
    public void testGettingTableSetFromAst() {
        String sql1 = "WITH tb AS (\n"
            + "  SELECT\n"
            + "    a.c1 x, b.c2 y\n"
            + "  FROM \n"
            + "    db1.t1 a\n"
            + "    JOIN db4.t2 b on a.name=b.name\n"
            + ")\n"
            + "\n"
            + "SELECT \n"
            + "  (tb.x + 1),\n"
            + "  concat(tb.y, 'xx', a.name),\n"
            + "  (select max(salary) from db5.t3),\n"
            + "  b.salary\n"
            + "FROM \n"
            + "  db2.t1 a \n"
            + "  JOIN db3.t2 b on a.id = b.id\n"
            + "  JOIN tb c on a.age = c.age\n"
            + "WHERE\n"
            + "\tb.val > (select avg(val) from db6.t1)\n"
            + "  AND NOT EXISTS ( SELECT 1 FROM t1 d WHERE d.id > 0);";
        SqlNode sqlNode1 = new FastsqlParser().parse(sql1).get(0);
        Set<Pair<String, String>> tableSet1 = PlanManagerUtil.getTableSetFromAst(sqlNode1);
        Set<Pair<String, String>> real1 = new HashSet<>();
        real1.add(Pair.of("db1", "t1"));
        real1.add(Pair.of("db4", "t2"));
        real1.add(Pair.of("db5", "t3"));
        real1.add(Pair.of("db2", "t1"));
        real1.add(Pair.of("db3", "t2"));
        real1.add(Pair.of("db6", "t1"));
        real1.add(Pair.of(null, "t1"));
        Assert.assertTrue(tableSet1.size() == real1.size());
        for (Pair<String, String> pair : tableSet1) {
            if (!real1.contains(pair)) {
                Assert.fail();
            }
        }

        String sql2 = "Select * from b.t b join a.t a on a.id=b.id where a.id = 2";
        SqlNode sqlNode2 = new FastsqlParser().parse(sql2).get(0);
        Set<Pair<String, String>> tableSet2 = PlanManagerUtil.getTableSetFromAst(sqlNode2);
        Set<Pair<String, String>> real2 = new HashSet<>();
        real2.add(Pair.of("a", "t"));
        real2.add(Pair.of("b", "t"));
        Assert.assertTrue(tableSet2.size() == real2.size());
        for (Pair<String, String> pair : tableSet2) {
            if (!real2.contains(pair)) {
                Assert.fail();
            }
        }

        String sql3 = "select * from (select * from a.t1 a join b.t2 b on a.id = b.id)";
        SqlNode sqlNode3 = new FastsqlParser().parse(sql3).get(0);
        Set<Pair<String, String>> tableSet3 = PlanManagerUtil.getTableSetFromAst(sqlNode3);
        Set<Pair<String, String>> real3 = new HashSet<>();
        real3.add(Pair.of("a", "t1"));
        real3.add(Pair.of("b", "t2"));
        Assert.assertTrue(tableSet3.size() == real3.size());
        for (Pair<String, String> pair : tableSet3) {
            if (!real3.contains(pair)) {
                Assert.fail();
            }
        }

    }

}
