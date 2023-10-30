package com.alibaba.polardbx.optimizer.utils;

import com.clearspring.analytics.util.Lists;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.junit.Test;

import java.util.List;
import java.util.Locale;

/**
 * @author fangwu
 */
public class RexUtilsTest {
    @Test
    public void testIndexHintSerialization() {
        List<String> testLines = Lists.newArrayList();
        testLines.add("FORCE INDEXIDX_NAME");
        testLines.add("IGNORE INDEXIDXID");
        testLines.add("use INDEXIDXID1,name1,name2");

        SqlNode node = RexUtil.deSeriIndexHint(testLines);
        System.out.println(node);
        assert node instanceof SqlNodeList;
        assert ((SqlNodeList) node).getList().size() == 3;
        String rs = node.toSqlString(MysqlSqlDialect.DEFAULT).toString().toUpperCase(Locale.ROOT);
        assert rs.contains("FORCE INDEX(IDX_NAME)");
        assert rs.contains("IGNORE INDEX(IDXID)");
        assert rs.contains("USE INDEX(IDXID1, NAME1, NAME2)");
    }
}
