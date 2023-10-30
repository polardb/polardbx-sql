package com.alibaba.polardbx.qatest.dql.sharding.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExplainPhysicalTest extends ReadBaseTestCase {
    private static final String TABLE_NAME = "test_explain_physical";
    private static final String CREATE_TABLE = "create table %s(c1 int, c2 int) dbpartition by hash(c1)";
    private static final String DROP_TABLE = "drop table if exists %s";
    private static final String INSERT_DATA = "insert into %s values (1,1), (1,2), (2,2)";
    private static final String CHECK_SQL = "select c2, count(*) from %s group by c2";

    @Before
    public void init() {
        tddlConnection = getPolardbxConnection();
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(CREATE_TABLE, TABLE_NAME));
        JdbcUtil.executeSuccess(tddlConnection, String.format(INSERT_DATA, TABLE_NAME));
    }

    @Test
    public void testTpLocal() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuery(String.format(
            "explain physical " + "/*+TDDL: WORKLOAD_TYPE=TP ENABLE_MPP=FALSE ENABLE_MASTER_MPP=FALSE*/" + CHECK_SQL,
            TABLE_NAME), tddlConnection);
        if (rs.next()) {
            Assert.assertTrue(rs.getObject(1).toString().contains("TP_LOCAL"), "execute mode should be tp_local");
        } else {
            Assert.fail("result of explain physical should not be empty");
        }
        // parallelism of fragments should be 1
        while (rs.next()) {
            String content = rs.getObject(1).toString();
            if (content.contains("parallelism")) {
                String[] elems = StringUtils.split(content, " ");
                for (int i = 0; i < elems.length; ++i) {
                    if (elems[i].startsWith("parallelism")) {
                        Integer parallelism = Integer.valueOf(elems[i + 1]);
                        Assert.assertTrue(parallelism == 1, "parallelism of fragments should be 1");
                    }
                }
            }
        }
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }
}
