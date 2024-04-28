package com.alibaba.polardbx.qatest.dql.sharding.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ExplainPhysicalTest extends AutoReadBaseTestCase {
    private static final String TABLE_NAME = "test_explain_physical";
    private static final String CREATE_TABLE =
        "/*+TDDL:cmd_extra(ENABLE_RANDOM_PHY_TABLE_NAME=false)*/ create table %s(c1 int, c2 int) partition by hash(c1) partitions 4";
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

    @Test
    public void testMpp() throws SQLException {
        String result = getExplainPhysicalResult(tddlConnection, String.format(
            "/*+TDDL: WORKLOAD_TYPE=AP ENABLE_MPP=TRUE ENABLE_MASTER_MPP=TRUE MPP_NODE_SIZE=2 MPP_PARALLELISM=4*/"
                + CHECK_SQL,
            TABLE_NAME));
        String expect = "ExecutorType: MPP\n"
            + "The Query's MaxConcurrentParallelism: 8\n"
            + "Fragment 1 \n"
            + "    Output partitioning: SINGLE [] Parallelism: 4 \n"
            + "    HashAgg(group=\"c2\", count(*)=\"SUM(count(*))\")\n"
            + "  RemoteSource(sourceFragmentIds=[0], type=RecordType(INTEGER c2, BIGINT count(*)))\n"
            + "Pipeline 0 dependency: [] parallelism: 2 \n"
            + " RemoteSource(sourceFragmentIds=[0], type=RecordType(INTEGER c2, BIGINT count(*)))\n"
            + "Pipeline 1 dependency: [] parallelism: 2 \n"
            + " LocalBuffer\n"
            + "  RemoteSource(sourceFragmentIds=[0], type=RecordType(INTEGER c2, BIGINT count(*)))\n"
            + "Pipeline 2 dependency: [0, 1] parallelism: 2 \n"
            + " HashAgg(group=\"c2\", count(*)=\"SUM(count(*))\")\n"
            + "  RemoteSource(sourceFragmentIds=[1], type=RecordType(INTEGER c2, BIGINT count(*)))\n"
            + "Fragment 0 \n"
            + "    Output partitioning: FIXED [0] Parallelism: 4 Splits: 4 \n"
            + "    LogicalView(tables=\"test_explain_physical[p1,p2,p3,p4]\", shardCount=4, sql=\"SELECT `c2`, COUNT(*) AS `count(*)` FROM `test_explain_physical` AS `test_explain_physical` GROUP BY `c2`\")\n"
            + "Pipeline 0 dependency: [] parallelism: 2 \n"
            + " LogicalView(tables=\"test_explain_physical[p1,p2,p3,p4]\", shardCount=4, sql=\"SELECT `c2`, COUNT(*) AS `count(*)` FROM `test_explain_physical` AS `test_explain_physical` GROUP BY `c2`\")\n";
        Assert.assertTrue(StringUtils.equals(result, expect),
            String.format("result of explain physical not expect, expect %s, but was %s", expect, result));
    }

    @After
    public void dropTable() {
        JdbcUtil.executeSuccess(tddlConnection, String.format(DROP_TABLE, TABLE_NAME));
    }
}
