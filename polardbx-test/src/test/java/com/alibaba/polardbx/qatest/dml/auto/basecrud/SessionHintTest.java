package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;

/**
 * @author fangwu
 */
public class SessionHintTest extends BaseTestCase {
    private static final Log log = LogFactory.getLog("ROOT");

    /**
     * test if push hint working
     * set partition_name=xxx;
     * trace select * from xxx;
     * show trace && check if partition_name work
     */
    @Test
    public void testSessionHintWithShardingTable() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_qatest_app");
            c.createStatement().execute("set partition_name=DRDS_POLARX1_QATEST_APP_000001_GROUP");
            c.createStatement().execute("trace select * from select_base_three_multi_db_one_tb");
            ResultSet rs = c.createStatement().executeQuery("show trace");

            while (rs.next()) {
                String groupName = rs.getString("GROUP_NAME");
                Assert.assertTrue(groupName.equalsIgnoreCase("DRDS_POLARX1_QATEST_APP_000001_GROUP"));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_name=''");
            log.info("session hint test end");
        }
    }

    /**
     * test if push hint working
     * set partition_name=xxx;
     * trace select * from xxx;
     * show trace && check if partition_name work
     */
    @Test
    public void testSessionHintWithAutoTable() throws SQLException {
        Connection c = null;
        try {
            c = getPolardbxConnection();
            c.createStatement().execute("use drds_polarx1_part_qatest_app");

            String tableName = "select_base_three_multi_db_multi_tb";
            ResultSet topologyRs =
                c.createStatement().executeQuery("show topology from " + tableName);
            Map<String, String> partGroupMap = Maps.newHashMap();
            while (topologyRs.next()) {
                String groupName = topologyRs.getString("GROUP_NAME");
                String partName = topologyRs.getString("PARTITION_NAME");
                partGroupMap.put(partName, groupName);
            }

            topologyRs.close();
            for (Map.Entry<String, String> entry : partGroupMap.entrySet()) {
                String partName = entry.getKey();
                String checkGroupName = entry.getValue();
                c.createStatement().execute("set partition_name=" + partName);
                c.createStatement().execute("trace select * from " + tableName);
                ResultSet rs = c.createStatement().executeQuery("show trace");

                while (rs.next()) {
                    String groupName = rs.getString("GROUP_NAME");
                    Assert.assertTrue(groupName.equalsIgnoreCase(checkGroupName));
                }
                rs.close();
                String joinSql =
                    "trace select * from " + tableName + " a join select_base_two_multi_db_multi_tb b on a.pk=b.pk";
                c.createStatement().executeQuery(joinSql);
                rs = c.createStatement().executeQuery("show trace");

                while (rs.next()) {
                    String groupName = rs.getString("GROUP_NAME");
                    Assert.assertTrue(groupName.equalsIgnoreCase(checkGroupName));
                }
            }

            try {
                String errorSql =
                    "trace select * from select_base_two_multi_db_one_tb a join select_base_four_broadcast b on a.pk=b.pk;";
                c.createStatement().execute("set partition_name=p3");
                c.createStatement().executeQuery(errorSql);
                Assert.fail(" error sql should product error msg");
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.assertTrue(
                    sqlException.getMessage().contains("Unsupported to use direct HINT "));
            }

            try {
                String errorSql =
                    "trace select * from select_base_two_multi_db_one_tb a";
                c.createStatement().execute("set partition_name=p1111");
                c.createStatement().executeQuery(errorSql);
                Assert.fail(" error sql should product error msg");
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
                log.info(sqlException.getMessage());
                Assert.assertTrue(
                    sqlException.getMessage().contains("Unsupported to use direct HINT "));
            }
        } finally {
            Objects.requireNonNull(c).createStatement().execute("set partition_name=''");
            log.info("session hint test end");
        }
    }

    @Test
    public void testSessionHintWithTrans() throws SQLException {
        Connection c1 = null;
        Connection c2 = null;
        Connection c3 = null;
        ResultSet rs = null;
        try {
            c1 = getPolardbxConnection();
            c2 = getPolardbxConnection();
            c3 = getPolardbxConnection();
            c1.createStatement().execute("use drds_polarx1_part_qatest_app");
            c2.createStatement().execute("use drds_polarx1_part_qatest_app");
            c3.createStatement().execute("use drds_polarx1_part_qatest_app");

            String tableName = "update_delete_base_date_one_multi_db_one_tb";
            // c1 using hint, truncate data
            c1.createStatement().execute("truncate table " + tableName);

            // c2 begin and check no data
            c2.setAutoCommit(false);
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c3 check no data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c1 begin and insert
            c1.setAutoCommit(false);
            c1.createStatement().execute("insert into " + tableName
                + " values(1,1,'a','a',null,1,1,1,1,1,1,1,1,1,now(), now(),now(),now(),now(),'a')");

            // c2 check no data
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c3 check no data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

            // c1 commit
            c1.commit();

            // c3 check data
            rs = c3.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(rs.next());
            rs.close();

            // c2 check no data
            rs = c2.createStatement().executeQuery("select * from " + tableName);
            Assert.assertTrue(!rs.next());
            rs.close();

        } finally {
            Objects.requireNonNull(c1).createStatement().execute("set partition_name=''");
            Objects.requireNonNull(c1).close();
            Objects.requireNonNull(c2).close();
            Objects.requireNonNull(c3).close();
            log.info("session hint test end");
        }
    }
}
