package com.alibaba.polardbx.qatest.plancache;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class ResizePlanCacheTest extends BaseTestCase {
    private static final String SHOW_CAPACITY_SQL = "select * from information_schema.plan_cache_capacity";
    private static final String RESIZE_SQL_PATTERN = "resize plancache %d";
    private static final int[] dataset = new int[] {1100, 1120, 1200, 2400, 4800, 9600, 4000};

    @Test
    public void testResizePlanCache() {
        for (int i = 0; i < dataset.length; i++) {
            resize(dataset[i]);
            // 本地的plan cache size 应该是刚刚修改后的plan cache size
            int cnCapacity = getCapacityFromCN();
            Assert.assertEquals(dataset[i], cnCapacity);
            // metaDB 应该固化了plan cache size 且与本地相同
            int metaCapacity = getCapacityFromMeta();
            Assert.assertEquals(dataset[i], metaCapacity);
        }
    }

    private void resize(int newSize) {
        try (Connection c = this.getPolardbxConnection()) {
            JdbcUtil.executeQuery(String.format(RESIZE_SQL_PATTERN, newSize), c);
        } catch (SQLException e) {
            e.printStackTrace();
            com.alibaba.polardbx.common.utils.Assert.fail(e.getMessage());
        }
    }

    private int getCapacityFromCN() {
        // each CN plan cache size should be the same
        int capacity = -1;
        try (Connection cn = this.getPolardbxConnection(); Connection meta = this.getMetaConnection()) {
            Set<String> cnIps = new HashSet<>();
            ResultSet cnRes = JdbcUtil.executeQuery(SHOW_CAPACITY_SQL, cn);
            while (cnRes.next()) {
                cnIps.add(cnRes.getString("COMPUTE_NODE"));
                int curCapacity = cnRes.getInt("CAPACITY");
                if (capacity != -1) {
                    Assert.assertEquals("each CN node plan cache size should be the same", capacity, curCapacity);
                }
                capacity = curCapacity;
            }
            ResultSet metaRes = JdbcUtil.executeQuery("select ip,port from node_info", cn);
            while (metaRes.next()) {
                String ip = metaRes.getString("ip") + ":" + metaRes.getString("port");
                Assert.assertTrue("all cn nodes should be in res", cnIps.contains(ip));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            com.alibaba.polardbx.common.utils.Assert.fail(e.getMessage());
        }
        return capacity;
    }

    private int getCapacityFromMeta() {
        String showCacheCapacitySql = "select * from inst_config where param_key = \"PLAN_CACHE_SIZE\";";
        ResultSet rs;
        int capacity = -1;
        try (Connection c = this.getMetaConnection()) {
            rs = JdbcUtil.executeQuery(showCacheCapacitySql, c);
            rs.next();
            capacity = rs.getInt("param_val");
        } catch (SQLException e) {
            e.printStackTrace();
            com.alibaba.polardbx.common.utils.Assert.fail(e.getMessage());
        }
        return capacity;
    }

}
