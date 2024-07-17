package com.alibaba.polardbx.qatest.syncTests;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PlanCacheCapacityTest extends ReadBaseTestCase {

    private static final String SHOW_CAPACITY_SQL = "select * from information_schema.plan_cache_capacity";
    private static final String RESIZE_SQL_PATTERN = "set global PLAN_CACHE_SIZE=%d";

    @Test
    public void testResizePlanCache() {
        long oldCapacity = getCapacity(polardbxOneDB);
        PreparedStatement tddlPs = null;
        ResultSet rs = null;
        try {
            long newCapacity = oldCapacity + 1000;
            String resizeSql = String.format(RESIZE_SQL_PATTERN, newCapacity);
            tddlPs = JdbcUtil.preparedStatementSet(resizeSql, null, tddlConnection);
            rs = JdbcUtil.executeQuery(resizeSql, tddlPs);
            Thread.sleep(2000);
            long newCapacityFromInfoSchema = getCapacity(polardbxOneDB);
            Assert.assertEquals("New capacity doesn't match", newCapacity, newCapacityFromInfoSchema);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(tddlPs);
        }
    }

    private long getCapacity(String schema) {
        String showCacheCapacitySql = SHOW_CAPACITY_SQL + String.format(" where SCHEMA_NAME = '%s'", schema);
        PreparedStatement tddlPs = null;
        ResultSet rs = null;
        long capacity = -1;
        try {
            tddlPs = JdbcUtil.preparedStatementSet(showCacheCapacitySql, null, tddlConnection);
            rs = JdbcUtil.executeQuery(showCacheCapacitySql, tddlPs);
            // 所有节点的plan cache容量应该是相等
            while (rs.next()) {
                long curCapacity = rs.getLong("CAPACITY");
                if (capacity != -1) {
                    Assert.assertEquals(capacity, curCapacity);
                }
                capacity = curCapacity;
            }
            Assert.assertTrue("Failed to get plan cache capacity", capacity > 0);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(tddlPs);
        }
        return capacity;
    }
}
