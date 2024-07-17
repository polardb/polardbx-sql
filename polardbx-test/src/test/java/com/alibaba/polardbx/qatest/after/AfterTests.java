package com.alibaba.polardbx.qatest.after;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

public class AfterTests extends BaseTestCase {
    @Test
    public void check() {
        try (Connection polarxConn = getPolardbxDirectConnection()) {
            // 检查复制中断
            String sql = "show storage";
            ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, sql);
            while (rs.next()) {
                long delay = rs.getLong("DELAY");
                String id = rs.getString("STORAGE_INST_ID");
                Assert.assertTrue(delay < 10, "delay " + delay + " too long for " + id);
            }

            // 检查事务泄露
            sql = "select * from information_schema.polardbx_trx";
            rs = JdbcUtil.executeQuerySuccess(polarxConn, sql);
            int cnt = 0;
            while (rs.next()) {
                cnt++;
                String txid = rs.getString("TRX_ID");
                String schema = rs.getString("SCHEMA");
                long duration = rs.getLong("DURATION_TIME");
                String cn = rs.getString("CN_ADDRESS");
                String sql2 = rs.getString("SQL");
                if (!sql.equals(sql2)) {
                    System.out.println("txid: " + txid + ", schema: " + schema
                        + ", duration: " + duration + ", cn: " + cn + ", sql: " + sql2);
                }
            }
            Assert.assertTrue(cnt == 1, "more than 1 trx found");
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
