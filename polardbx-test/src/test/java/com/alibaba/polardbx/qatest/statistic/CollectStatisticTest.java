package com.alibaba.polardbx.qatest.statistic;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

/**
 * @author fangwu
 */
public class CollectStatisticTest extends BaseTestCase {
    static final String collectSql = "COLLECT STATISTIC";

    @Test
    public void testSampleSketchJob() throws SQLException, InterruptedException {
        long now = System.currentTimeMillis();

        // start collect statistic
        Connection c = this.getPolardbxConnection();
        JdbcUtil.executeUpdateSuccess(c, collectSql);

        // waiting job done
        boolean hasSample = false;
        boolean hasPersist = false;
        boolean hasSync = false;
        // check schedule job step
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String sql =
            "select EVENT from information_schema.module_event where MODULE_NAME='STATISTICS' and TIMESTAMP>'"
                + sdf.format(new Date(now)) + "' AND trace_info like '%ServerExecutor%'";
        ResultSet moduleRs = JdbcUtil.executeQuery(sql, this.getPolardbxConnection());
        System.out.println("get event log from module_event");

        while (moduleRs.next()) {
            String event = moduleRs.getString("EVENT");
            if (!hasSample && event.contains("statistic sample started")) {
                hasSample = true;
                System.out.println(event);
                System.out.println("hasSample");
            } else if (!hasPersist && event.contains("persist tables statistic")) {
                hasPersist = true;
                System.out.println(event);
                System.out.println("hasPersist");
            } else if (!hasSync && event.contains("sync statistic info")) {
                hasSync = true;
                System.out.println(event);
                System.out.println("hasSync");
            }
            if (hasSample && hasSync && hasPersist) {
                break;
            }
        }
        Assert.assertTrue(hasSample && hasSync && hasPersist);
    }
}
