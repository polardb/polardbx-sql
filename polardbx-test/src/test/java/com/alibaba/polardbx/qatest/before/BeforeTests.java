package com.alibaba.polardbx.qatest.before;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Connection;

public class BeforeTests extends BaseTestCase {
    private static final Log log = LogFactory.getLog(BeforeTests.class);

    @Test
    public void config() {
        // Config database before running tests.
        try (Connection polarxConn = getPolardbxDirectConnection()) {
            SecureRandom random = new SecureRandom();
            String sql;
            if (random.nextBoolean()) {
                sql = "set global ENABLE_XA_TSO = true";
                JdbcUtil.executeUpdateSuccess(polarxConn, sql);
                log.warn(sql);
            }
            if (random.nextBoolean()) {
                sql = "set global ENABLE_AUTO_COMMIT_TSO = true";
                JdbcUtil.executeUpdateSuccess(polarxConn, sql);
                log.warn(sql);
            }
        } catch (Throwable t) {
            log.error("config before tests failed", t);
        }
    }
}
