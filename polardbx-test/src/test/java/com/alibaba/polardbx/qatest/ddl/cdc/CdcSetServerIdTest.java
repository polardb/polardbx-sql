package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

public class CdcSetServerIdTest extends CdcBaseTest {
    @Test
    public void testSetServerIdTest() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("set global server_id  = 123");
                throw new TddlNestableRuntimeException("should not set global server_id success");
            } catch (SQLException e) {
                // ignore
                logger.info("set server_id failed!");
            }
            stmt.execute("set enable_set_global_server_id = 'on' ");
            stmt.execute("set global server_id = 456");

            stmt.execute("set enable_set_global_server_id = 0 ");
            try {
                stmt.execute("set global server_id = 123");
                throw new TddlNestableRuntimeException("should not set global server_id success");
            } catch (SQLException e) {
                // ignore
                logger.info("set server_id failed!");
            }
        }
    }
}
