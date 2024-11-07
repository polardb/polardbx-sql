package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class AnsiQuotesTest extends BaseTestCase {
    private static final String TABLE_NAME = "AnsiQuotesTest_tb";
    public static final String CREATE_GLOBAL_TX_TABLE_V2 =
        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "_0"
            + " (\n"
            + "  `TXID` BIGINT UNSIGNED NOT NULL,\n"
            + "  `TRX_SEQ` BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551615 COMMENT 'DEFAULT INVALID_SEQUENCE_NUMBER',\n"
            + "  `N_PARTICIPANTS` INT UNSIGNED NOT NULL DEFAULT 0,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    public static final String CREATE_GLOBAL_TX_TABLE_V2_OLD =
        "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + "_1"
            + " (\n"
            + "  `TXID` BIGINT UNSIGNED NOT NULL,\n"
            + "  `TRX_SEQ` BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551615 COMMENT \"DEFAULT INVALID_SEQUENCE_NUMBER\",\n"
            + "  `N_PARTICIPANTS` INT UNSIGNED NOT NULL DEFAULT 0,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

    @Test
    public void test() throws SQLException {
        try (Connection connection = getMysqlConnection()) {
            JdbcUtil.executeIgnoreErrors(connection, "drop table if exists " + TABLE_NAME + "_0");
            JdbcUtil.executeIgnoreErrors(connection, "drop table if exists " + TABLE_NAME + "_1");
            JdbcUtil.executeUpdateSuccess(connection, "set sql_mode='ansi_quotes'");
            JdbcUtil.executeUpdateSuccess(connection, CREATE_GLOBAL_TX_TABLE_V2);
            JdbcUtil.executeUpdateFailed(connection, CREATE_GLOBAL_TX_TABLE_V2_OLD,
                "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near '\"DEFAULT INVALID_SEQUENCE_NUMBER\",");
        }
    }
}
