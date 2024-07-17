package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.Account;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wuzhe
 */
public class CheckBalancePlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(CheckBalancePlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final String beforeCheckStmt;

    public CheckBalancePlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("check_balance");
        if (null == config) {
            enabled = false;
            beforeCheckStmt = null;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        beforeCheckStmt = config.getString("before_check_stmt", null);
    }

    @Override
    public void runInternal() {
        final AtomicReference<List<Account>> accounts = new AtomicReference<>();
        getConnectionAndExecute(dsn, (conn, error) -> {
            try {
                accounts.set(getAccounts(conn));
            } catch (SQLException e) {
                logger.error("Check balance error.", e);
                error.set(e);
            }
        });
        checkConsistency(accounts.get(), hint);
    }

    private List<Account> getAccounts(MyConnection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            if (null != beforeCheckStmt) {
                stmt.execute(beforeCheckStmt);
            }
            float choice = random.nextFloat();
            // 1/3 begin, 1/3 start transaction readonly, 1/3 auto-commit
            if (choice < 0.33) {
                conn.setAutoCommit(false);
            } else if (choice < 0.66) {
                conn.setReadOnly(true);
                conn.setAutoCommit(false);
            }

            List<Account> accounts;
            try {
                accounts = Utils.getAccounts(hint, stmt);
            } finally {
                // rollback if necessary
                if (choice < 0.66) {
                    conn.rollback();
                }
            }
            return accounts;
        }
    }

}
