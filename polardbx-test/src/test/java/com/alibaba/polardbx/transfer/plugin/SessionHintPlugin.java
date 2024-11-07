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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author wuzhe
 */
public class SessionHintPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(SessionHintPlugin.class);

    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final List<String> sessionHint = new ArrayList<>();

    public SessionHintPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("session_hint");
        if (null == config) {
            enabled = false;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        if (enabled) {
            // init session hint
            try (MyConnection conn = getConnection(dsn);
                Statement stmt = conn.createStatement()) {
                Utils.initSessionHint(stmt, sessionHint);
                logger.info("All partitions: " + String.join(",", sessionHint));
            } catch (Throwable t) {
                logger.error("Init session hint failed.", t);
            }
        }
    }

    @Override
    public void runInternal() {
        final AtomicReference<List<Account>> accounts = new AtomicReference<>();
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                // 1/2 begin, 1/2 start transaction readonly
                if (random.nextBoolean()) {
                    conn.setAutoCommit(false);
                } else {
                    conn.setReadOnly(true);
                    conn.setAutoCommit(false);
                }

                try {
                    accounts.set(Utils.getAccountsWithSessionHint(hint, stmt, sessionHint));
                } finally {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException e) {
                error.set(e);
                logger.error("Check balance using session hint error.", e);
            }
        });
        checkConsistency(accounts.get(), hint);
    }
}
