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
public class ReplicaSHintPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSHintPlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final String readHint;
    private final String replicaDsn;
    private final String sessionVar;
    private final boolean strongConsistency;
    private final List<String> sessionHint = new ArrayList<>();

    public ReplicaSHintPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("replica_session_hint");
        if (null == config) {
            enabled = false;
            readHint = null;
            replicaDsn = null;
            sessionVar = null;
            strongConsistency = false;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        readHint = config.getString("replica_read_hint", null);
        replicaDsn = config.getString("replica_dsn", dsn);
        sessionVar = config.getString("session_var", null);
        strongConsistency = config.getBoolean("replica_strong_consistency", false);
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
        final AtomicReference<List<Account>> masterAccounts = new AtomicReference<>();
        if (strongConsistency) {
            // read master data first
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    // 1/3 begin, 1/3 start transaction readonly, 1/3 auto-commit
                    if (random.nextBoolean()) {
                        conn.setAutoCommit(false);
                    } else {
                        conn.setReadOnly(true);
                        conn.setAutoCommit(false);
                    }

                    try {
                        masterAccounts.set(Utils.getAccountsWithSessionHint(hint, stmt, sessionHint));
                    } finally {
                        conn.rollback();
                        conn.setAutoCommit(true);
                    }
                } catch (SQLException e) {
                    error.set(e);
                    logger.error("Get accounts in master error.", e);
                }
            });
        }
        // read slave data
        final AtomicReference<List<Account>> slaveAccounts = new AtomicReference<>();
        String slaveHint = hint + readHint;
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                conn.setReadOnly(true);
                conn.setAutoCommit(false);

                try {
                    slaveAccounts.set(Utils.getAccountsWithSessionHint(slaveHint, stmt, sessionHint));
                } finally {
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
            } catch (SQLException e) {
                error.set(e);
                logger.error("Check balance with session hint replica read error.", e);
            }
        });
        checkConsistency(slaveAccounts.get(), slaveHint);

        // check slave data should be fresher
        if (strongConsistency) {
            checkStrongConsistency(masterAccounts.get(), slaveAccounts.get(), hint);
        }
    }
}
