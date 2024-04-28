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
public class ReplicaReadPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaReadPlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final String readHint;
    private final String replicaDsn;
    private final String sessionVar;
    private final boolean strongConsistency;

    public ReplicaReadPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("replica_read");
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
    }

    @Override
    public void runInternal() {
        final AtomicReference<List<Account>> masterAccounts = new AtomicReference<>();
        if (strongConsistency) {
            // read master data first
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    float choice = random.nextFloat();
                    // 1/3 begin, 1/3 start transaction readonly, 1/3 auto-commit
                    if (choice < 0.33) {
                        conn.setAutoCommit(false);
                    } else if (choice < 0.66) {
                        conn.setReadOnly(true);
                        conn.setAutoCommit(false);
                    }

                    try {
                        masterAccounts.set(Utils.getAccounts(hint, stmt));
                    } finally {
                        // rollback if necessary
                        if (choice < 0.66) {
                            conn.rollback();
                        }
                    }
                } catch (SQLException e) {
                    error.set(e);
                    logger.error("Get accounts in master error.", e);
                }
            });
        }
        // read slave data
        String slaveHint = hint + readHint;
        final AtomicReference<List<Account>> slaveAccounts = new AtomicReference<>();
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                float choice = random.nextFloat();
                // 1/3 begin, 1/3 start transaction readonly, 1/3 auto-commit
                if (choice < 0.33) {
                    conn.setAutoCommit(false);
                } else if (choice < 0.66) {
                    conn.setReadOnly(true);
                    conn.setAutoCommit(false);
                }

                try {
                    slaveAccounts.set(Utils.getAccounts(slaveHint, stmt));
                } finally {
                    // rollback if necessary
                    if (choice < 0.66) {
                        conn.rollback();
                    }
                }
            } catch (SQLException e) {
                error.set(e);
                logger.error("Check balance with replica read error.", e);
            }
        });
        // check consistency for slave
        checkConsistency(slaveAccounts.get(), slaveHint);
        // check slave data should be fresher
        if (strongConsistency) {
            checkStrongConsistency(masterAccounts.get(), slaveAccounts.get(), hint);
        }
    }
}
