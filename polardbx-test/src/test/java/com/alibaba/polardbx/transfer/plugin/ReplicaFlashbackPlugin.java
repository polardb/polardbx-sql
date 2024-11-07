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
public class ReplicaFlashbackPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaFlashbackPlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final String readHint;
    private final String replicaDsn;
    private final String sessionVar;
    private final int minTime;
    private final int maxTime;
    private boolean firstTime = true;

    public ReplicaFlashbackPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("replica_flashback_query");
        if (null == config) {
            enabled = false;
            readHint = null;
            replicaDsn = null;
            sessionVar = null;
            minTime = 0;
            maxTime = 0;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        readHint = config.getString("replica_read_hint", null);
        replicaDsn = config.getString("replica_dsn", dsn);
        sessionVar = config.getString("session_var", null);
        minTime = Math.toIntExact(config.getLong("min_seconds"));
        maxTime = Math.toIntExact(config.getLong("max_seconds"));
    }

    @Override
    public void runInternal() {
        if (firstTime) {
            // wait until flashback timestamp is valid
            try {
                Thread.sleep(maxTime * 1000L);
                firstTime = false;
            } catch (Throwable t) {
                // ignored
                return;
            }
        }
        String slaveHint = hint + readHint;
        AtomicReference<List<Account>> slaveAccounts = new AtomicReference<>();
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                slaveAccounts.set(Utils.getAccountsWithFlashbackQuery(slaveHint, stmt, minTime, maxTime));
            } catch (SQLException e) {
                error.set(e);
                logger.error("Check balance with replica flashback query error.", e);
            }
        });
        // check consistency for slave
        checkConsistency(slaveAccounts.get(), slaveHint);
    }
}
