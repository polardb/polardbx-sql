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
public class CheckCdcPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(CheckBalancePlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final String replicaDsn;

    public CheckCdcPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("check_cdc");
        if (null == config) {
            enabled = false;
            replicaDsn = null;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        replicaDsn = config.getString("replica_dsn", dsn);
    }

    @Override
    public void runInternal() {
        AtomicReference<List<Account>> slaveAccounts = new AtomicReference<>();
        String slaveHint = hint;
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            // read slave data from cdc
            try (Statement stmt = conn.createStatement()) {
                slaveAccounts.set(Utils.getAccounts(slaveHint, stmt));
            } catch (SQLException e) {
                logger.error("Check balance with cdc error.", e);
                error.set(e);
            }
        });
        // check consistency for slave
        checkConsistency(slaveAccounts.get(), slaveHint);
    }
}
