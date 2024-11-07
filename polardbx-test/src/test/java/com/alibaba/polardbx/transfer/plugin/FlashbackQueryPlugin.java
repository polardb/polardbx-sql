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
public class FlashbackQueryPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(FlashbackQueryPlugin.class);
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final SecureRandom random = new SecureRandom();
    private final int minTime;
    private final int maxTime;

    private boolean firstTime = true;

    public FlashbackQueryPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("flashback_query");
        if (null == config) {
            enabled = false;
            minTime = 0;
            maxTime = 0;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
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
        AtomicReference<List<Account>> accounts = new AtomicReference<>();
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                accounts.set(Utils.getAccountsWithFlashbackQuery(hint, stmt, minTime, maxTime));
            } catch (SQLException e) {
                error.set(e);
                logger.error("Check balance with flashback query error.", e);
            }
        });
        // check consistency
        checkConsistency(accounts.get(), hint);
    }
}
