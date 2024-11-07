package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.Account;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yudong
 * @since 2024/6/13 17:37
 **/
public class CheckSyncPointPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(CheckSyncPointPlugin.class);
    private static final String SELECT_SYNC_POINT_SQL =
        "select * from information_schema.rpl_sync_point order by create_time desc limit 1";
    private static final String snapshotHint = "/*+TDDL:SNAPSHOT_TS=%s*/";
    private final String hint = "/*" + UUID.randomUUID() + "*/";
    private final String replicaDsn;

    public CheckSyncPointPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("check_rpl_sync_point");
        if (null == config) {
            enabled = false;
            replicaDsn = null;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        replicaDsn = config.getString("replica_dsn", dsn);
    }

    @Override
    public void runInternal() {
        // get sync point
        final AtomicReference<String> primaryTso = new AtomicReference<>();
        final AtomicReference<String> secondaryTso = new AtomicReference<>();
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(SELECT_SYNC_POINT_SQL)) {
                if (rs.next()) {
                    primaryTso.set(rs.getString("PRIMARY_TSO"));
                    secondaryTso.set(rs.getString("SECONDARY_TSO"));
                    if (logger.isDebugEnabled()) {
                        logger.debug("primary tso: " + primaryTso.get() + ", secondary tso: " + secondaryTso.get());
                    }
                } else {
                    throw new SQLException("Cannot get sync point from metadb!");
                }
            } catch (SQLException e) {
                error.set(e);
                logger.error("Get sync point error.", e);
            }
        });

        // read master data first
        String masterHint = String.format(snapshotHint, primaryTso.get());
        final AtomicReference<List<Account>> masterAccounts = new AtomicReference<>();
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                masterAccounts.set(Utils.getAccounts(masterHint, stmt));
            } catch (SQLException e) {
                error.set(e);
                logger.error("Get sync point error.", e);
            }
        });

        // read slave data
        String slaveHint = String.format(snapshotHint, secondaryTso.get());
        final AtomicReference<List<Account>> slaveAccounts = new AtomicReference<>();
        getConnectionAndExecute(replicaDsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                slaveAccounts.set(Utils.getAccounts(slaveHint, stmt));
            } catch (SQLException e) {
                error.set(e);
                logger.error("Get sync point error.", e);
            }
        });

        checkStrongConsistency(masterAccounts.get(), slaveAccounts.get(), hint);
    }
}
