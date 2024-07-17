package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author wuzhe
 */
public class SimpleTransferPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(SimpleTransferPlugin.class);
    private final boolean injectCommitFailure;
    private final double injectCommitFailureProb;
    private final SecureRandom random = new SecureRandom();

    public SimpleTransferPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("transfer_simple");
        if (null == config) {
            enabled = false;
            injectCommitFailure = false;
            injectCommitFailureProb = 0;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        injectCommitFailure = config.getBoolean("inject_commit_failure", false);
        injectCommitFailureProb = config.getDouble("inject_commit_failure_prob", 0.1);
    }

    @Override
    public void runInternal() {
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                long src = random.nextInt(rowCount);
                long dst = random.nextInt(rowCount);
                while (src == dst) {
                    dst = random.nextInt(rowCount);
                }

                transfer(random, stmt, src, dst);
            } catch (SQLException e) {
                error.set(e);
                logger.error("Transfer simple error.", e);
            }
        });
    }

    private void transfer(SecureRandom random, Statement stmt, long src, long dst) throws SQLException {
        // begin
        stmt.execute("begin");
        boolean injectError = false;

        try {
            // check balance is enough
            ResultSet rs = stmt.executeQuery("select balance from accounts where id = " + src + " for update");

            if (!rs.next() || rs.getLong(1) < 0) {
                stmt.execute("rollback");
                return;
            }

            // transfer
            String hint = "";
            if (injectCommitFailure && random.nextFloat() < injectCommitFailureProb) {
                injectError = true;
                float choice = random.nextFloat();
                if (choice < 0.45) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";
                } else if (choice < 0.9) {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_DURING_PRIMARY_COMMIT') */";
                } else {
                    hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='DELAY_BEFORE_WRITE_COMMIT_LOG') */";
                }
            }
            stmt.execute(hint + "update accounts SET balance = balance - 1, version = version + 1 where id = " + src);
            stmt.execute(hint + "update accounts SET balance = balance + 1, version = version + 1 where id = " + dst);
            // commit
            stmt.execute("commit");
        } catch (SQLException e) {
            if (!injectError && !e.getMessage().contains("Deadlock found when trying to get lock")) {
                logger.error("Transfer simple error.", e);
            }
            stmt.execute("rollback ");
        }
    }
}
