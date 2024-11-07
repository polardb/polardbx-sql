package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.gms.metadb.trx.DeadlocksAccessor;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.executor.handler.LogicalShowLocalDeadlocksHandler.SHOW_ENGINE_INNODB_STATUS;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

public class DeadlockManager {
    protected static final Logger logger = LoggerFactory.getLogger(DeadlockManager.class);
    private static final Object MONITOR = new Object();
    private static final AtomicReference<String> LOCAL_DEADLOCK = new AtomicReference<>(null);
    private static final Pattern PATTERN =
        Pattern.compile(".*Error occurs when execute on GROUP .* ATOM '(.*?)#(.*?)#.*'");

    static {
        try {
            (new Thread(() -> {
                try {
                    while (true) {
                        synchronized (MONITOR) {
                            MONITOR.wait(1000 * 60 * 10);
                        }
                        if (null != LOCAL_DEADLOCK.get()) {
                            // Record local deadlock into meta db.
                            recordLocalDeadlock();
                            LOCAL_DEADLOCK.set(null);
                        }
                        // Clean deadlock logs if necessary.
                        cleanDeadlockLogs();
                    }
                } catch (Throwable t) {
                    logger.error("deadlock recorder failed, exit.", t);
                }
            }, "Deadlock-Log-Recorder")).start();
        } catch (Throwable t) {
            logger.error("init deadlock recorder failed.", t);
        }
    }

    private static void cleanDeadlockLogs() {
        if (!ExecUtils.hasLeadership(DEFAULT_DB_NAME)) {
            return;
        }
        try (Connection connection = MetaDbUtil.getConnection()) {
            DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
            deadlocksAccessor.setConnection(connection);
            deadlocksAccessor.rotate();
        } catch (Exception e) {
            logger.error("record local deadlock failed.", e);
        }
    }

    protected static void recordLocalDeadlock() {
        Matcher matcher = PATTERN.matcher(LOCAL_DEADLOCK.get());
        if (matcher.find()) {
            // Only fetch this dn.
            String dn = matcher.group(2);
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dn);
                Statement stmt = conn.createStatement()) {
                final ResultSet rs = stmt.executeQuery(SHOW_ENGINE_INNODB_STATUS);
                if (rs.next()) {
                    final String status = rs.getString("Status");
                    if (null != status) {
                        // Parse the {status} to get deadlock information,
                        final String deadlockLog = DeadlockParser.parseLocalDeadlock(status);
                        try (Connection connection = MetaDbUtil.getConnection()) {
                            DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
                            deadlocksAccessor.setConnection(connection);
                            deadlocksAccessor.recordDeadlock(dn, "LOCAL", deadlockLog);
                        } catch (Exception e) {
                            logger.error("record local deadlock failed.", e);
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to fetch deadlock information on dn " + dn, e);
            }
        }
    }

    private static final DeadlockManager INSTANCE = new DeadlockManager();

    public static DeadlockManager getInstance() {
        return INSTANCE;
    }

    public static void recordLocalDeadlock(String errMsg) {
        LOCAL_DEADLOCK.set(errMsg);
        synchronized (MONITOR) {
            MONITOR.notify();
        }
    }

    private DeadlockManager() {
    }
}
