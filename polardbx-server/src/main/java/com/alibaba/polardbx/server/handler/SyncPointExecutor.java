package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.cdc.CdcTableUtil;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.trx.ISyncPointExecutor;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.cdc.SyncPointMetaAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.SyncPointTransaction;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.cdc.CdcTableUtil.CDC_TABLE_SCHEMA;

/**
 * @author yaozhili
 */
public class SyncPointExecutor implements ISyncPointExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SyncPointExecutor.class);

    private static final SyncPointExecutor INSTANCE = new SyncPointExecutor();
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS "
        + SystemTables.POLARDBX_SYNC_POINT_TB + "("
        + "  `ID` CHAR(36) NOT NULL,\n"
        + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
        + "  PRIMARY KEY (`ID`)\n"
        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
    private static final String SHOW_TABLE =
        String.format("SHOW TABLES LIKE '%s'", SystemTables.POLARDBX_SYNC_POINT_TB);
    private static final String INSERT_SQL =
        String.format("INSERT INTO `%s`(`ID`) VALUES ('%%s')", SystemTables.POLARDBX_SYNC_POINT_TB);
    private static final String DELETE_SQL =
        String.format("DELETE FROM %s WHERE GMT_CREATED < (NOW() - INTERVAL 7 DAY)",
            SystemTables.POLARDBX_SYNC_POINT_TB);
    private static final String SYNC_POINT_VAR = "enable_polarx_sync_point";

    protected static final String SHOW_SYNC_POINT_VAR =
        String.format("SHOW GLOBAL VARIABLES LIKE '%s'", SYNC_POINT_VAR);

    private SyncPointExecutor() {
    }

    public static SyncPointExecutor getInstance() {
        return INSTANCE;
    }

    // Actually, it is a set.
    private static final Cache<TGroupDataSource, Boolean> GROUP_DATASOURCE_CACHE = CacheBuilder.newBuilder()
        .maximumSize(1024)
        .expireAfterWrite(3600, TimeUnit.SECONDS)
        .softValues()
        .build();

    @Override
    public boolean execute() {
        try (InnerConnection conn = (InnerConnection) InnerConnectionManager.getInstance()
            .getConnection(CDC_TABLE_SCHEMA)) {
            conn.setExtraServerVariables(ConnectionProperties.MARK_SYNC_POINT, "true");
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeQuery(CdcTableUtil.QUERY_CDC_DDL_RECORD_LIMIT_1);
            }
            final ITransaction trx = conn.getTransaction();
            final String uuid = UUID.randomUUID().toString();
            if (trx instanceof SyncPointTransaction) {
                // 0. Get all groups, group by host:port.
                Map<String, List<TGroupDataSource>> groups = ExecUtils.getInstId2GroupList(CDC_TABLE_SCHEMA);

                ServerThreadPool threadPool =
                    TransactionManager.getInstance(CDC_TABLE_SCHEMA).getTransactionExecutor().getExecutorService();

                // 1. Ensure all first-group has a table.
                for (List<TGroupDataSource> groupList : groups.values()) {
                    if (groupList.isEmpty()) {
                        logger.warn("Found empty group.");
                        return false;
                    }

                    TGroupDataSource firstGroup = groupList.get(0);
                    if (null != GROUP_DATASOURCE_CACHE.getIfPresent(firstGroup)) {
                        continue;
                    }
                    try (Connection phyConn = firstGroup.getConnection(MasterSlave.MASTER_ONLY);
                        Statement stmt = phyConn.createStatement()) {
                        ResultSet rs = stmt.executeQuery(SHOW_TABLE);
                        if (!rs.next()) {
                            // Table not found, create a new one.
                            stmt.execute(CREATE_TABLE);
                        }
                        GROUP_DATASOURCE_CACHE.put(firstGroup, true);
                    }
                }

                // 2. Start a rw trx in each DN.
                List<Future<Boolean>> futures = new ArrayList<>();
                boolean success = true;
                int i = 0;
                for (final Map.Entry<String, List<TGroupDataSource>> entry : groups.entrySet()) {
                    Callable<Boolean> task = () -> {
                        try {
                            String groupName = entry.getKey();
                            TGroupDataSource dataSource = entry.getValue().get(0);
                            IConnection phyConn =
                                trx.getConnection(CDC_TABLE_SCHEMA, groupName, dataSource, ITransaction.RW.WRITE);
                            try (Statement stmt = phyConn.createStatement()) {
                                // Make sure enable_sync_point is on.
                                ResultSet rs = stmt.executeQuery(SHOW_SYNC_POINT_VAR);
                                if (!rs.next() || !"ON".equalsIgnoreCase(rs.getString(2))) {
                                    throw new RuntimeException("enable_sync_point is off for " + groupName);
                                }
                                // Delete out-dated data.
                                stmt.execute(DELETE_SQL);
                                // Insert new data.
                                stmt.execute(String.format(INSERT_SQL, uuid));
                            }
                        } catch (Throwable t) {
                            logger.warn("Sync point trx error: " + t.getMessage());
                            return false;
                        }
                        return true;
                    };
                    i++;
                    if (i != groups.size()) {
                        futures.add(threadPool.submit(null, null, task));
                    } else {
                        // Last DN is processed in this thread.
                        success = task.call();
                    }
                }

                for (Future<Boolean> future : futures) {
                    if (!future.get()) {
                        success = false;
                    }
                }

                if (!success) {
                    conn.rollback();
                    return false;
                }

                // 3. Commit trx.
                conn.commit();
                int participants = ((SyncPointTransaction) trx).getNumberOfPartitions();
                if (participants != groups.size()) {
                    throw new RuntimeException(
                        "Not all DN commit successfully expected: " + groups.size() + ", actual: " + participants);
                }

                // 4. After committing trx successfully, write a record in meta-db.
                long tso = ((SyncPointTransaction) trx).getCommitTso();
                SyncPointMetaAccessor accessor = new SyncPointMetaAccessor();
                try (Connection connection = MetaDbUtil.getConnection()) {
                    accessor.setConnection(connection);
                    // Delete out-dated records.
                    accessor.delete();
                    // Insert a new record.
                    accessor.insert(uuid, participants, tso);
                }
                return true;
            }
        } catch (Throwable t) {
            logger.error("Execute sync point trx failed.", t);
        }
        return false;
    }
}
