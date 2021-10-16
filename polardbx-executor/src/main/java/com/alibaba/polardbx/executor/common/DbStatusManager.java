/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author shicai.xsc 2018/9/5 下午4:31
 * @since 5.0.0.0
 */
public class DbStatusManager {

    public enum DbReadOnlyStatus {
        WRITEABLE, READONLY_PREPARE, READONLY_COMMIT
    }

    public static final String DRDS_DB_STATUS = SystemTables.DRDS_DB_STATUS;
    private static int CHECK_DB_LOCK_PERIOD = 5;
    private static int PREPARE_LOCK_TIME = 30;
    private static int MAX_LOCK_TIME = 1800;
    private static final String CREATE_DRDS_TABLE_STATUS = "CREATE TABLE IF NOT EXISTS `"
        + DRDS_DB_STATUS
        + "` ("
        + "  `id` int(20) unsigned NOT NULL AUTO_INCREMENT,"
        + "  `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
        + "  `gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',\n"
        + " `db_name` varchar(64) NOT NULL DEFAULT '',"
        + " `tb_name` varchar(64) NOT NULL DEFAULT '', "
        + " `expire_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,"
        + " `status` tinyint(4) NOT NULL,"
        + "  PRIMARY KEY (`id`),"
        + "  UNIQUE KEY `unique_index` (`db_name`, `tb_name`)"
        + ");";
    private final String INSERT_SQL = "INSERT INTO "
        + DRDS_DB_STATUS
        + " VALUES "
        + "(null, now(), now(), ''{0}'', ''{1}'', ''{2}'', 1) on duplicate key update "
        + "gmt_modified = now()";
    // Add expire_time as a where condition for DELETE sql to make sure DELETE
    // won't work if expire_time updated
    private final String DELETE_WITH_EXPIRETIME_SQL = "DELETE FROM "
        + DRDS_DB_STATUS
        + " WHERE db_name = ''{0}'' AND tb_name = ''{1}'' AND expire_time = ''{2}'' ";
    private final String DELETE_SQL = "DELETE FROM "
        + DRDS_DB_STATUS
        + " WHERE db_name = ''{0}'' AND tb_name = ''{1}'' ";
    private final String UPDATE_SQL = "UPDATE "
        + DRDS_DB_STATUS
        + " SET expire_time=''{0}'' WHERE db_name = ''{1}'' AND tb_name = ''{2}'' ";
    private final String SELECT_ALL_SQL = "SELECT * FROM " + DRDS_DB_STATUS;
    private final String SELECT_SQL = "SELECT * FROM "
        + DRDS_DB_STATUS
        + " WHERE db_name = ''{0}'' AND tb_name = ''{1}'' ";
    private final String SELECT_TIME_SQL = "SELECT (NOW() + INTERVAL %d SECOND)";
    private ConcurrentHashMap<String, Boolean> readOnlyCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DataSource> dataSourceMap = new ConcurrentHashMap<>();
    private static DbStatusManager instance = new DbStatusManager();
    private final Logger logger = LoggerFactory.getLogger(DbStatusManager.class);
    private ScheduledExecutorService executorService;
    private volatile boolean checkTaskRunning = false;

    public static DbStatusManager getInstance() {
        return instance;
    }

    public boolean isDbReadOnly(String database) {
        String key = database.toLowerCase() + ".*";
        return readOnlyCache.size() > 0 && readOnlyCache.containsKey(key);
    }

    public void init(DataSource dataSource, String schema) {
        if (ConfigDataMode.isFastMock()) {
            // Avoid check dblock in mock mode
            return;
        }

        try {
            dataSourceMap.put(schema.toLowerCase(), dataSource);
            createTableIfNotExist(dataSource);

            // first add all readOnlyCache to allTmpReadOnlyCache
            ConcurrentHashMap<String, Boolean> allTmpReadOnlyCache = new ConcurrentHashMap<>();
            addOneDbCache(allTmpReadOnlyCache, readOnlyCache);
            ConcurrentHashMap<String, Boolean> tmpReadOnlyCache = initDbLockStatus(dataSource);
            addOneDbCache(allTmpReadOnlyCache, tmpReadOnlyCache);
            setReadOnlyCacheRef(allTmpReadOnlyCache);

            startCheckDbLockTask();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DB_STATUS_EXECUTE, e);
        }
    }

    public void setDataSource(DataSource dataSource, String schema) {
        dataSourceMap.put(schema.toLowerCase(), dataSource);
    }

    public void removeDataSource(String schema) {
        dataSourceMap.remove(schema.toLowerCase());
    }

    public synchronized long setDbStatus(String database, DbReadOnlyStatus status, int time, Date currentExpireTime)
        throws SQLException {
        DataSource dataSource = dataSourceMap.get(database.toLowerCase());
        Connection connection = null;
        Statement statement = null;
        String table = "*";
        try {
            Date expireTimeToSet = null;
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            switch (status) {
            case WRITEABLE:
                String deleteSql = MessageFormat.format(DELETE_SQL, database, table);
                if (currentExpireTime != null) {
                    deleteSql = MessageFormat.format(DELETE_WITH_EXPIRETIME_SQL,
                        database,
                        table,
                        currentExpireTime.toString());
                }
                statement.execute(deleteSql);
                break;
            case READONLY_PREPARE:
                time = time <= 0 ? PREPARE_LOCK_TIME : time;
                expireTimeToSet = generateTimeFromDb(dataSource, time);
                String insertSql = MessageFormat.format(INSERT_SQL, database, table, expireTimeToSet.toString());
                statement.execute(insertSql);
                break;
            case READONLY_COMMIT:
                time = time <= 0 ? MAX_LOCK_TIME : time;
                expireTimeToSet = generateTimeFromDb(dataSource, time);
                String updateSql = MessageFormat.format(UPDATE_SQL, expireTimeToSet.toString(), database, table);
                statement.execute(updateSql);
                break;
            default:
                break;
            }

            if (status != DbReadOnlyStatus.WRITEABLE && !checkDbLockExist(dataSource, database)) {
                throw new SQLException("Lock failed, no lock record found in __drds_db_status__");
            }

            // currentExpireTime == null when this is called by user manually
            if (status == DbReadOnlyStatus.WRITEABLE && currentExpireTime == null
                && checkDbLockExist(dataSource, database)) {
                throw new SQLException("Unlock failed, lock record found in __drds_db_status__");
            }

            // update cache immediately
            setReadOnlyCache(readOnlyCache, database, table, status != DbReadOnlyStatus.WRITEABLE);

            return statement.getUpdateCount();
        } finally {
            // datasource should be closed by the caller
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void setReadOnlyCache(ConcurrentHashMap<String, Boolean> cache, String database, String table,
                                  boolean readOnly) {
        table = StringUtils.isBlank(table) ? "*" : table;
        String key = database.toLowerCase() + '.' + table.toLowerCase();

        if (readOnly) {
            cache.putIfAbsent(key, readOnly);
        } else {
            cache.remove(key);
        }
    }

    private boolean checkDbLockExist(DataSource dataSource, String database) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        String table = "*";
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            String selectSql = MessageFormat.format(SELECT_SQL, database, table);
            ResultSet rs = statement.executeQuery(selectSql);
            return rs.next();
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private ConcurrentHashMap<String, Boolean> initDbLockStatus(DataSource dataSource) throws SQLException,
        TddlNestableRuntimeException {
        Connection connection = null;
        Statement statement = null;
        List<Triple> expiredLocks = new ArrayList();
        ConcurrentHashMap<String, Boolean> tmpReadOnlyCache = new ConcurrentHashMap<>();

        try {
            Date now = generateTimeFromDb(dataSource, 0);
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(SELECT_ALL_SQL);
            while (rs.next()) {
                String database = rs.getString("db_name");
                String table = rs.getString("tb_name");
                int status = rs.getInt("status");

                // check if expired
                Date expireTime = rs.getTimestamp("expire_time");
                if (expireTime.before(now)) {
                    expiredLocks.add(new ImmutableTriple(database, table, expireTime));
                } else {
                    setReadOnlyCache(tmpReadOnlyCache, database, table, status != 0);
                }
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        // remove expired locks
        for (Triple triple : expiredLocks) {
            setDbStatus((String) triple.getLeft(), DbReadOnlyStatus.WRITEABLE, -1, (Date) triple.getRight());
        }

        return tmpReadOnlyCache;
    }

    private void startCheckDbLockTask() {
        if (!checkTaskRunning) {
            executorService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("checkDbLock", true));
            executorService.scheduleWithFixedDelay(() -> {
                try {
                    ConcurrentHashMap<String, Boolean> allTmpReadOnlyCache = new ConcurrentHashMap<>();
                    for (DataSource dataSource : dataSourceMap.values()) {
                        ConcurrentHashMap<String, Boolean> tmpReadOnlyCache = initDbLockStatus(dataSource);
                        addOneDbCache(allTmpReadOnlyCache, tmpReadOnlyCache);
                    }

                    setReadOnlyCacheRef(allTmpReadOnlyCache);
                } catch (Throwable e) {
                    logger.error("Failed in startCheckDbLockTask", e);
                }
            }, CHECK_DB_LOCK_PERIOD, CHECK_DB_LOCK_PERIOD, TimeUnit.SECONDS);
            checkTaskRunning = true;
        }
    }

    private void createTableIfNotExist(DataSource dataSource) throws SQLException, TddlNestableRuntimeException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(CREATE_DRDS_TABLE_STATUS);
        } catch (Exception e) {
            logger.warn("Failed init db status table", e);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private ConcurrentHashMap<String, Boolean> addOneDbCache(ConcurrentHashMap<String, Boolean> allDbCache,
                                                             ConcurrentHashMap<String, Boolean> oneDbCache) {
        if (oneDbCache != null && oneDbCache.keys() != null) {
            for (Map.Entry<String, Boolean> entry : oneDbCache.entrySet()) {
                allDbCache.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        return allDbCache;
    }

    private synchronized void setReadOnlyCacheRef(ConcurrentHashMap<String, Boolean> tmpReadOnlyCache) {
        this.readOnlyCache = tmpReadOnlyCache;
    }

    private Date generateTimeFromDb(DataSource dataSource, int addSeconds) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        Date time = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            String sql = String.format(SELECT_TIME_SQL, addSeconds);
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                time = rs.getTimestamp(1);
                return time;
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }

        return time;
    }

    /**
     * For test usage
     */
    public void resetForTest(int checkDbLockPeriod, int prepareLockTime, int maxLockTime) throws Exception {
        CHECK_DB_LOCK_PERIOD = checkDbLockPeriod;
        PREPARE_LOCK_TIME = prepareLockTime;
        MAX_LOCK_TIME = maxLockTime;
        executorService.shutdown();
        while (!executorService.isTerminated()) {
            Thread.sleep(1000);
        }

        checkTaskRunning = false;
        startCheckDbLockTask();
    }
}
