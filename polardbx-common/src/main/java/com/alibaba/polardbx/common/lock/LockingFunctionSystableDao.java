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

package com.alibaba.polardbx.common.lock;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LockingFunctionSystableDao {
    private static final Logger logger = LoggerFactory.getLogger(LockingFunctionSystableDao.class);

    static final String COLUMN_ID = "id";
    static final String COLUMN_LOCK_NAME = "lock_name";
    static final String COLUMN_DRDS_SESSION = "drds_session";
    static final String COLUMN_NEED_LIST = "need_list";
    static final String COLUMN_GMT_MODIFIED = "gmt_modified";
    static final String COLUMN_GMT_CREATED = "gmt_created";
    static final String COLUMN_EXPIRED = "expired";

    private static final String SQL_CHECK_LOCK_HOLDER =
        "select lock_name, drds_session, (date_add(gmt_modified, interval " + LockingFunctionManager.EXPIRATION_TIME
            + " second) < now()) as expired from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " where lock_name=?";
    private static final String SQL_CHECK_LOCK_VALIDITY_FOR_UPDATE =
        "select lock_name, drds_session, gmt_modified, now() as now, (date_add(gmt_modified, interval "
            + LockingFunctionManager.EXPIRATION_TIME + " second)< now()) as expired from "
            + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " where lock_name=? for update";
    private static final String SQL_IS_MY_LOCK_VALID =
        "select lock_name, drds_session, (date_add(gmt_modified, interval " + LockingFunctionManager.EXPIRATION_TIME
            + " second)< now()) as expired from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
            + " where lock_name=? and drds_session=? for update";
    private static final String SQL_IS_MY_LOCK_VALID_WITHOUT_FOR_UPDATE =
        "select lock_name, drds_session, (date_add(gmt_modified, interval " + LockingFunctionManager.EXPIRATION_TIME
            + " second)< now()) as expired from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
            + " where lock_name=? and drds_session=?";
    private static final String SQL_CHECK_LOCK_EXISTENCE =
        "select lock_name, drds_session from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " where lock_name=?";
    private static final String SQL_GET_NEED_LIST =
        "select need_list from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " where lock_name=?";
    private static final String SQL_UPDATE_SESSION_AND_NEED_LIST =
        "update " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " set drds_session=?, need_list=? where lock_name=?";
    private static final String SQL_TRY_CREATE_LOCK =
        "insert ignore into " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
            + "  (lock_name, drds_session, gmt_created, gmt_modified) values (?, ?, now(), now())";
    private static final String SQL_DELETE_LOCK_TUPLE =
        "delete from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + "  where lock_name = ? and drds_session = ?";
    private static final String SQL_REGISTER_NEED_LIST = "update " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
        + " set need_list=concat(need_list,?) where lock_name=?";
    private static final String SQL_UPDATE_NEED_LIST =
        "update " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " set need_list=? where lock_name=?";
    private static final String SQL_TRY_SEIZE_LOCK = "update " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
        + " set drds_session=?, gmt_modified = now() where lock_name=?";
    private static final String SQL_UPDATE_MODIFIED_TIME = "update " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION
        + " set gmt_modified=now() where lock_name=? and drds_session=?";
    private static final String SQL_CHECK_LOCK =
        "select lock_name, drds_session, (date_add(gmt_modified, interval " + LockingFunctionManager.EXPIRATION_TIME
            + " second)< now()) as expired from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " where lock_name=?";

    private LockingFunctionManager manager;
    private String drdsSession;

    LockingFunctionSystableDao(LockingFunctionManager manager, String drdsSession) {
        this.manager = manager;
        this.drdsSession = drdsSession;
    }

    public boolean isMyLockValid(String lockName) throws SQLException {
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_IS_MY_LOCK_VALID_WITHOUT_FOR_UPDATE)) {
            ps.setString(1, lockName);
            ps.setString(2, drdsSession);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                return expired != 1;
            }
        }
        return false;
    }

    public boolean tryCreateLock(String lockName) throws SQLException {
        int updates = 0;
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_TRY_CREATE_LOCK)) {
            ps.setString(1, lockName);
            ps.setString(2, drdsSession);
            updates = ps.executeUpdate();
            logger.debug(connection + "try create lock: " + (updates == 1));
        }
        return updates == 1;
    }

    public boolean checkLockHolder(String lockName) throws SQLException {
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK_HOLDER)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                String session = resultSet.getString(COLUMN_DRDS_SESSION);
                return expired != 1 && session.equals(drdsSession);
            }
        }
        return false;
    }

    public boolean checkLockValidityForUpdate(Connection connectionInTransaction, String lockName)
        throws SQLException {
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_CHECK_LOCK_VALIDITY_FOR_UPDATE)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                if (expired == 1) {
                    logger.debug(drdsSession + " find the lock: " + lockName + " is expired!");
                }
                return expired != 1;
            }
        }
        return false;
    }

    public boolean trySeizeLock(Connection connectionInTransaction, String lockName) throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_TRY_SEIZE_LOCK)) {
            ps.setString(1, drdsSession);
            ps.setString(2, lockName);
            updates = ps.executeUpdate();
            if (updates == 1) {
                logger.debug(drdsSession + " seize the lock " + lockName);
            }
        }
        return updates == 1;
    }

    public int registerNeedList(Connection connectionInTransaction, String lockName) throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_REGISTER_NEED_LIST)) {
            ps.setString(1, drdsSession + ",");
            ps.setString(2, lockName);
            updates = ps.executeUpdate();
            logger.debug(drdsSession + " register");
        }
        return updates;
    }

    public void unregisterNeedList(Connection connectionInTransaction, String lockName) throws SQLException {
        String needList = null;
        try (PreparedStatement getNeedListPs = connectionInTransaction.prepareStatement(SQL_GET_NEED_LIST)) {
            getNeedListPs.setString(1, lockName);
            ResultSet rs = getNeedListPs.executeQuery();
            while (rs.next()) {
                needList = rs.getString("need_list");
            }
            logger.debug(drdsSession + " unregister");
        }

        try (PreparedStatement updateNeedListPs = connectionInTransaction.prepareStatement(SQL_UPDATE_NEED_LIST)) {
            if (needList != null) {
                int index = needList.indexOf(drdsSession + ",");
                if (index != -1) {
                    needList = needList.substring(0, index) + needList.substring(index + drdsSession.length() + 1);
                    updateNeedListPs.setString(1, needList);
                    updateNeedListPs.setString(2, lockName);
                    updateNeedListPs.executeUpdate();
                }
            }
        }
    }

    public boolean isMyLockValid(Connection connectionInTransaction, String lockName) throws SQLException {
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_IS_MY_LOCK_VALID)) {
            ps.setString(1, lockName);
            ps.setString(2, drdsSession);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                return expired != 1;
            }
        }
        return false;
    }

    public boolean grantLock(Connection connectionInTransaction, String lockName) throws SQLException {
        int updates = 0;
        String needList = null;
        try (PreparedStatement getNeedListPs = connectionInTransaction.prepareStatement(SQL_GET_NEED_LIST)) {
            getNeedListPs.setString(1, lockName);
            ResultSet resultSet = getNeedListPs.executeQuery();
            while (resultSet.next()) {
                needList = resultSet.getString(COLUMN_NEED_LIST);
            }
        }

        try (PreparedStatement updateSessionPs = connectionInTransaction
            .prepareStatement(SQL_UPDATE_SESSION_AND_NEED_LIST)) {
            if (needList != null && !needList.isEmpty()) {

                int index = needList.indexOf(",");
                String newNeedList = needList.substring(index + 1);
                String grantedSession = needList.substring(0, index);

                updateSessionPs.setString(1, grantedSession);
                updateSessionPs.setString(2, newNeedList);
                updateSessionPs.setString(3, lockName);
                updates = updateSessionPs.executeUpdate();
                if (updates == 1) {
                    logger.debug(drdsSession + " grant the lock: " + lockName + " to " + grantedSession);
                }
            }
        }
        return updates == 1;
    }

    public boolean deleteLockTuple(Connection connectionInTransaction, String lockName) throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_DELETE_LOCK_TUPLE)) {
            ps.setString(1, lockName);
            ps.setString(2, drdsSession);
            updates = ps.executeUpdate();
            logger.debug(drdsSession + " delete the lock: " + lockName);
        }
        return updates == 1;
    }

    public boolean checkLockExistence(String lockName) throws SQLException {
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK_EXISTENCE)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        }
        return false;
    }

    public Integer isFreeLock(String lockName) throws SQLException {
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK)) {
            ps.setString(1, lockName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int expired = rs.getInt(COLUMN_EXPIRED);
                return expired == 1 ? 1 : 0;
            }
            return 1;
        }
    }

    public String isUsedLock(String lockName) throws SQLException {
        try (Connection connection = manager.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK)) {
            ps.setString(1, lockName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int expired = rs.getInt(COLUMN_EXPIRED);
                String session = rs.getString(COLUMN_DRDS_SESSION);
                return expired == 1 ? null : session;
            }
            return null;
        }
    }

    public Integer updateModifiedTime(Connection connection, String lockName) throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connection.prepareStatement(SQL_UPDATE_MODIFIED_TIME)) {
            ps.setString(1, lockName);
            ps.setString(2, drdsSession);
            updates = ps.executeUpdate();
            if (updates == 1) {
                logger.debug("heartbeat for lock: " + lockName + ", session: " + drdsSession);
            }
        }
        return updates;
    }

}
