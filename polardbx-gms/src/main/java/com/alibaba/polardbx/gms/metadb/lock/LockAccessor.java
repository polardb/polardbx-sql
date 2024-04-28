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

package com.alibaba.polardbx.gms.metadb.lock;

import com.alibaba.polardbx.common.lock.LockingConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class LockAccessor extends AbstractAccessor {

    private static final Logger logger = LoggerFactory.getLogger(LockAccessor.class);

    /**
     * columns or alias in locks.
     */
    public static final String COLUMN_ID = "id";
    public static final String COLUMN_LOCK_NAME = "lock_name";
    public static final String COLUMN_SESSION_ID = "session_id";
    public static final String COLUMN_NEED_LIST = "need_list";
    public static final String COLUMN_GMT_MODIFIED = "gmt_modified";
    public static final String COLUMN_GMT_CREATED = "gmt_created";
    public static final String COLUMN_EXPIRED = "expired";

    /**
     * sqls to process the lock requirement
     */
    private static final String SQL_CHECK_LOCK_HOLDER =
        "select lock_name, session_id, (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME
            + " second) < now()) as expired from " + GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name=?";
    private static final String SQL_CHECK_LOCK_VALIDITY_FOR_UPDATE =
        "select lock_name, session_id, gmt_modified, now() as now, (date_add(gmt_modified, interval "
            + LockingConfig.EXPIRATION_TIME + " second)< now()) as expired from "
            + GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name=? for update";
    private static final String SQL_IS_MY_LOCK_VALID =
        "select lock_name, session_id, (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME
            + " second)< now()) as expired from " + GmsSystemTables.LOCKING_FUNCTIONS
            + " where lock_name=? and session_id=? for update";
    private static final String SQL_IS_MY_LOCK_VALID_WITHOUT_FOR_UPDATE =
        "select lock_name, session_id, (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME
            + " second)< now()) as expired from " + GmsSystemTables.LOCKING_FUNCTIONS
            + " where lock_name=? and session_id=?";
    private static final String SQL_CHECK_LOCK_EXISTENCE =
        "select lock_name, session_id from " + GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name=?";
    private static final String SQL_GET_NEED_LIST =
        "select need_list from " + GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name=?";
    private static final String SQL_UPDATE_SESSION_AND_NEED_LIST =
        "update " + GmsSystemTables.LOCKING_FUNCTIONS + " set session_id=?, need_list=? where lock_name=?";
    private static final String SQL_UPDATE_SESSION =
        "update " + GmsSystemTables.LOCKING_FUNCTIONS + " set session_id=? where lock_name=? and session_id=?";
    private static final String SQL_TRY_CREATE_LOCK =
        "insert ignore into " + GmsSystemTables.LOCKING_FUNCTIONS
            + "  (lock_name, session_id, gmt_created, gmt_modified) values (?, ?, now(), now())";
    private static final String SQL_DELETE_LOCK_TUPLE =
        "delete from " + GmsSystemTables.LOCKING_FUNCTIONS + "  where lock_name = ? and session_id = ?";
    private static final String SQL_REGISTER_NEED_LIST = "update " + GmsSystemTables.LOCKING_FUNCTIONS
        + " set need_list=concat(need_list,?) where lock_name=?";
    private static final String SQL_UPDATE_NEED_LIST =
        "update " + GmsSystemTables.LOCKING_FUNCTIONS + " set need_list=? where lock_name=?";
    private static final String SQL_TRY_SEIZE_LOCK = "update " + GmsSystemTables.LOCKING_FUNCTIONS
        + " set session_id=?, gmt_modified = now() where lock_name=?";
    private static final String SQL_UPDATE_MODIFIED_TIME = "update " + GmsSystemTables.LOCKING_FUNCTIONS
        + " set gmt_modified=now() where lock_name=? and session_id=?";
    private static final String SQL_CHECK_LOCK =
        "select lock_name, session_id, (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME
            + " second)< now()) as expired from " + GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name=?";
    private static final String SQL_GET_RELEASE_LOCKS =
        "select lock_name from " + GmsSystemTables.LOCKING_FUNCTIONS + " where session_id is null";
    private static final String SQL_GET_EXPIRED_LOCKS = "select lock_name from " + GmsSystemTables.LOCKING_FUNCTIONS
        + " where (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME + " second) < now())";
    public static final String SQL_UPDATE_MODIFIED_TIME_BY_SESSION = "update " + GmsSystemTables.LOCKING_FUNCTIONS
        + " set gmt_modified=now() where session_id=?";
    public static final String SQL_DELETE_SESSION_LOCKS =
        "delete from " + GmsSystemTables.LOCKING_FUNCTIONS + " where session_id=?";
    public static final String SQL_SELECT_SESSION_LOCKS =
        "select lock_name from " + GmsSystemTables.LOCKING_FUNCTIONS + " where session_id=?";
    private static final String SQL_CHECK_LOCK_VALID_AND_UPDATE = "update " + GmsSystemTables.LOCKING_FUNCTIONS
        + " set gmt_modified=now() where lock_name=? and session_id=? "
        + "and (date_add(gmt_modified, interval " + LockingConfig.EXPIRATION_TIME + " second) >= now())";

    public LockAccessor() {

    }

    public int checkLockValidAndUpdate(String lockName, String sessionID) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK_VALID_AND_UPDATE)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            return ps.executeUpdate();
        }
    }

    /**
     * if the lock had a tuple in system table and was not expired, it's valid.
     * without tuple lock.
     */
    public boolean isMyLockValid(String lockName, String sessionID) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_IS_MY_LOCK_VALID_WITHOUT_FOR_UPDATE)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                return expired != 1;
            }
        }
        return false;
    }

    public List<String> getAllMyLocks(String sessionID) throws SQLException {
        List<String> res = new ArrayList<>();
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_SELECT_SESSION_LOCKS)) {
            ps.setString(1, sessionID);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String lockName = resultSet.getString(COLUMN_LOCK_NAME);
                res.add(lockName);
            }
        }
        return res;
    }

    /**
     * try to insert a new lock tuple, using 'insert ignore'
     */
    public boolean tryCreateLock(String lockName, String sessionID) throws SQLException {
        int updates = 0;
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_TRY_CREATE_LOCK)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            updates = ps.executeUpdate();
            logger.debug(connection + "try create lock: " + (updates == 1));
        }
        return updates == 1;
    }

    public boolean checkLockHolder(String lockName, String sessionID) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK_HOLDER)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                String session = resultSet.getString(COLUMN_SESSION_ID);
                return expired != 1 && sessionID.equals(session);
            }
        }
        return false;
    }

    /**
     * check if tuple with the lock name is valid.
     * invalid if tuple is expired or disappeared.
     *
     * @return true: valid; false: invalid
     */
    public boolean checkLockValidityForUpdate(Connection connectionInTransaction, String lockName)
        throws SQLException {
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_CHECK_LOCK_VALIDITY_FOR_UPDATE)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String sessionID = resultSet.getString(COLUMN_SESSION_ID);
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                return expired != 1 && sessionID != null;
            }
        }
        return false;
    }

    /**
     * replace the old session with the drds session.
     */
    public boolean trySeizeLock(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_TRY_SEIZE_LOCK)) {
            ps.setString(1, sessionID);
            ps.setString(2, lockName);
            updates = ps.executeUpdate();
            if (updates == 1) {
                logger.debug(sessionID + " seize the lock " + lockName);
            }
        }
        return updates == 1;
    }

    /**
     * register the session to the need list.
     */
    public int registerNeedList(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_REGISTER_NEED_LIST)) {
            ps.setString(1, sessionID + ",");
            ps.setString(2, lockName);
            updates = ps.executeUpdate();
            logger.debug(sessionID + " register");
        }
        return updates;
    }

    /**
     * remove the session from the need list.
     */
    public void unregisterNeedList(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
        String needList = null;
        try (PreparedStatement getNeedListPs = connectionInTransaction.prepareStatement(SQL_GET_NEED_LIST)) {
            getNeedListPs.setString(1, lockName);
            ResultSet rs = getNeedListPs.executeQuery();
            while (rs.next()) {
                needList = rs.getString("need_list");
            }
            logger.debug(sessionID + " unregister");
        }

        try (PreparedStatement updateNeedListPs = connectionInTransaction.prepareStatement(SQL_UPDATE_NEED_LIST)) {
            if (needList != null) {
                int index = needList.indexOf(sessionID + ",");
                if (index != -1) {
                    needList = needList.substring(0, index) + needList.substring(index + sessionID.length() + 1);
                    updateNeedListPs.setString(1, needList);
                    updateNeedListPs.setString(2, lockName);
                    updateNeedListPs.executeUpdate();
                }
            }
        }
    }

    /**
     * check if the lock that hold by current session is valid
     */
    public boolean isMyLockValid(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_IS_MY_LOCK_VALID)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int expired = resultSet.getInt(COLUMN_EXPIRED);
                return expired != 1;
            }
        }
        return false;
    }

    /**
     * when going to release the lock, grant the lock to the session from need list.
     */
    public boolean grantLock(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
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
                // update the need list and drds session
                int index = needList.indexOf(",");
                String newNeedList = needList.substring(index + 1);
                String grantedSession = needList.substring(0, index);

                updateSessionPs.setString(1, grantedSession);
                updateSessionPs.setString(2, newNeedList);
                updateSessionPs.setString(3, lockName);
                updates = updateSessionPs.executeUpdate();
                if (updates == 1) {
                    logger.debug(sessionID + " grant the lock: " + lockName + " to " + grantedSession);
                }
            }
        }
        return updates == 1;
    }

    /**
     * releaseLock之前一定需要isMyLockValid
     */
    public boolean releaseLock(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
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
            .prepareStatement(SQL_UPDATE_SESSION)) {
            if (needList != null && !needList.isEmpty()) {
                // update the session
                updateSessionPs.setString(1, null);
                updateSessionPs.setString(2, lockName);
                updateSessionPs.setString(3, sessionID);
                updates = updateSessionPs.executeUpdate();
            }
        }
        return updates == 1;
    }

    /**
     * delete a lock's tuple when releasing.
     */
    public boolean deleteLockTuple(Connection connectionInTransaction, String lockName, String sessionID)
        throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_DELETE_LOCK_TUPLE)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            updates = ps.executeUpdate();
            logger.debug(sessionID + " delete the lock: " + lockName);
        }
        return updates == 1;
    }

    /**
     * check if lock exists.
     */
    public boolean checkLockExistence(String lockName) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK_EXISTENCE)) {
            ps.setString(1, lockName);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                return true;
            }
        }
        return false;
    }

    /**
     * is lock released.
     *
     * @return 1 if free, or 0 if in use.
     */
    public Integer isFreeLock(String lockName) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK)) {
            ps.setString(1, lockName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int expired = rs.getInt(COLUMN_EXPIRED);
                String session = rs.getString(COLUMN_SESSION_ID);
                return (expired == 1 || session == null) ? 1 : 0;
            }
            return 1;
        }
    }

    /**
     * is lock in use
     *
     * @return the session holding the lock, or null.
     */
    public String isUsedLock(String lockName) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_CHECK_LOCK)) {
            ps.setString(1, lockName);
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int expired = rs.getInt(COLUMN_EXPIRED);
                String session = rs.getString(COLUMN_SESSION_ID);
                return (expired == 1 || session == null) ? null : session;
            }
            return null;
        }
    }

    /**
     * update the last modified time to maintain the validity of lock.
     */
    public Integer updateModifiedTime(Connection connection, String lockName, String sessionID) throws SQLException {
        int updates = 0;
        try (PreparedStatement ps = connection.prepareStatement(SQL_UPDATE_MODIFIED_TIME)) {
            ps.setString(1, lockName);
            ps.setString(2, sessionID);
            updates = ps.executeUpdate();
            if (updates == 1) {
                logger.debug("heartbeat for lock: " + lockName + ", sessionID: " + sessionID);
            }
        }
        return updates;
    }

    public Set<String> getReleaseLocks() throws SQLException {
        Set<String> releaseLock = new HashSet<>();
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_GET_RELEASE_LOCKS)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String lockName = rs.getString(COLUMN_LOCK_NAME);
                releaseLock.add(lockName);
            }
        }
        return releaseLock;
    }

    public Set<String> getExpireLocks() throws SQLException {
        Set<String> releaseLock = new HashSet<>();
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_GET_EXPIRED_LOCKS)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String lockName = rs.getString(COLUMN_LOCK_NAME);
                releaseLock.add(lockName);
            }
        }
        return releaseLock;
    }

    public int[] sessionBatchHeartbeat(Collection<String> sessionIds) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_UPDATE_MODIFIED_TIME_BY_SESSION)) {
            for (String session : sessionIds) {
                ps.setString(1, session);
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

    public int deleteOldSessionRemainingLocks(String sessionID) throws SQLException {
        try (Connection connection = MetaDbUtil.getConnection();
            PreparedStatement ps = connection.prepareStatement(SQL_DELETE_SESSION_LOCKS)) {
            ps.setString(1, sessionID);
            return ps.executeUpdate();
        }
    }

}
