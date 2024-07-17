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

package com.alibaba.polardbx.server.lock;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.lock.LockingConfig;
import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.lock.LockAccessor;
import com.alibaba.polardbx.net.AbstractConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class PolarDBXLockingFunctionHandle implements LockingFunctionHandle {
    private static final Logger logger = LoggerFactory.getLogger(LockingFunctionHandle.class);
    /**
     * sql to retrieve the deadlock states.
     */
    private static final String SQL_GET_DEADLOCK_SNAPSHOT =
        "select lock_name, session_id, need_list from " + GmsSystemTables.LOCKING_FUNCTIONS + " for update";

    private LockingFunctionManager manager;

    /**
     * drds connection and session binding to this handle
     */
    private FrontendConnection sessionConn;
    private String sessionID;
    private LockAccessor dao;

    /**
     * the reentry times of the lock (lock name - reentry times)
     * can't maintain the consistency of cache, so we should check tuple in mysql to validate it.
     */
    private Map<String, Integer> reentries;

    public PolarDBXLockingFunctionHandle(LockingFunctionManager manager, FrontendConnection connection,
                                         String sessionID) {
        this.manager = manager;
        this.sessionConn = connection;
        this.reentries = new HashMap<>();
        this.sessionID = sessionID;
        this.dao = new LockAccessor();
        try {
            dao.deleteOldSessionRemainingLocks(sessionID);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public Integer tryAcquireLock(final String lockName, final int timeout) {
        try {
            // timeout second -> millis second. if timeout<0, try infinitely.
            long restTime = timeout < 0 ? Integer.MAX_VALUE : (long) timeout * 1000;
            return tryAcquireLockInner(lockName, restTime, 0);
        } catch (Exception e) {
            if (e instanceof TddlRuntimeException
                && ((TddlRuntimeException) e).getErrorCodeType() == ErrorCode.ERR_USER_LOCK_DEADLOCK) {
                throw (TddlRuntimeException) e;
            }
            logger.error(e.getMessage(), e);
            return null;

        }

    }

    /**
     * try to acquire the lock, following the steps:
     * 1. check local cache, if failed:
     * 2. create new lock, if failed:
     * 3. freeze the table and get snapshot to check deadlock, if no deadlock:
     * 4. try to continuously acquire the lock within the timeout.
     *
     * @param restTime milliseconds , positive
     */
    private Integer tryAcquireLockInner(final String lockName, final long restTime, final int retryTime)
        throws SQLException, InterruptedException {
        if (restTime <= 0 || retryTime > LockingConfig.MAX_RETRY_TIMES) {
            return 0;
        }
        long startTs = System.currentTimeMillis();
        /*
         * check local cache that if lock has been seized or not. if so, update reentry times.
         */
        Integer reentryTimes = 0;
        if ((reentryTimes = reentries.get(lockName)) != null) {
            // check if local cache is invalid or not
            if (dao.isMyLockValid(lockName, sessionID)) {
                // local cache is valid
                reentries.put(lockName, reentryTimes + 1);
                return 1;
            } else {
                // local cache is invalid
                reentries.remove(lockName);
                GeneralUtil.nestedException(
                    "the lock: " + lockName + " has been abnormally lost. please try again to get this lock.");
            }
        }

        /*
         * try to create a lock
         */
        boolean successToCreateLock = false;
        try {
            successToCreateLock = dao.tryCreateLock(lockName, sessionID);
        } catch (SQLException e) {
            // retry when innodb deadlock occur
            if (e.getMessage().toLowerCase().contains("deadlock")) {
                logger.info("innodb deadlock occur, retry", e);
                return tryAcquireLockInner(lockName,
                    restTime - (System.currentTimeMillis() - startTs), retryTime + 1);
            }
            throw e;
        }
        if (successToCreateLock) {
            // success to get lock: post handle
            // start heartbeat
            postHandleAfterGetLock(lockName);
            return 1;
        }

        /*
         * deadlock avoidance: by table lock
         */
        boolean deadlock = false;
        boolean unRepeatedRead = false;
        try (Connection connectionInTransaction = manager.getConnection()) {
            try {
                connectionInTransaction.setAutoCommit(false);
                // get mysql table lock
                DeadlockAvoidanceHelper helper = new DeadlockAvoidanceHelper(connectionInTransaction, lockName);
                if (!(unRepeatedRead = helper.unRepeatedRead()) && !(deadlock = helper.tryLockAndDetect())) {
                    // register drds_session to lock-tuple, means that I am waiting for this lock
                    dao.registerNeedList(connectionInTransaction, lockName, sessionID);
                }
                // release mysql table lock
                connectionInTransaction.commit();
            } catch (SQLException e) {
                connectionInTransaction.rollback();
                throw e;
            } finally {
                connectionInTransaction.setAutoCommit(true);
            }
        }

        /*
         * handle unrepeated read:
         * insert ignore fail because tuple exists -> tuple delete -> cannot find this tuple when deadlock detection
         */
        if (unRepeatedRead) {
            logger.info("retry the acquisition!");
            return tryAcquireLockInner(lockName,
                restTime - (System.currentTimeMillis() - startTs), retryTime + 1);
        }
        if (deadlock) {
            throw new TddlRuntimeException(ErrorCode.ERR_USER_LOCK_DEADLOCK);
        }

        /*
         * try to seize the lock
         */
        return tryAcquireInnerWithinTimeout(lockName, restTime - (System.currentTimeMillis() - startTs));
    }

    /**
     * release the lock with lock name.
     * 1. check the local cache. if the lock is valid, count down the reentry times.
     * 2. if the lock in local cache is counted down to < 0 or invalid, release the lock and grant the lock to the session in need.
     */
    @Override
    public Integer release(String lockName) {
        for (int i = 0; ; i++) {
            try {
                return releaseInner(lockName);
            } catch (SQLException e) {
                if (i < LockingConfig.MAX_RETRY_TIMES && e.getMessage().toLowerCase().contains("deadlock")) {
                    logger.info("innodb deadlock occur, retry", e);
                    continue;
                }
                throw new TddlNestableRuntimeException(e);
            }
        }
    }

    public Integer releaseInner(String lockName) throws SQLException {
        Integer releaseResult = null;
        Integer reentryTimes;
        if ((reentryTimes = reentries.get(lockName)) != null) {
            // begin transaction
            try (Connection connectionInTransaction = manager.getConnection()) {
                try {
                    connectionInTransaction.setAutoCommit(false);

                    // check if local cache is invalid or not
                    if (dao.isMyLockValid(connectionInTransaction, lockName, sessionID)) {
                        // local cache is valid
                        reentryTimes--;
                        if (reentryTimes >= 0) {
                            reentries.put(lockName, reentryTimes);
                            releaseResult = 1;
                        } else {
                            // check need_list and grant lock. or delete lock tuple.
                            releaseResult =
                                (
                                    dao.releaseLock(connectionInTransaction, lockName, sessionID)
                                        || dao.deleteLockTuple(
                                        connectionInTransaction, lockName, sessionID)) ? 1 : 0;
                            reentries.remove(lockName);
                        }
                    } else {
                        // local cache is invalid
                        reentries.remove(lockName);
                        releaseResult = 0;
                    }

                    connectionInTransaction.commit();
                } catch (SQLException e) {
                    connectionInTransaction.rollback();
                    throw e;
                } finally {
                    connectionInTransaction.setAutoCommit(true);
                }
            }
        } else {
            // check if this lock exists
            boolean exists = dao.checkLockExistence(lockName);
            releaseResult = exists ? 0 : null;
        }
        return releaseResult;
    }

    /**
     * release all locks (when connection close)
     */
    @Override
    public Integer releaseAllLocks() {
        Integer count = 0;
        try (Connection connection = manager.getConnection()) {
            count = releaseAllLocks(connection);
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
        return count;
    }

    /**
     * is lock released.
     *
     * @return 1 if free, or 0 if in use.
     */
    @Override
    public Integer isFreeLock(String lockName) {
        try {
            return dao.isFreeLock(lockName);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            return null;
        }

    }

    /**
     * is lock in use
     *
     * @return the session holding the lock, or null.
     */
    @Override
    public String isUsedLock(String lockName) {
        try {
            return dao.isUsedLock(lockName);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new TddlNestableRuntimeException(e);
        }
    }

    private boolean exceedMaxActiveLockCounts() {
        return reentries.size() + 1 > LockingConfig.MAX_LOCKS_NUMBER;
    }

    private Integer releaseAllLocks(Connection connectionInTransaction) throws SQLException {
        int count = 0;
        for (Iterator iter = reentries.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
            String lockName = entry.getKey();
            Integer reentryTimes = entry.getValue();
            // begin transaction
            try {
                connectionInTransaction.setAutoCommit(false);
                // if the lock in local cache is valid, try to grant the lock to other session. Or, delete the lock.
                if (dao.isMyLockValid(connectionInTransaction, lockName, sessionID) && !dao
                    .releaseLock(connectionInTransaction, lockName, sessionID)) {
                    dao.deleteLockTuple(connectionInTransaction, lockName, sessionID);
                }
                connectionInTransaction.commit();
            } catch (SQLException e) {
                connectionInTransaction.rollback();
                throw e;
            } finally {
                connectionInTransaction.setAutoCommit(true);
            }
            // remove the lock in local cache.
            iter.remove();
            count += (reentryTimes + 1);
        }
        return count;
    }

    /**
     * init the reentry map and start heartbeat after getting lock for the first time.
     */
    private void postHandleAfterGetLock(String lockName) {
        // init local lock cache
        reentries.put(lockName, 0);
        // check if the lock number is under the maximum lock number.
        if (exceedMaxActiveLockCounts()) {
            release(lockName);
            GeneralUtil.nestedException(
                "the lock number exceeds the maximum lock number " + LockingConfig.MAX_LOCKS_NUMBER);
        }
        manager.startHeartBeat();
    }

    /**
     * try to get the lock within the timeout
     */
    private Integer tryAcquireInnerWithinTimeout(final String lockName, long restTime)
        throws SQLException, InterruptedException {
        boolean successToGetLock = false;
        while (!successToGetLock) {
            long startTime = System.currentTimeMillis();
            if (Thread.interrupted()) {
                throw GeneralUtil.nestedException("interrupted");
            }
            try (Connection connectionInTransaction = manager.getConnection()) {
                try {
                    connectionInTransaction.setAutoCommit(false);
                    if (restTime <= 0) {
                        // if there is no rest time to try, unregister from need_list and break
                        dao.unregisterNeedList(connectionInTransaction, lockName, sessionID);
                        break;
                    }
                    if (!dao.checkLockValidityForUpdate(connectionInTransaction, lockName) && dao.trySeizeLock(
                        connectionInTransaction, lockName, sessionID)) {
                        // success to seize
                        // unregister
                        dao.unregisterNeedList(connectionInTransaction, lockName, sessionID);
                        successToGetLock = true;
                    }
                    // release mysql record lock
                    connectionInTransaction.commit();
                } catch (SQLException e) {
                    connectionInTransaction.rollback();
                    throw e;
                } finally {
                    connectionInTransaction.setAutoCommit(true);
                }
            }

            if (!successToGetLock) {
                Semaphore semaphore = new Semaphore(0);
                manager.addWaitingLock(lockName, semaphore);
                long waitTime = restTime - (System.currentTimeMillis() - startTime);
                //当被唤醒的时候，再去尝试获取锁
                if (!semaphore.tryAcquire(waitTime, TimeUnit.MILLISECONDS)) {
                    manager.removeWaitingLock(lockName, semaphore);
                }
            }

            restTime -= (System.currentTimeMillis() - startTime);
        }
        if (successToGetLock) {
            // start heartbeat
            postHandleAfterGetLock(lockName);
        }
        return successToGetLock ? 1 : 0;
    }

    @Override
    public Integer isMyLockValid(String lockName) {
        try {
            return dao.checkLockValidAndUpdate(lockName, sessionID);
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    @Override
    public List<String> getAllMyLocks() {
        try {
            return dao.getAllMyLocks(sessionID);
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public boolean isSessionClosed() {
        return sessionConn == null || sessionConn.isClosed();
    }

    public int getHoldingLockSize() {
        return reentries.size();
    }

    public Set<String> getHoldingLocks() {
        return Collections.unmodifiableSet(reentries.keySet());
    }

    private String getHeartbeatTag(String lockName) {
        return sessionID + "_" + lockName;
    }

    public String getSessionID() {
        return sessionID;
    }

    private class DeadlockAvoidanceHelper {
        private String tryAcquiredLockName;

        /**
         * map of (session - map of (lock - allocated (always 1)))
         */
        private Map<String, Map<String, Integer>> allocationMatrix;

        /**
         * map of (session - lock (needed))
         */
        private Map<String, String> needMatrix;

        /**
         * map of (lock - available(0 or 1))
         */
        private Map<String, Integer> availableList;

        private Set<String> sessions;

        private Connection connectionInTransaction;

        DeadlockAvoidanceHelper(Connection connectionInTransaction, String tryAcquiredLockName) throws SQLException {
            this.connectionInTransaction = connectionInTransaction;
            this.tryAcquiredLockName = tryAcquiredLockName;
            // init
            this.availableList = new HashMap<>();
            this.needMatrix = new HashMap<>();
            this.allocationMatrix = new HashMap<>();
            this.sessions = new HashSet<>();
            // scan table and get matrix.
            // must get record lock for all tuples.
            try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_GET_DEADLOCK_SNAPSHOT)) {
                ResultSet resultSet = ps.executeQuery();
                while (resultSet.next()) {
                    String lockName = resultSet.getString(LockAccessor.COLUMN_LOCK_NAME);
                    String session = resultSet.getString(LockAccessor.COLUMN_SESSION_ID);
                    String needList = resultSet.getString(LockAccessor.COLUMN_NEED_LIST);

                    // init available list
                    availableList.put(lockName, session == null ? 1 : 0);
                    if (session != null) {
                        sessions.add(session);
                        // init allocation matrix
                        Map<String, Integer> allocationList = null;
                        if ((allocationList = allocationMatrix.get(session)) != null) {
                            allocationList.put(lockName, 1);
                        } else {
                            allocationList = new HashMap<>();
                            allocationList.put(lockName, 1);
                            allocationMatrix.put(session, allocationList);
                        }
                    }

                    // init need_matrix for sessions that need certain lock.
                    if (!"".equals(needList)) {
                        String[] sessionsInNeedList = needList.split(",");
                        for (String sessionInNeedList : sessionsInNeedList) {
                            if (sessionInNeedList.length() >= 3) {
                                needMatrix.put(sessionInNeedList, lockName);
                                sessions.add(sessionInNeedList);
                                if (!allocationMatrix.containsKey(sessionInNeedList)) {
                                    allocationMatrix.put(sessionInNeedList, new HashMap<>());
                                }
                            }
                        }
                    }
                }
            }
            // don't forget your own needed lock
            needMatrix.put(sessionID, tryAcquiredLockName);
            sessions.add(sessionID);
            if (!allocationMatrix.containsKey(sessionID)) {
                allocationMatrix.put(sessionID, new HashMap<>());
            }
            // get sessions that don't need any lock
            for (String session : sessions) {
                if (!needMatrix.containsKey(session)) {
                    needMatrix.put(session, "");
                }
            }
        }

        public boolean unRepeatedRead() {
            return !availableList.containsKey(tryAcquiredLockName);
        }

        /**
         * implementation of Banker's Algorithm
         */
        public boolean tryLockAndDetect() {
            if (logger.isDebugEnabled()) {
                logger.debug("deadlock snapshot: " + sessionID + " need: " + tryAcquiredLockName);
                logger.debug(sessions.toString());
                logger.debug(allocationMatrix.toString());
                logger.debug(needMatrix.toString());
                logger.debug(availableList.toString());
            }

            boolean detected = false;
            // try to remove all sessions from set
            while (!sessions.isEmpty()) {
                boolean found = false;
                for (Iterator<String> iter = sessions.iterator(); iter.hasNext(); ) {
                    String session = iter.next();
                    String needLock = needMatrix.get(session);
                    // needMatrix <= availableList
                    if (needLock.isEmpty() || availableList.get(needLock) == 1) {
                        // availableList = availableList + allocationList_of_this_session
                        Map<String, Integer> allocationList = allocationMatrix.get(session);
                        for (String lockName : availableList.keySet()) {
                            if (allocationList.containsKey(lockName)) {
                                availableList.put(lockName, 1);
                            }
                        }
                        // {session} = {session} - this_session
                        iter.remove();
                        found = true;
                    }
                }
                if (!found) {
                    detected = true;
                    break;
                }
            }
            return detected;
        }

    }

}
