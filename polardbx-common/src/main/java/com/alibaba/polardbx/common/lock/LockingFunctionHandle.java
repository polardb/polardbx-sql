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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class LockingFunctionHandle {
    private static final Logger logger = LoggerFactory.getLogger(LockingFunctionHandle.class);

    private static final String SQL_GET_DEADLOCK_SNAPSHOT =
        "select lock_name, drds_session, need_list from " + SystemTables.DRDS_SYSTABLE_LOCKING_FUNCTION + " for update";


    private static final ScheduledExecutorService HEARTBEAT_EXECUTOR =
        new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("heartbeat-thread-pool", true));

    private static final Map<String, ScheduledFuture> HEARTBEAT_FUTURES = new ConcurrentHashMap<>();
    private LockingFunctionManager manager;

    private ITConnection tddlConnection;
    private String drdsSession;
    private LockingFunctionSystableDao dao;

    private Map<String, Integer> reentries;

    LockingFunctionHandle(LockingFunctionManager manager, ITConnection tddlConnection, String drdsSession) {
        this.manager = manager;
        this.tddlConnection = tddlConnection;
        this.reentries = new HashMap<>();
        this.drdsSession = drdsSession;
        this.dao = new LockingFunctionSystableDao(manager, drdsSession);
    }

    public Integer tryAcquireLock(final String lockName, final int timeout) throws SQLException, InterruptedException {

        Integer reentryTimes = 0;
        if ((reentryTimes = reentries.get(lockName)) != null) {

            if (dao.isMyLockValid(lockName)) {

                reentries.put(lockName, reentryTimes + 1);
                return 1;
            } else {

                reentries.remove(lockName);
                GeneralUtil.nestedException(
                    "the lock: " + lockName + " has been abnormally lost. please try again to get this lock.");
            }
        }

        boolean successToCreateLock = false;
        try {
            successToCreateLock = dao.tryCreateLock(lockName);
        } catch (SQLException e) {
            logger.info("innodb deadlock occur, retry", e);

            return tryAcquireLock(lockName, timeout);
        }
        if (successToCreateLock) {

            postHandleAfterGetLock(lockName);
            return 1;
        }

        boolean deadlock = false;
        boolean unRepeatedRead = false;
        try (Connection connectionInTransaction = manager.getConnection()) {
            try {
                connectionInTransaction.setAutoCommit(false);

                DeadlockAvoidanceHelper helper = new DeadlockAvoidanceHelper(connectionInTransaction, lockName);
                if (!(unRepeatedRead = helper.unRepeatedRead()) && !(deadlock = helper.tryLockAndDetect())) {

                    dao.registerNeedList(connectionInTransaction, lockName);
                }

                connectionInTransaction.commit();
            } catch (SQLException e) {
                connectionInTransaction.rollback();
                throw e;
            } finally {
                connectionInTransaction.setAutoCommit(true);
            }
        }

        if (unRepeatedRead) {
            logger.info("retry the acquisition!");
            return tryAcquireLock(lockName, timeout);
        }
        if (deadlock) {
            throw new TddlRuntimeException(ErrorCode.ERR_USER_LOCK_DEADLOCK);
        }

        return tryAcquireWithinTimeout(lockName, timeout);
    }

    public Integer release(String lockName) throws SQLException {
        Integer releaseResult = null;
        Integer reentryTimes;
        if ((reentryTimes = reentries.get(lockName)) != null) {

            try (Connection connectionInTransaction = manager.getConnection()) {
                try {
                    connectionInTransaction.setAutoCommit(false);

                    if (dao.isMyLockValid(connectionInTransaction, lockName)) {

                        reentryTimes--;
                        if (reentryTimes >= 0) {
                            reentries.put(lockName, reentryTimes);
                            releaseResult = 1;
                        } else {

                            releaseResult =
                                (dao.grantLock(connectionInTransaction, lockName) || dao.deleteLockTuple(
                                    connectionInTransaction, lockName)) ? 1 : 0;
                            reentries.remove(lockName);
                            endHeartbeatTask(lockName);
                        }
                    } else {

                        reentries.remove(lockName);
                        releaseResult = 0;
                        endHeartbeatTask(lockName);
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

            boolean exists = dao.checkLockExistence(lockName);
            releaseResult = exists ? 0 : null;
        }
        return releaseResult;
    }

    public Integer releaseAllLocks() throws SQLException {
        Integer count = 0;
        try (Connection connection = manager.getConnection()) {
            count = releaseAllLocks(connection);
        }
        manager.removeHandle(drdsSession);
        return count;
    }

    public Integer isFreeLock(String lockName) throws SQLException {
        return dao.isFreeLock(lockName);
    }

    public String isUsedLock(String lockName) throws SQLException {
        return dao.isUsedLock(lockName);
    }

    private boolean exceedMaxActiveLockCounts() {
        return HEARTBEAT_FUTURES.size() + 1 > LockingFunctionManager.MAX_LOCKS_NUMBER;
    }

    private Integer releaseAllLocks(Connection connectionInTransaction) throws SQLException {
        int count = 0;
        for (Iterator iter = reentries.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, Integer> entry = (Map.Entry<String, Integer>) iter.next();
            String lockName = entry.getKey();
            Integer reentryTimes = entry.getValue();

            endHeartbeatTask(lockName);

            try {
                connectionInTransaction.setAutoCommit(false);

                if (dao.isMyLockValid(connectionInTransaction, lockName) && !dao
                    .grantLock(connectionInTransaction, lockName)) {
                    dao.deleteLockTuple(connectionInTransaction, lockName);
                }
                connectionInTransaction.commit();
            } catch (SQLException e) {
                connectionInTransaction.rollback();
                throw e;
            } finally {
                connectionInTransaction.setAutoCommit(true);
            }

            iter.remove();
            count += (reentryTimes + 1);
        }
        return count;
    }

    private void postHandleAfterGetLock(String lockName) {

        reentries.put(lockName, 0);

        if (exceedMaxActiveLockCounts()) {
            try {
                release(lockName);
            } catch (SQLException e) {

            }
            GeneralUtil.nestedException(
                "the lock number exceeds the maximum lock number " + LockingFunctionManager.MAX_LOCKS_NUMBER);
        }
        startHeartbeatTask(lockName);
    }

    private Integer tryAcquireWithinTimeout(final String lockName, final int timeout)
        throws SQLException, InterruptedException {

        long restTime = timeout < 0 ? Integer.MAX_VALUE : timeout * 1000;
        boolean successToGetLock = false;
        while (!successToGetLock) {
            long startTime = System.currentTimeMillis();
            if (Thread.interrupted()) {
                throw GeneralUtil.nestedException("interrupted");
            }
            if (dao.checkLockHolder(lockName)) {
                logger.debug(lockName + " lock belongs to " + drdsSession);

                successToGetLock = true;
            }
            try (Connection connectionInTransaction = manager.getConnection()) {
                try {
                    connectionInTransaction.setAutoCommit(false);
                    if (!dao.checkLockValidityForUpdate(connectionInTransaction, lockName) && dao.trySeizeLock(
                        connectionInTransaction, lockName)) {

                        dao.unregisterNeedList(connectionInTransaction, lockName);
                        successToGetLock = true;
                    } else {

                        if (restTime <= 0) {

                            dao.unregisterNeedList(connectionInTransaction, lockName);
                            break;
                        } else {

                            Thread.sleep(100);
                        }
                    }

                    connectionInTransaction.commit();
                } catch (SQLException e) {
                    connectionInTransaction.rollback();
                    throw e;
                } finally {
                    connectionInTransaction.setAutoCommit(true);
                }
            }

            restTime -= (System.currentTimeMillis() - startTime);
        }
        if (successToGetLock) {

            postHandleAfterGetLock(lockName);
        }
        return successToGetLock ? 1 : 0;
    }

    private void startHeartbeatTask(String lockName) {
        logger.debug("start heartbeat: " + drdsSession);
        HeartBeatRunnable heartBeatRunnable = new HeartBeatRunnable(lockName);
        ScheduledFuture future =
            HEARTBEAT_EXECUTOR.scheduleAtFixedRate(heartBeatRunnable, 1000, LockingFunctionManager.HEART_BEAT_INTERVAL,
                TimeUnit.MILLISECONDS);
        HEARTBEAT_FUTURES.put(getHeartbeatTag(lockName), future);
    }

    private void endHeartbeatTask(String lockName) {
        logger.debug("end heartbeat: " + drdsSession);
        String heartbeatTag = getHeartbeatTag(lockName);
        ScheduledFuture future = HEARTBEAT_FUTURES.get(heartbeatTag);
        if (future != null) {
            future.cancel(false);
            HEARTBEAT_FUTURES.remove(heartbeatTag);
        }
    }

    private String getHeartbeatTag(String lockName) {
        return drdsSession + "_" + lockName;
    }

    private class DeadlockAvoidanceHelper {
        private String tryAcquiredLockName;

        private Map<String, Map<String, Integer>> allocationMatrix;

        private Map<String, String> needMatrix;

        private Map<String, Integer> availableList;

        private Set<String> sessions;

        private Connection connectionInTransaction;

        DeadlockAvoidanceHelper(Connection connectionInTransaction, String tryAcquiredLockName) throws SQLException {
            this.connectionInTransaction = connectionInTransaction;
            this.tryAcquiredLockName = tryAcquiredLockName;

            this.availableList = new HashMap<>();
            this.needMatrix = new HashMap<>();
            this.allocationMatrix = new HashMap<>();
            this.sessions = new HashSet<>();

            try (PreparedStatement ps = connectionInTransaction.prepareStatement(SQL_GET_DEADLOCK_SNAPSHOT)) {
                ResultSet resultSet = ps.executeQuery();
                logger.debug("lock_name | drds_session | need_list");
                while (resultSet.next()) {
                    String lockName = resultSet.getString(LockingFunctionSystableDao.COLUMN_LOCK_NAME);
                    String session = resultSet.getString(LockingFunctionSystableDao.COLUMN_DRDS_SESSION);
                    String needList = resultSet.getString(LockingFunctionSystableDao.COLUMN_NEED_LIST);
                    logger.debug(lockName + " | " + session + " | " + needList);
                    sessions.add(session);

                    availableList.put(lockName, 0);

                    Map<String, Integer> allocationList = null;
                    if ((allocationList = allocationMatrix.get(session)) != null) {
                        allocationList.put(lockName, 1);
                    } else {
                        allocationList = new HashMap<>();
                        allocationList.put(lockName, 1);
                        allocationMatrix.put(session, allocationList);
                    }

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

            needMatrix.put(drdsSession, tryAcquiredLockName);
            sessions.add(drdsSession);
            if (!allocationMatrix.containsKey(drdsSession)) {
                allocationMatrix.put(drdsSession, new HashMap<>());
            }

            for (String session : sessions) {
                if (!needMatrix.containsKey(session)) {
                    needMatrix.put(session, "");
                }
            }
        }

        public boolean unRepeatedRead() {
            return !availableList.containsKey(tryAcquiredLockName);
        }

        public boolean tryLockAndDetect() {
            logger.debug("deadlock snapshot: " + drdsSession + " need: " + tryAcquiredLockName);
            logger.debug(sessions.toString());
            logger.debug(allocationMatrix.toString());
            logger.debug(needMatrix.toString());
            logger.debug(availableList.toString());
            boolean detected = false;

            while (!sessions.isEmpty()) {
                boolean found = false;
                for (Iterator<String> iter = sessions.iterator(); iter.hasNext(); ) {
                    String session = iter.next();
                    String needLock = needMatrix.get(session);

                    if (needLock.isEmpty() || availableList.get(needLock) == 1) {

                        Map<String, Integer> allocationList = allocationMatrix.get(session);
                        for (String lockName : availableList.keySet()) {
                            if (allocationList.containsKey(lockName)) {
                                availableList.put(lockName, 1);
                            }
                        }

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

    private class HeartBeatRunnable implements Runnable {
        private String lockName;

        HeartBeatRunnable(String lockName) {
            this.lockName = lockName;
        }

        private boolean heartbeat(String lockName) throws SQLException {

            if (tddlConnection == null || tddlConnection.isClosed()) {
                return false;
            }
            int updates = 0;
            try (Connection connectionInTransaction = manager.getConnection()) {
                try {
                    connectionInTransaction.setAutoCommit(false);

                    if (dao.isMyLockValid(connectionInTransaction, lockName)) {
                        updates = dao.updateModifiedTime(connectionInTransaction, lockName);
                        logger.debug("heartbeat: " + drdsSession + ", " + lockName);
                    }
                    connectionInTransaction.commit();
                } catch (SQLException e) {
                    connectionInTransaction.rollback();
                    throw e;
                } finally {
                    connectionInTransaction.setAutoCommit(true);
                }
            }
            return updates == 1;
        }

        @Override
        public void run() {
            try {
                if (!heartbeat(lockName)) {
                    endHeartbeatTask(lockName);
                }
            } catch (SQLException e) {
                logger.error("heartbeat task for lock " + lockName + " failed", e);
            }
        }
    }

}
