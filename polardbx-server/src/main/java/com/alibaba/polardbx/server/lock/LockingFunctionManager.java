package com.alibaba.polardbx.server.lock;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.lock.LockingFunctionHandle;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.metadb.lock.LockAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.server.ServerConnection;
import com.google.common.collect.HashMultimap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.lock.LockingConfig.*;

public class LockingFunctionManager {

    private static final Logger logger = LoggerFactory.getLogger(LockingFunctionManager.class);

    private static final LockingFunctionManager INSTANCE = new LockingFunctionManager();

    /**
     * map of (session id - lock handle)
     */
    private final Map<String, PolarDBXLockingFunctionHandle> lockFunctionHandles = new ConcurrentHashMap<>();

    /**
     * multi session may acquire the same lock
     * not thread-safe
     */
    private final HashMultimap<String, Semaphore> waitingLocks = HashMultimap.create();

    private static final ScheduledExecutorService LOCKING_FUNCTION_EXECUTOR =
        new ScheduledThreadPoolExecutor(2, new NamedThreadFactory("locking-function-thread-pool", true));

    private ScheduledFuture notifyFuture;

    private ScheduledFuture heartbeatFuture;

    private LockingFunctionManager() {
    }

    /**
     * singleton
     */
    public static LockingFunctionManager getInstance() {
        return INSTANCE;
    }

    /**
     * get lock handle by connection object and connection id.
     */
    public synchronized LockingFunctionHandle getHandle(ServerConnection connection) {

        String sessionId = sessionId(connection.getId());
        PolarDBXLockingFunctionHandle lockingFunctionHandle = lockFunctionHandles.get(sessionId);
        if (lockingFunctionHandle == null) {
            lockingFunctionHandle = new PolarDBXLockingFunctionHandle(this, connection, sessionId);
            lockFunctionHandles.put(sessionId, lockingFunctionHandle);
        }
        return lockingFunctionHandle;
    }

    public synchronized LockingFunctionHandle getHandle(long connectionId) {

        String sessionId = sessionId(connectionId);
        PolarDBXLockingFunctionHandle lockingFunctionHandle = lockFunctionHandles.get(sessionId);
        if (lockingFunctionHandle == null) {
            FrontendConnection sessionConn = getSessionConnection(connectionId);
            if (sessionConn == null) {
                throw new RuntimeException("the connectionId is invalid");
            }
            lockingFunctionHandle = new PolarDBXLockingFunctionHandle(this, sessionConn, sessionId);
            lockFunctionHandles.put(sessionId, lockingFunctionHandle);
        }
        return lockingFunctionHandle;
    }

    private FrontendConnection getSessionConnection(long connectionId) {
        NIOProcessor[] processors = CobarServer.getInstance().getProcessors();
        for (NIOProcessor processor : processors) {
            FrontendConnection connection = processor.getFrontends().get(connectionId);
            if (connection != null) {
                return connection;
            }
        }
        return null;
    }

    public void releaseAllLocksOnConnClose(long connectionId) throws SQLException {
        String sessionId = sessionId(connectionId);
        PolarDBXLockingFunctionHandle lockingFunctionHandle = lockFunctionHandles.get(sessionId);
        if (lockingFunctionHandle != null) {
            lockingFunctionHandle.releaseAllLocks();
            removeHandle(sessionId);
        }
    }

    /**
     * remove lock handle from manager.
     */
    public void removeHandle(String sessionId) {
        lockFunctionHandles.remove(sessionId);
    }

    private String sessionId(long connectionId) {
        return TddlNode.getNodeId() + ":" + connectionId;
    }

    /**
     * get the connection of lock table.
     */
    Connection getConnection() throws SQLException {
        return MetaDbUtil.getConnection();
    }

    /**
     * PolarDBXLockingFunctionHandle中getLock时会调用此函数
     */
    public void startHeartBeat() {
        if (heartbeatFuture == null) {
            synchronized (this) {
                if (heartbeatFuture == null) {
                    heartbeatFuture = LOCKING_FUNCTION_EXECUTOR.scheduleAtFixedRate(new HeartBeatRunnable(),
                        HEART_BEAT_INTERVAL, HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
                    logger.info("start locking function heartbeat ");
                }
            }
        }
    }

    /**
     * stop heartbeat when no holding locks
     * 不主动停止心跳任务，心跳任务中如果发现holding locks为0，则自行调用stopHeartBeat
     */
    public void stopHeartBeat() {
        if (heartbeatFuture != null) {
            synchronized (this) {
                if (heartbeatFuture != null) {
                    int lockNum = lockFunctionHandles.values().stream().mapToInt(
                        PolarDBXLockingFunctionHandle::getHoldingLockSize).sum();
                    if (lockNum != 0) {
                        return;
                    }
                    heartbeatFuture.cancel(false);
                    heartbeatFuture = null;
                    logger.info("stop locking function heartbeat ");
                }
            }
        }
    }

    /**
     * add waiting task时会调用此函数
     */
    private void startNotifyWaitingLock() {
        if (notifyFuture == null) {
            synchronized (this) {
                if (notifyFuture == null) {
                    notifyFuture = LOCKING_FUNCTION_EXECUTOR.scheduleAtFixedRate(new NotifyWaitingLock(),
                        1000, 1000, TimeUnit.MILLISECONDS);
                    logger.info("start scanning invalid locks ");
                }
            }
        }
    }

    /**
     * stop notify waiting locks where no waiting locks
     * 不主动停止唤醒waiting locks，NotifyWaitingLock任务中如果发现waiting locks为0，则自行调用stopNotifyWaitingLock
     */
    private void stopNotifyWaitingLock() {
        if (notifyFuture != null && waitingLocks.isEmpty()) {
            synchronized (this) {
                if (notifyFuture != null && waitingLocks.isEmpty()) {
                    notifyFuture.cancel(false);
                    notifyFuture = null;
                    logger.info("stop scanning invalid locks ");
                }
            }
        }
    }

    public synchronized void addWaitingLock(String lockName, Semaphore semaphore) {
        startNotifyWaitingLock();
        waitingLocks.put(lockName, semaphore);
    }

    public synchronized void removeWaitingLock(String lockName, Semaphore semaphore) {
        waitingLocks.remove(lockName, semaphore);
    }

    /**
     * notify and remove waiting locks
     */
    private synchronized void notifyWaitingLock(Set<String> lockNames) {
        for (String lockName : lockNames) {
            if (!waitingLocks.containsKey(lockName)) {
                continue;
            }
            //unfair lock
            Collection<Semaphore> semaphores = waitingLocks.get(lockName);
            if (!semaphores.isEmpty()) {
                Iterator<Semaphore> iterator = semaphores.iterator();
                Semaphore semaphore = iterator.next();
                iterator.remove();
                semaphore.release();
            }
        }
    }

    private class NotifyWaitingLock implements Runnable {

        LockAccessor dao;

        long lastExpireScan;

        static final int EXPIRE_SCAN_INTERVAL = EXPIRATION_TIME * 1000 / 2;

        public NotifyWaitingLock() {
            dao = new LockAccessor();
            lastExpireScan = System.currentTimeMillis() - EXPIRE_SCAN_INTERVAL;
        }

        @Override
        public void run() {
            if (waitingLocks.isEmpty()) {
                stopNotifyWaitingLock();
                return;
            }
            //扫描locks表，找到释放的锁
            try {
                Set<String> releaseLocks = dao.getReleaseLocks();
                notifyWaitingLock(releaseLocks);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }

            //扫描locks表，找到过期的锁
            try {
                if (System.currentTimeMillis() - lastExpireScan >= EXPIRE_SCAN_INTERVAL) {
                    Set<String> releaseLocks = dao.getExpireLocks();
                    notifyWaitingLock(releaseLocks);
                    lastExpireScan = System.currentTimeMillis();
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }

        }

    }

    private class HeartBeatRunnable implements Runnable {
        LockAccessor dao;

        public HeartBeatRunnable() {
            dao = new LockAccessor();
        }

        @Override
        public void run() {
            List<String> sessionIds = new ArrayList<>();
            Iterator<Map.Entry<String, PolarDBXLockingFunctionHandle>> iterator =
                lockFunctionHandles.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, PolarDBXLockingFunctionHandle> entry = iterator.next();
                if (entry.getValue().isSessionClosed()) {
                    iterator.remove();
                    continue;
                }
                if (entry.getValue().getHoldingLockSize() > 0) {
                    sessionIds.add(entry.getKey());
                }
            }

            if (sessionIds.isEmpty()) {
                stopHeartBeat();
                return;
            }
            try {
                dao.sessionBatchHeartbeat(sessionIds);
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

}

