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

package com.alibaba.polardbx.executor.ddl.newengine.meta;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * For the purpose of avoiding deadlock
 * Always use this class to manage DDL Engine Resources
 */
public class DdlEngineResourceManager {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private PersistentReadWriteLock lockManager = PersistentReadWriteLock.create();

    private static final long RETRY_INTERVAL = 1000L;

    private static final Map<String, List<DdlContext>> allLocksTryingToAcquire =
        new ConcurrentHashMap<>(DdlConstants.DEFAULT_LOGICAL_DDL_PARALLELISM * 16);

    /**
     * Check whether the resource is available
     *
     * @return true if available
     */
    public boolean checkResource(Set<String> shared,
                                 Set<String> exclusive) {
        Set<String> resources = Sets.union(shared, exclusive);
        Set<String> blockers = lockManager.queryBlocker(resources);
        return blockers.isEmpty();
    }

    public void acquireResource(@NotNull String schemaName,
                                @NotNull long jobId,
                                @NotNull Set<String> shared,
                                @NotNull Set<String> exclusive) {
        acquireResource(schemaName, jobId, __ -> false, shared, exclusive, (Connection conn) -> true);
    }

    public void acquireResource(@NotNull String schemaName,
                                @NotNull long jobId,
                                @NotNull Predicate shouldInterrupt,
                                @NotNull Set<String> shared,
                                @NotNull Set<String> exclusive,
                                @NotNull Function<Connection, Boolean> func) {
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");
        Preconditions.checkNotNull(shared, "shared resource can't be null");
        Preconditions.checkNotNull(exclusive, "exclusive resource can't be null");
        Pair<Set<String>, Set<String>> rwLocks = inferRwLocks(shared, exclusive);
        Set<String> readLocks = rwLocks.getKey();
        Set<String> writeLocks = rwLocks.getValue();
        String owner = PersistentReadWriteLock.OWNER_PREFIX + jobId;

        final LocalDateTime beginTs = LocalDateTime.now();
        int retryCount = 0;

        try {
            while (!lockManager.tryReadWriteLockBatch(schemaName, owner, readLocks, writeLocks, func)) {
                LocalDateTime now = LocalDateTime.now();
                if (now.minusHours(1L).isAfter(beginTs)) {
                    throw new TddlNestableRuntimeException("GET DDL LOCK TIMEOUT");
                }

                if (Thread.interrupted() || shouldInterrupt.test(null)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                }

                try {
                    Thread.sleep(RETRY_INTERVAL);
                } catch (InterruptedException e) {
                    // ignore interrupt
                    Thread.currentThread().interrupt();
                    throw new TddlRuntimeException(ErrorCode.ERR_QUERY_CANCLED);
                }

                //check if there's any failed Job holds the lock
                //if true, don't wait anymore
                Set<String> blockers = lockManager.queryBlocker(Sets.union(shared, exclusive));
                LOGGER.info(String.format(
                    "tryReadWriteLockBatch failed, schemaName:[%s], jobId:[%s], retryCount:[%d], shared:[%s], exclusive:[%s], blockers:[%s]",
                    schemaName, jobId, retryCount++, setToString(shared), setToString(exclusive), setToString(blockers))
                );
                Set<String> ddlBlockers =
                    blockers.stream().filter(e -> StringUtils.startsWith(e, PersistentReadWriteLock.OWNER_PREFIX))
                        .collect(Collectors.toSet());
                if (CollectionUtils.isNotEmpty(ddlBlockers)) {
                    List<DdlEngineRecord> blockerJobRecords = getBlockerJobRecords(schemaName, ddlBlockers);
                    if (CollectionUtils.isEmpty(blockerJobRecords)) {
                        //there are pending locks(which are without jobs).
                        //this situation should not happen
                        //but if we come up with this situation
                        //we release all the pending locks
                        String errMsg = String.format(
                            "found pending locks without DDL jobs, lock id:[%s].",
                            Joiner.on(",").join(ddlBlockers)
                        );
                        LOGGER.error(errMsg);
                        EventLogger.log(EventType.DDL_WARN, errMsg);
                        for (String ddlBlocker : ddlBlockers) {
                            lockManager.unlockReadWriteByOwner(ddlBlocker);
                        }
                    } else {
                        for (DdlEngineRecord record : blockerJobRecords) {
                            DdlState ddlState = DdlState.valueOf(record.state);
                            if (DdlHelper.isTerminated(ddlState)) {
                                StringBuilder sb = new StringBuilder();
                                sb.append(String
                                    .format("Found Paused DDL JOB. You can use 'SHOW DDL %s' to check it for details",
                                        record.jobId));
                                if (record.isSupportContinue() || record.isSupportCancel()) {
                                    sb.append(", ");
                                }
                                if (record.isSupportContinue()) {
                                    sb.append(
                                        String.format("use 'CONTINUE DDL %s' to continue executing it", record.jobId));
                                }
                                if (record.isSupportContinue() && record.isSupportCancel()) {
                                    sb.append(", or ");
                                }
                                if (record.isSupportCancel()) {
                                    sb.append(String.format("use 'CANCEL DDL %s' to cancel it", record.jobId));
                                }
                                sb.append(".");
                                throw new TddlRuntimeException(ErrorCode.ERR_PAUSED_DDL_JOB_EXISTS, sb.toString());
                            }
                        }
                    }
                }
            }
            //exceed the attempt times, still unable to acquire all the locks
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
    }

    public boolean downGradeWriteLock(Connection connection, long jobId, String writeLock) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        return lockManager.downGradeWriteLock(connection, owner, writeLock);
    }

    public int releaseResource(long jobId) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        return lockManager.unlockReadWriteByOwner(owner);
    }

    public int releaseResource(Connection connection, long jobId) {
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        return lockManager.unlockReadWriteByOwner(connection, owner);
    }

    public int releaseResource(Connection connection, long jobId, Set<String> resouceSet) {
        if (CollectionUtils.isEmpty(resouceSet)) {
            return 0;
        }
        String owner = PersistentReadWriteLock.OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        return lockManager.unlockReadWriteByOwner(connection, owner, resouceSet);
    }

    /**
     * infer all the read/write locks need to acquire from resources
     * if a resource exists in both shared and exclusive sets, a write lock is needed
     */
    private Pair<Set<String>, Set<String>> inferRwLocks(Set<String> shared, Set<String> exclusive) {
        Set<String> writeLocks = Sets.newHashSet(exclusive);
        Set<String> readLocks = Sets.newHashSet(shared);
        readLocks = Sets.filter(readLocks, e -> !writeLocks.contains(e));
        return Pair.of(readLocks, writeLocks);
    }

    private List<DdlEngineRecord> getBlockerJobRecords(String schemaName, Set<String> blockerSet) {
        Set<Long> jobIdSet = PersistentReadWriteLock.toJobIdSet(blockerSet);
        List<DdlEngineRecord> result = new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(Lists.newArrayList(jobIdSet));
            }
        }.execute();
        return result;
    }

    private String setToString(Set<String> lockSet) {
        if (CollectionUtils.isEmpty(lockSet)) {
            return "";
        }
        return Joiner.on(",").join(lockSet);
    }

    public static void startAcquiringLock(String schemaName, DdlContext ddlContext) {
        synchronized (allLocksTryingToAcquire) {
            if (!allLocksTryingToAcquire.containsKey(schemaName)) {
                allLocksTryingToAcquire.put(schemaName, new ArrayList<>());
            }
            allLocksTryingToAcquire.get(schemaName).add(ddlContext);
        }
    }

    public static void finishAcquiringLock(String schemaName, DdlContext ddlContext) {
        synchronized (allLocksTryingToAcquire) {
            if (allLocksTryingToAcquire.containsKey(schemaName)) {
                allLocksTryingToAcquire.get(schemaName).remove(ddlContext);
                if (CollectionUtils.isEmpty(allLocksTryingToAcquire.get(schemaName))) {
                    allLocksTryingToAcquire.remove(schemaName);
                }
            }
        }
    }

    public static List<DdlContext> getAllDdlAcquiringLocks(String schemaName) {
        List<DdlContext> result = new ArrayList<>();
        synchronized (allLocksTryingToAcquire) {
            if (CollectionUtils.isEmpty(allLocksTryingToAcquire.get(schemaName))) {
                return result;
            }
            result.addAll(allLocksTryingToAcquire.get(schemaName));
            return result;
        }
    }

}
