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

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import javax.validation.constraints.NotNull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * For the purpose of avoiding deadlock
 * Always use this class to manage DDL Engine Resources
 */
public class DdlEngineResourceManager {

    private static final Logger LOGGER = SQLRecorderLogger.ddlEngineLogger;

    private PersistentReadWriteLock lockManager = PersistentReadWriteLock.create();

    private static final String OWNER_PREFIX = "DDL_";
    private static final long RETRY_INTERVAL = 1000L;

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
        Preconditions.checkNotNull(schemaName, "schemaName can't be null");
        Preconditions.checkNotNull(shared, "shared resource can't be null");
        Preconditions.checkNotNull(exclusive, "exclusive resource can't be null");
        Pair<Set<String>, Set<String>> rwLocks = inferRwLocks(shared, exclusive);
        Set<String> readLocks = rwLocks.getKey();
        Set<String> writeLocks = rwLocks.getValue();
        String owner = OWNER_PREFIX + String.valueOf(jobId);

        try {
            while (!lockManager.tryReadWriteLockBatch(schemaName, owner, readLocks, writeLocks)) {
                if (Thread.interrupted()) {
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
                if (CollectionUtils.isNotEmpty(blockers)) {
                    List<DdlEngineRecord> blockerJobRecords = getBlockerJobRecords(schemaName, blockers);
                    if (CollectionUtils.isEmpty(blockerJobRecords)) {
                        //there are pending locks(which are without jobs).
                        //this situation should not happen
                        //but if we come up with this situation
                        //we release all the pending locks
                        String errMsg = String.format(
                            "found pending locks without DDL jobs, lock id:[%s].",
                            Joiner.on(",").join(blockers)
                        );
                        LOGGER.error(errMsg);
                        for (String blocker : blockers) {
                            lockManager.unlockReadWriteByOwner(blocker);
                        }
                        //throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, errMsg);
                    } else {
                        for (DdlEngineRecord record : blockerJobRecords) {
                            DdlState ddlState = DdlState.valueOf(record.state);
                            if (DdlHelper.isTerminated(ddlState)) {
                                //问题：应该直接提示用户如何操作；还是让用户去show前一个失败的任务，在那边提示用户如何操作？
                                throw new TddlRuntimeException(ErrorCode.ERR_PENDING_DDL_JOB_EXISTS,
                                    String.valueOf(record.jobId),
                                    record.ddlType,
                                    record.schemaName,
                                    record.objectName,
                                    record.schemaName);
                            }
                        }
                    }
                }
            }

            //exceed the attempt times, still unable to acquire all the locks
        } catch (TddlRuntimeException e) {
            releaseResource(jobId);
            throw e;
        } catch (Exception e) {
            releaseResource(jobId);
            throw new RuntimeException(e);
        }
    }

    public void releaseResource(long jobId) {
        String owner = OWNER_PREFIX + String.valueOf(jobId);
        FailPoint.injectCrash("fp_ddl_engine_release_write_lock_crash");
        lockManager.unlockReadWriteByOwner(owner);
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
        Set<Long> jobIdSet = toJobIdSet(blockerSet);
        List<DdlEngineRecord> result = new DdlEngineAccessorDelegate<List<DdlEngineRecord>>() {
            @Override
            protected List<DdlEngineRecord> invoke() {
                return engineAccessor.query(Lists.newArrayList(jobIdSet));
            }
        }.execute();
        return result;
    }

    private Set<Long> toJobIdSet(Set<String> ownerSet) {
        if (CollectionUtils.isEmpty(ownerSet)) {
            return new HashSet<>(1);
        }
        Set<Long> result = new HashSet<>(ownerSet.size());
        ownerSet.forEach(e -> {
            try {
                result.add(Long.valueOf(e.substring(OWNER_PREFIX.length())));
            } catch (Exception exception) {
                LOGGER.error("fail to convert owner to job id. owner:" + e, exception);
            }
        });
        return result;
    }

}
