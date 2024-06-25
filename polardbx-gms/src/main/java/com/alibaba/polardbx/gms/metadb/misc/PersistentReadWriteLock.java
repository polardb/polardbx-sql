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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * a read-write lock based on metaDB
 * 支持读写锁
 * 支持读写锁重入
 * 支持隐式锁升级
 * 不支持隐式锁降级
 * 获取写锁后无需再获取读锁
 * 非公平锁, 偏向读者，使用不当可能会写者形成活锁
 * 支持批量获取读锁、写锁
 * 为了性能以及避免死锁，推荐使用批量获取锁的接口
 * <p>
 * 未来支持死锁打破，目前使用的是死锁避免
 * 未来考虑支持其他的锁优先级，比如公平锁、偏向写者
 * 未来考虑整合进MdlLock
 * 未来考虑提供锁超时接口
 */
public class PersistentReadWriteLock {

    private static final Logger LOGGER = LoggerFactory.getLogger(PersistentReadWriteLock.class);

    private static final String EXCLUSIVE = "EXCLUSIVE";

    private static final long RETRY_INTERVAL = 1000L;
    public static final String OWNER_PREFIX = "DDL_";

    public static final long GET_LOCK_TIMEOUT = 50l; // the same as default innodb_lock_wait_timeout

    private PersistentReadWriteLock() {
    }

    public static PersistentReadWriteLock create() {
        return new PersistentReadWriteLock();
    }

    public void readLock(String schemaName, String owner, String resource) {
        readLockBatch(schemaName, owner, Sets.newHashSet(resource));
    }

    /**
     * try to get the write lock, blocked if unable to
     */
    public void readLockBatch(String schemaName, String owner, Set<String> resourceList) {
        while (true) {
            if (tryReadLockBatch(schemaName, owner, resourceList)) {
                return;
            }
            try {
                Thread.sleep(RETRY_INTERVAL);
            } catch (InterruptedException e) {
                // interrupt it if necessary
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * try to get the read lock, return false immediately if unable to
     *
     * @return if get read lock success
     */
    public boolean tryReadLock(String schemaName, String owner, String resource) {
        return tryReadLockBatch(schemaName, owner, Sets.newHashSet(resource));
    }

    /**
     * 暂不支持锁降级
     */
    public boolean tryReadLockBatch(String schemaName, String owner, Set<String> readLocks) {
        if (StringUtils.isEmpty(owner)) {
            throw new IllegalArgumentException("owner is empty");
        }
        if (CollectionUtils.isEmpty(readLocks)) {
            return true;
        }

        return tryReadWriteLockBatch(schemaName, owner, readLocks, new HashSet<>());
    }

    public int unlockRead(String owner, String resource) {
        return unlockReadBatch(owner, Sets.newHashSet(resource));
    }

    public int unlockReadBatch(String owner, Set<String> resourceList) {
        if (StringUtils.isEmpty(owner)) {
            throw new IllegalArgumentException("owner or resource is empty");
        }
        if (CollectionUtils.isEmpty(resourceList)) {
            return 0;
        }

        int count = new ReadWriteLockAccessDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);

                    int c = 0;
                    for (String resource : resourceList) {
                        c += accessor.deleteByOwnerAndResourceAndType(owner, resource, owner);
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return c;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return 0;
                }
            }
        }.execute();
        return count;
    }

    public int unlockReadByOwner(String owner) {
        int count = new ReadWriteLockAccessDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.query(owner);
                    int c = 0;
                    for (ReadWriteLockRecord r : currentLocks) {
                        c += accessor.deleteByOwnerAndResourceAndType(owner, r.resource, r.owner);
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return c;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return 0;
                }
            }
        }.execute();
        return count;
    }

    /**
     * try to get the write lock, blocked if unable to
     */
    public void writeLock(String schemaName, String owner, String resource) {
        writeLockBatch(schemaName, owner, Sets.newHashSet(resource));
    }

    /**
     * try to get the write lock, blocked if unable to
     */
    public void writeLockBatch(String schemaName, String owner, Set<String> resourceList) {
        while (true) {
            if (tryWriteLockBatch(schemaName, owner, resourceList)) {
                return;
            }
            try {
                Thread.sleep(RETRY_INTERVAL);
            } catch (InterruptedException e) {
                // interrupt it if necessary
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /**
     * try to get the write lock, return false immediately if unable to
     *
     * @return if get write lock success
     */
    public boolean tryWriteLock(String schemaName, String owner, String resource) {
        return tryWriteLockBatch(schemaName, owner, Sets.newHashSet(resource));
    }

    public boolean tryWriteLockBatch(String schemaName, String owner, Set<String> writeLocks) {
        if (StringUtils.isEmpty(owner)) {
            throw new IllegalArgumentException("owner or resource is empty");
        }
        if (CollectionUtils.isEmpty(writeLocks)) {
            return true;
        }

        return tryReadWriteLockBatch(schemaName, owner, new HashSet<>(), writeLocks);
    }

    /**
     * unlock the write lock, since only the owner can
     * unlock it's write lock. so we need jobId and resource here
     *
     * @return unlock count
     */
    public int unlockWrite(String owner, String resource) {
        return unlockWriteBatch(owner, Sets.newHashSet(resource));
    }

    /**
     * unlock the write lock, since only the owner can
     * unlock it's write lock. so we need jobId and resource here
     *
     * @return unlock count
     */
    public int unlockWriteBatch(String owner, Set<String> resourceList) {
        if (StringUtils.isEmpty(owner)) {
            throw new IllegalArgumentException("owner or resource is empty");
        }
        if (CollectionUtils.isEmpty(resourceList)) {
            return 0;
        }

        int count = new ReadWriteLockAccessDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);

                    int c = 0;
                    for (String resource : resourceList) {
                        c += accessor.deleteByOwnerAndResourceAndType(owner, resource, EXCLUSIVE);
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return c;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return 0;
                }
            }
        }.execute();
        return count;
    }

    public int unlockWriteByOwner(String owner) {
        int count = new ReadWriteLockAccessDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.query(owner);
                    int c = 0;
                    for (ReadWriteLockRecord r : currentLocks) {
                        c += accessor.deleteByOwnerAndResourceAndType(owner, r.resource, EXCLUSIVE);
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return c;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return 0;
                }
            }
        }.execute();
        return count;
    }

    /**
     * 释放所有属于这个owner的锁
     */
    public int unlockReadWriteByOwner(String owner) {
        int count = new ReadWriteLockAccessDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.query(owner);
                    int c = 0;
                    for (ReadWriteLockRecord r : currentLocks) {
                        c += accessor.deleteByOwnerAndResourceAndType(owner, r.resource, r.type);
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return c;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return 0;
                }
            }
        }.execute();
        return count;
    }

    /**
     * 释放所有属于这个owner的锁
     */
    public int unlockReadWriteByOwner(Connection connection, String owner) {
        final ReadWriteLockAccessor accessor = new ReadWriteLockAccessor();
        accessor.setConnection(connection);
        List<ReadWriteLockRecord> currentLocks = accessor.query(owner);
        int count = 0;
        for (ReadWriteLockRecord r : currentLocks) {
            count += accessor.deleteByOwnerAndResourceAndType(owner, r.resource, r.type);
        }
        return count;
    }

    public int unlockReadWriteByOwner(Connection connection,
                                      String owner,
                                      Set<String> locks) {
        final ReadWriteLockAccessor accessor = new ReadWriteLockAccessor();
        accessor.setConnection(connection);
        List<ReadWriteLockRecord> currentLocks = accessor.query(owner);
        int count = 0;
        for (ReadWriteLockRecord r : currentLocks) {
            if (locks.contains(r.resource)) {
                count += accessor.deleteByOwnerAndResourceAndType(owner, r.resource, r.type);
            }
        }
        return count;
    }

    /**
     * iff there's no record's type is 'EXCLUSIVE' and there's exactly one record's owner equals to 'owner'
     */
    public boolean hasReadLock(String owner, String resource) {
        return new ReadWriteLockAccessDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.queryReader(resource);
                    boolean hasReadLock = false;
                    for (ReadWriteLockRecord r : currentLocks) {
                        if (isWriteLock(r.type)) {
                            return false;
                        }
                        if (isOwner(r, owner)) {
                            hasReadLock = true;
                        }
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return hasReadLock;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return false;
                }
            }
        }.execute();
    }

    /**
     * iff there's one record and it's type is 'EXCLUSIVE', and it's owner equals to 'owner'
     */
    public boolean hasWriteLock(String owner, String resource) {
        return new ReadWriteLockAccessDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.queryReader(resource);
                    if (CollectionUtils.size(currentLocks) != 1) {
                        return false;
                    }
                    ReadWriteLockRecord r = currentLocks.get(0);
                    if (isWriteLock(r.type) && isOwner(r, owner)) {
                        return true;
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return false;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    return false;
                }
            }
        }.execute();
    }

    /**
     * query the blocker of the resource set
     * 查看锁被谁阻塞了
     */
    public Set<String> queryBlocker(Set<String> resource) {
        return new ReadWriteLockAccessDelegate<Set<String>>() {
            @Override
            protected Set<String> invoke() {
                Set<String> blockerSet = new HashSet<>(resource.size());
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.query(Lists.newArrayList(resource), false);
                    for (ReadWriteLockRecord r : currentLocks) {
                        if (isWriteLock(r.type)) {
                            blockerSet.add(r.owner);
                        }
                    }

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return blockerSet;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    throw new TddlNestableRuntimeException(e);
                }
            }
        }.execute();
    }

    public List<ReadWriteLockRecord> queryByResource(Set<String> resource) {
        return new ReadWriteLockAccessDelegate<List<ReadWriteLockRecord>>() {
            @Override
            protected List<ReadWriteLockRecord> invoke() {
                try {
                    MetaDbUtil.beginTransaction(connection);
                    List<ReadWriteLockRecord> currentLocks = accessor.query(Lists.newArrayList(resource), false);

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return currentLocks;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "release write lock");
                    throw new TddlNestableRuntimeException(e);
                }
            }
        }.execute();
    }

    public boolean tryReadWriteLockBatch(String schemaName,
                                         String owner,
                                         Set<String> readLockSet,
                                         Set<String> writeLockSet) {
        return tryReadWriteLockBatch(schemaName, owner, readLockSet, writeLockSet, (Connection conn) -> true);
    }

    /**
     * 批量获取读锁和写锁
     * readLocks和writeLocks不允许出现交集
     */
    public boolean tryReadWriteLockBatch(String schemaName,
                                         String owner,
                                         Set<String> readLockSet,
                                         Set<String> writeLockSet,
                                         Function<Connection, Boolean> func) {
        if (StringUtils.isEmpty(owner)) {
            throw new IllegalArgumentException("owner is empty");
        }

        final Set<String> writeLocks = Sets.newHashSet(writeLockSet);
        final Set<String> readLocks = Sets.newHashSet(Sets.difference(readLockSet, writeLockSet));
        boolean isSuccess = new ReadWriteLockAccessDelegate<Boolean>() {
            @Override
            protected Boolean invoke() {
                boolean needRelease = false;
                try {
                    MetaDbUtil.beginTransaction(connection);
                    if (CollectionUtils.isEmpty(readLocks) && CollectionUtils.isEmpty(writeLocks)) {
                        func.apply(connection);
                        MetaDbUtil.commit(connection);
                        MetaDbUtil.endTransaction(connection, LOGGER);
                        return true;
                    }
                    needRelease = true;
                    if (!MetaDbUtil.tryGetLock(connection, schemaName, GET_LOCK_TIMEOUT)) {
                        return false;
                    }
                    List<String> allLocks = Lists.newArrayList(Sets.union(readLocks, writeLocks));
                    List<ReadWriteLockRecord> currentLocks = new ArrayList<>(allLocks.size());
                    List<List<String>> allLocksPartition = Lists.partition(allLocks, 100);
                    allLocksPartition.forEach(e -> currentLocks.addAll(accessor.query(e, true)));
                    for (ReadWriteLockRecord record : currentLocks) {
                        //write lock held by other owner
                        if (isWriteLock(record.type) && !isOwner(record, owner)) {
                            return false;
                        }
                        //try to acquire the write lock, but held by other reader
                        if (isReadLock(record.type)
                            && !isOwner(record, owner)
                            && writeLocks.contains(record.resource)) {
                            return false;
                        }
                    }

                    Set<String> acquiredReadLocks = getAcquiredReadLocks(currentLocks, owner);
                    Set<String> acquiredWriteLocks = getAcquiredWriteLocks(currentLocks, owner);

                    Set<String> needToUpgrade =
                        Sets.intersection(writeLocks, acquiredReadLocks);
                    Set<String> needToSkip =
                        Sets.union(acquiredWriteLocks, Sets.intersection(readLocks, acquiredReadLocks));
                    Set<String> needToAcquiredReadLocks =
                        Sets.difference(readLocks, Sets.union(needToSkip, needToUpgrade));
                    Set<String> needToAcquiredWriteLocks =
                        Sets.difference(writeLocks, Sets.union(needToSkip, needToUpgrade));

                    if (CollectionUtils.isEmpty(needToAcquiredReadLocks)
                        && CollectionUtils.isEmpty(needToAcquiredWriteLocks)
                        && CollectionUtils.isEmpty(needToUpgrade)) {

                        func.apply(connection);

                        MetaDbUtil.commit(connection);
                        MetaDbUtil.endTransaction(connection, LOGGER);
                        return true;
                    }

                    for (String r : needToUpgrade) {
                        accessor.deleteByOwnerAndResourceAndType(owner, r, owner);
                    }

                    List<ReadWriteLockRecord> readLockRecords = needToAcquiredReadLocks.stream().map(e -> {
                        ReadWriteLockRecord record = new ReadWriteLockRecord();
                        record.schemaName = schemaName;
                        record.owner = owner;
                        record.resource = e;
                        record.type = owner;
                        return record;
                    }).collect(Collectors.toList());
                    accessor.insert(readLockRecords);

                    List<ReadWriteLockRecord> writeLockRecords =
                        Sets.union(needToUpgrade, needToAcquiredWriteLocks).stream().map(e -> {
                            ReadWriteLockRecord record = new ReadWriteLockRecord();
                            record.schemaName = schemaName;
                            record.owner = owner;
                            record.resource = e;
                            record.type = EXCLUSIVE;
                            return record;
                        }).collect(Collectors.toList());
                    accessor.insert(writeLockRecords);

                    func.apply(connection);

                    MetaDbUtil.commit(connection);
                    MetaDbUtil.endTransaction(connection, LOGGER);
                    return true;
                } catch (Exception e) {
                    //rollback all, if any resource is unable to acquire
                    MetaDbUtil.rollback(connection, e, LOGGER, "acquire write lock");
                    return false;
                } finally {
                    if (needRelease) {
                        MetaDbUtil.releaseLock(connection, schemaName);
                    }
                }
            }
        }.execute().booleanValue();
        return isSuccess;
    }

    public boolean downGradeWriteLock(Connection connection,
                                      String owner,
                                      String writeLock) {
        Preconditions.checkArgument(!StringUtils.isEmpty(owner), "owner is empty");
        Preconditions.checkArgument(!StringUtils.isEmpty(writeLock), "writeLock is empty");

        final ReadWriteLockAccessor accessor = new ReadWriteLockAccessor();
        accessor.setConnection(connection);
        Optional<ReadWriteLockRecord> writeLockOptionalRecord = accessor.queryInShareMode(writeLock, EXCLUSIVE);
        if (!writeLockOptionalRecord.isPresent()) {
            return false;
        }

        if (!StringUtils.equals(writeLockOptionalRecord.get().owner, owner)) {
            return false;
        }

        int count = accessor.deleteByOwnerAndResourceAndType(owner, writeLock, EXCLUSIVE);
        if (count > 0) {
            ReadWriteLockRecord readLockRecord = new ReadWriteLockRecord();
            readLockRecord.schemaName = writeLockOptionalRecord.get().schemaName;
            readLockRecord.owner = owner;
            readLockRecord.resource = writeLock;
            readLockRecord.type = owner;
            accessor.insert(Lists.newArrayList(readLockRecord));
            return true;
        }
        return false;
    }

    public String toRecommend(List<ReadWriteLockRecord> records, Set<String> resources) {
        if (CollectionUtils.isEmpty(records)) {
            return CollectionUtils.isEmpty(resources) ? "empty resources" : null;
        }

        StringBuilder sb = new StringBuilder();
        records.stream()
            .filter(e -> org.apache.commons.lang3.StringUtils.startsWith(e.owner, PersistentReadWriteLock.OWNER_PREFIX))
            .forEach(e -> {
                try {
                    long id = Long.parseLong(e.owner.substring(PersistentReadWriteLock.OWNER_PREFIX.length()));
                    if (!StringUtils.isEmpty(e.schemaName)) {
                        sb.append(String.format("Please check ddl state in schema:[%s] using 'show ddl %d'"
                            , e.schemaName, id));
                    }
                } catch (Exception exception) {
                    LOGGER.error("fail to convert owner to job id. owner:" + e, exception);
                }
            });
        return sb.toString();
    }

    public static Set<Long> toJobIdSet(Set<String> ownerSet) {
        if (CollectionUtils.isEmpty(ownerSet)) {
            return new HashSet<>(1);
        }
        Set<Long> result = new HashSet<>(ownerSet.size());
        ownerSet.stream()
            .filter(e -> org.apache.commons.lang3.StringUtils.startsWith(e, PersistentReadWriteLock.OWNER_PREFIX))
            .forEach(e -> {
                try {
                    result.add(Long.valueOf(e.substring(PersistentReadWriteLock.OWNER_PREFIX.length())));
                } catch (Exception exception) {
                    LOGGER.error("fail to convert owner to job id. owner:" + e, exception);
                }
            });
        return result;
    }

    /***************************************** privete function ***********************************************/

    private Set<String> getAcquiredWriteLocks(List<ReadWriteLockRecord> currentLocks, String owner) {
        Set<String> acquiredWriteLocks = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(currentLocks)) {
            currentLocks.forEach(e -> {
                boolean isOwner = isOwner(e, owner);
                boolean isWriteLock = isWriteLock(e.type);

                if (isOwner && isWriteLock) {
                    acquiredWriteLocks.add(e.resource);
                }
            });
        }
        return acquiredWriteLocks;
    }

    private Set<String> getAcquiredReadLocks(List<ReadWriteLockRecord> currentLocks, String owner) {
        Set<String> acquiredReadLocks = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(currentLocks)) {
            currentLocks.forEach(e -> {
                boolean isOwner = isOwner(e, owner);
                boolean isWriteLock = isWriteLock(e.type);

                if (isOwner && !isWriteLock) {
                    acquiredReadLocks.add(e.resource);
                }
            });
        }
        return acquiredReadLocks;
    }

    private boolean isOwner(ReadWriteLockRecord record, String owner) {
        return record != null && StringUtils.equals(record.owner, owner);
    }

    private boolean isWriteLock(String str) {
        return StringUtils.equals(str, EXCLUSIVE);
    }

    private boolean isReadLock(String str) {
        return !StringUtils.equals(str, EXCLUSIVE);
    }
}
