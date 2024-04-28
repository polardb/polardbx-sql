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

package com.alibaba.polardbx.executor.mdl.manager;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlLock;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.executor.mdl.lock.MdlLockStamped;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author chenmo.cm
 */
public class MdlManagerStamped extends MdlManager {

    private static final Logger logger = LoggerFactory.getLogger(MdlManagerStamped.class);

    /**
     * 锁对象列表，schema 下的每个锁对象对应一个 MdlLock 由于被所有前端连接共享，需要异步定时清理
     */
    private final Map<MdlKey, MdlLock> mdlMap = new ConcurrentHashMap<>();

    /**
     * 当前 schema 中全部被持有的锁，按照 连接 和 MdlKey 分组 目前这里假定锁都是可重入的，因此每种锁都只有一个
     * map<frontConnId, Map<key, ticket>>
     */
    private final Map<String, Map<MdlKey, MdlTicket>> tickets = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler;

    private final static int cleanInterval = 60 * 60;

    public MdlManagerStamped(String schema) {
        this(schema, cleanInterval);
    }

    public MdlManagerStamped(String schema, int cleanIntervalInSec) {
        super(schema);

        scheduler = ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("Mdl-Clean-Threads"),
            new ThreadPoolExecutor.CallerRunsPolicy());

        /**
         * 定时清理无用的锁对象, 避免大量建/删表导致内存问题
         */
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                final Set<MdlKey> mdlKeys = new HashSet<>(mdlMap.keySet());

                mdlKeys.forEach(k -> mdlMap.computeIfPresent(k, (key, lock) -> {
                    if (lock.latchWrite()) {
                        try {
                            if (!lock.isLocked()) {
                                // remove unused lock
                                return null;
                            }
                        } finally {
                            lock.unlatchWrite();
                        }
                    }

                    return lock;
                }));

            } catch (Exception e) {
                logger.error(e);
            }
        }, cleanIntervalInSec, cleanIntervalInSec, TimeUnit.SECONDS);
    }

    // For show metadata lock.
    public Map<String, Map<MdlKey, MdlTicket>> getTickets() {
        return tickets;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        if (scheduler != null) {
            try {
                scheduler.shutdown();
                scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Implemented as a reentrant lock, for single front connection, always
     * return same MdlTicket object for same MdlKey
     */
    @Override
    public MdlTicket acquireLock(@NotNull final MdlRequest request, @NotNull final MdlContext context) {
        final boolean readLock = MdlManager.isReadLock(request.getType());

        if (readLock) {
            return readLock(request, context);
        } else {
            return writeLock(request, context);
        }
    }

    @Override
    public List<MdlTicket> getWaitFor(MdlKey mdlKey) {
        List<MdlTicket> contextIdList = new ArrayList<>();
        if (mdlKey == null) {
            return contextIdList;
        }
        tickets.forEach((contextId, keyTicketMap) -> {
            MdlTicket mdlTicket = keyTicketMap.get(mdlKey);
            if (mdlTicket != null) {
                contextIdList.add(mdlTicket);
            }
        });
        return contextIdList;
    }

    @Override
    public void releaseLock(@NotNull final MdlTicket ticket) {
        final boolean readLock = MdlManager.isReadLock(ticket.getType());

        if (readLock) {
            unlockRead(ticket);
        } else {
            unlockWrite(ticket);
        }
    }

    private MdlTicket readLock(@NotNull final MdlRequest request, @NotNull final MdlContext context) {
        // 缩小 map 锁定的范围
        final Map<MdlKey, MdlTicket> keyTickets = tickets.computeIfAbsent(context.getConnId(),
            cid -> new ConcurrentHashMap<>());

        final MdlLock mdlLock = getAndLatchMdlLock(request.getKey());
        try {
            return keyTickets.compute(request.getKey(), (k, t) -> {

                if (null == t) {
                    t = new MdlTicket(request, mdlLock, context, mdlLock.readLock());
                } else if (t.getType() == MdlType.MDL_EXCLUSIVE) {
                    // only for ossLoadData , need to downgrade to readlock
                    t = new MdlTicket(request, mdlLock, context, mdlLock.tryConvertToReadLock(t.getStamp()));
                }

                // 仅支持事务级别的MDL，事务中的所有语句顺序加锁，且 DDL 不可能出现在事务当中
                // 因此 ticket 存在代表当前事务已经从锁对象获取到了读锁，无需再次加锁
                // 另外，tickets 中保存的凭证一定会在解锁时移除（参见 unlockRead 方法），因此无需判断
                // t.isValidate

                request.setTicket(t);

                return t;
            });
        } finally {
            mdlLock.unlatchRead();
        }
    }

    private void unlockRead(@NotNull final MdlTicket ticket) {
        if (!ticket.isValidate()) {
            // 已经被解锁，无需重复操作
            return;
        }

        tickets.computeIfPresent(ticket.getContext().getConnId(), (cid, value) -> {
            value.computeIfPresent(ticket.getLock().getKey(), (k, t) -> {
                t.setValidate(false);

                t.getLock().unlockRead(t.getStamp());

                // 解锁后从释放对象
                return null;
            });

            // 如果表上没有任何正在执行的连接，释放对象
            return value.isEmpty() ? null : value;
        });
    }

    private MdlTicket writeLock(@NotNull MdlRequest request, @NotNull MdlContext context) {
        // 缩小 map 锁定的范围
        final Map<MdlKey, MdlTicket> keyTickets = tickets.computeIfAbsent(context.getConnId(),
            cid -> new ConcurrentHashMap<>());

        final MdlLock mdlLock = getAndLatchMdlLock(request.getKey());
        try {
            return keyTickets.compute(request.getKey(), (k, t) -> {

                if (null == t) {
                    t = new MdlTicket(request, mdlLock, context, mdlLock.writeLock());
                } else if (t.getType() != MdlType.MDL_EXCLUSIVE) {
                    // 这里已经存在的读锁是当前事务id对应的读锁，在这里只会有ossLoadData会走到获取读锁
                    t = new MdlTicket(request, mdlLock, context, mdlLock.tryConvertToWriteLock(t.getStamp()));
                } else {
                    // 由于只有 DDL 语句加写锁，如果 ticket 已经存在，代表当前连接上已经有一个DDL在执行了
                    // MySQL 未支持这种用法，需要退出临界区，否则 unlockWrite 会被阻塞，导致死锁
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Do not support concurrent ddl");
                }

                request.setTicket(t);
                return t;
            });
        } finally {
            mdlLock.unlatchRead();
        }
    }

    private void unlockWrite(@NotNull final MdlTicket ticket) {
        if (!ticket.isValidate()) {
            // 已经被解锁
            return;
        }

        tickets.computeIfPresent(ticket.getContext().getConnId(), (cid, value) -> {
            value.computeIfPresent(ticket.getLock().getKey(), (k, t) -> {
                t.setValidate(false);

                t.getLock().unlockWrite(t.getStamp());

                // 解锁后释放对象
                return null;
            });

            // 如果表上没有任何正在执行的连接，释放对象
            return value.isEmpty() ? null : value;
        });
    }

    private MdlLock getAndLatchMdlLock(@NotNull MdlKey key) {
        return mdlMap.compute(key, (k, l) -> {
            if (null == l) {
                l = new MdlLockStamped(k);
            }

            // 防止锁取出后被清理
            l.latchRead();
            return l;
        });
    }
}
