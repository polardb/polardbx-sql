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

package com.alibaba.polardbx.executor.mdl.context;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;

/**
 * 锁的所有者的上下文，每个前端连接都有一个对应的上下文
 *
 * @author chenmo.cm
 */
public class MdlContextStamped extends MdlContext {
    private static final Logger logger = LoggerFactory.getLogger(MdlContextStamped.class);

    public class TransactionInfo {
        private final long trxId;
        private final String traceId;
        private final ByteString sql;
        private final String frontend;

        public TransactionInfo(long trxId) {
            this.trxId = trxId;
            this.traceId = null;
            this.sql = null;
            this.frontend = null;
        }

        public TransactionInfo(long trxId, String traceId, ByteString sql, String frontend) {
            this.trxId = trxId;
            this.traceId = traceId;
            this.sql = sql;
            this.frontend = frontend;
        }

        public long getTrxId() {
            return trxId;
        }

        public String getTraceId() {
            return traceId;
        }

        public ByteString getSql() {
            return sql;
        }

        public String getFrontend() {
            return frontend;
        }

        @Override
        public int hashCode() {
            return (int) trxId;
        }

        @Override
        public boolean equals(Object obj) {
            return ((TransactionInfo) obj).trxId == trxId;
        }
    }

    /**
     * 当前上下文中已经获取到的锁, 按照事务 ID 分组
     */
    private final Map<TransactionInfo, Map<MdlKey, MdlTicket>> tickets;
    /**
     * lock for protection of local field tickets
     */
    private final StampedLock lock = new StampedLock();

    public MdlContextStamped(String connId) {
        super(connId);
        this.tickets = new ConcurrentHashMap<>();
    }

    // For show metadata lock.
    public Map<TransactionInfo, Map<MdlKey, MdlTicket>> getTickets() {
        return tickets;
    }

    @Override
    public MdlTicket acquireLock(@NotNull final MdlRequest request) {
        final MdlManager mdlManager = getMdlManager(request.getKey().getDbName());

        final long l = lock.readLock();
        try {
            final Map<MdlKey, MdlTicket> ticketMap = tickets.computeIfAbsent(
                new TransactionInfo(request.getTrxId(), request.getTraceId(), request.getSql(), request.getFrontend()),
                tid -> new ConcurrentHashMap<>());

            return ticketMap.compute(request.getKey(), (key, ticket) -> {
                if (null == ticket || !ticket.isValidate()) {
                    ticket = mdlManager.acquireLock(request, this);
                }
                return ticket;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    @Override
    public List<MdlTicket> getWaitFor(MdlKey mdlKey) {
        final MdlManager mdlManager = getMdlManager(mdlKey.getDbName());
        List<MdlTicket> waitForList = mdlManager.getWaitFor(mdlKey);
        waitForList.removeIf(e->e!=null && e.getContext()==this);
        return waitForList;
    }

    @Override
    public void releaseLock(@NotNull Long trxId, @NotNull final MdlTicket ticket) {
        final MdlManager mdlManager = getMdlManager(ticket.getLock().getKey().getDbName());

        final long l = lock.readLock();
        try {
            tickets.computeIfPresent(new TransactionInfo(trxId), (tid, ticketMap) -> {
                ticketMap.computeIfPresent(ticket.getLock().getKey(), (k, t) -> {
                    if (t.isValidate()) {
                        mdlManager.releaseLock(ticket);
                    }
                    return null;
                });

                return ticketMap.isEmpty() ? null : ticketMap;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    @Override
    public void releaseTransactionalLocks(Long trxId) {
        final long l = lock.readLock();
        try {
            tickets.computeIfPresent(new TransactionInfo(trxId), (tid, ticketMap) -> {
                releaseTransactionalLocks(ticketMap);

                return ticketMap.isEmpty() ? null : ticketMap;
            });
        } finally {
            lock.unlockRead(l);
        }
    }

    @Override
    public void releaseAllTransactionalLocks() {

        // make sure no more mdl acquired during releasing all of current mdl
        final long l = lock.writeLock();
        try {
            final Iterator<Entry<TransactionInfo, Map<MdlKey, MdlTicket>>> it = tickets.entrySet().iterator();
            while (it.hasNext()) {
                final Map<MdlKey, MdlTicket> v = it.next().getValue();

                releaseTransactionalLocks(v);

                if (v.isEmpty()) {
                    it.remove();
                }
            }
        } finally {
            lock.unlockWrite(l);
        }
    }

    @Override
    protected MdlManager getMdlManager(String schema) {
        return MdlManager.getInstance(schema);
    }

    @Override
    public String toString() {
        return "MdlContextStamped{" +
            "tickets: " + tickets.size() +
            ", lock=" + lock +
            '}';
    }
}
