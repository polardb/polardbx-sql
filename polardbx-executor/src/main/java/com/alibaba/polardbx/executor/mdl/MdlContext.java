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

package com.alibaba.polardbx.executor.mdl;

import com.google.common.base.Preconditions;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.IntStream;

/**
 * MDL锁的持有者
 * 通常是一个frontend connectionId。
 * 也可以是一个schemaName。
 *
 * @author chenmo.cm
 */
public abstract class MdlContext {

    public static final String MDL_CONTEXT = "MDL_CONTEXT";

    private static final int LATCH_COUNT = 16;
    private static final StampedLock[] OPTIMIZER_LATCHES = new StampedLock[LATCH_COUNT];

    static {
        IntStream.range(0, LATCH_COUNT).forEach(i -> OPTIMIZER_LATCHES[i] = new StampedLock());
    }

    /**
     * Id of the owner of metadata locks. I.e. each server connection has
     * such an Id.
     */
    final String connId;

    protected MdlContext(String connId) {
        this.connId = connId;
    }

    /**
     * acquires the lock, blocking if necessary until available.
     *
     * @param request MdlRequest
     * @return MdlTicket
     */
    public abstract MdlTicket acquireLock(@NotNull MdlRequest request);

    public abstract List<MdlTicket> getWaitFor(MdlKey mdlKey);

    /**
     * release the lock
     *
     * @param trxId transaction id
     * @param ticket mdl ticket
     * @return MdlTicket
     */
    public abstract void releaseLock(@NotNull Long trxId, @NotNull MdlTicket ticket);

    /**
     * release MDL_TRANSACTION lock by transaction id
     */
    public abstract void releaseTransactionalLocks(@NotNull Long trxId);

    /**
     * release all MDL_TRANSACTION and MDL_STATEMENT locks
     */
    public abstract void releaseAllTransactionalLocks();

    public String getConnId() {
        return connId;
    }

    protected abstract MdlManager getMdlManager(String schema);

    protected void releaseTransactionalLocks(Map<MdlKey, MdlTicket> ticketMap) {
        Preconditions.checkNotNull(ticketMap);

        final Iterator<Entry<MdlKey, MdlTicket>> it = ticketMap.entrySet().iterator();
        while (it.hasNext()) {
            final Entry<MdlKey, MdlTicket> entry = it.next();
            final MdlTicket t = entry.getValue();
            final MdlKey k = entry.getKey();

            if (t.isValidate()) {
                if (!t.getDuration().transactional()) {
                    continue;
                }

                // release validate and transactional lock
                final MdlManager mdlManager = getMdlManager(k.getDbName());
                mdlManager.releaseLock(t);
            }

            it.remove();
        }

    }

    private static StampedLock getLatch(String schema, String table) {
        final int latchIndex = getLatchIndex(schema, table);

        if (null == OPTIMIZER_LATCHES[latchIndex]) {
            synchronized (OPTIMIZER_LATCHES) {
                if (null == OPTIMIZER_LATCHES[latchIndex]) {
                    OPTIMIZER_LATCHES[latchIndex] = new StampedLock();
                }
            }
        }
        return OPTIMIZER_LATCHES[latchIndex];
    }

    private static int getLatchIndex(String schema, String table) {
        final int hash = (schema + table).toLowerCase().hashCode();
        return Math.floorMod(hash, LATCH_COUNT);
    }

    public static long[] snapshotMetaVersions() {
        long[] stamps = new long[LATCH_COUNT];
        IntStream.range(0, LATCH_COUNT).forEach(i -> stamps[i] = OPTIMIZER_LATCHES[i].tryOptimisticRead());
        return stamps;
    }

    public static boolean validateMetaVersion(String schema, String table, long[] stamps) {
        final int latchIndex = getLatchIndex(schema, table);

        return OPTIMIZER_LATCHES[latchIndex].validate(stamps[latchIndex]);
    }

    public static void updateMetaVersion(String schema, String table) {
        final StampedLock latch = getLatch(schema, table);
        latch.unlockWrite(latch.writeLock());
    }

    @Override
    public String toString() {
        return "MdlContext{" +
            "connId='" + connId + '\'' +
            '}';
    }
}
