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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.mdl.context.MdlContextStamped;
import com.alibaba.polardbx.executor.mdl.context.PreemptiveMdlContextStamped;
import com.alibaba.polardbx.executor.mdl.manager.MdlManagerStamped;
import com.google.common.base.Preconditions;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author chenmo.cm
 */
public abstract class MdlManager extends AbstractLifecycle {

    protected static final Map<String, MdlManager> SCHEMA_MDL_MANAGERS = new ConcurrentHashMap<>();
    protected final String schema;
    /**
     * <pre>
     * Hold all MdlContext, every front connection(ServerConnection) should remove its own context before closed
     * map<front_conn_id, MdlContext>
     * </pre>
     */
    protected static final Map<String, MdlContext> contextMap = new ConcurrentHashMap<>();

    public MdlManager(String schema) {
        this.schema = schema;
    }

    /**
     * For show metadata lock.
     */
    public static Map<String, MdlContext> getContextMap() {
        return contextMap;
    }

    /**
     * For each schema element there will be a MdlManager
     */
    public static MdlManager getInstance(@NotNull String schema) {
        return SCHEMA_MDL_MANAGERS.computeIfAbsent(schema, MdlManagerStamped::new);
    }

    /**
     * Remove MdlManager after schema unloaded
     */
    public static MdlManager removeInstance(@NotNull String schema) {
        final MdlManager removed = SCHEMA_MDL_MANAGERS.remove(schema);
        if (null != removed) {
            removed.destroy();
        }
        return removed;
    }

    /**
     * Context of the owner of metadata locks. I.e. each server connection has
     * such a context.
     */
    public static MdlContext addContext(@NotNull Long connId) {
        return contextMap.computeIfAbsent(connId.toString(), MdlContextStamped::new);
    }

    public static MdlContext addContext(@NotNull String schemaName, boolean preemptive) {
        if(preemptive){
            return contextMap.computeIfAbsent(
                schemaName,
                s -> new PreemptiveMdlContextStamped(schemaName, 15, 15, TimeUnit.SECONDS)
            );
        }
        return contextMap.computeIfAbsent(schemaName, MdlContextStamped::new);
    }

    public static MdlContext addContext(@NotNull String schemaName, long initWait, long interval, TimeUnit timeUnit) {
        Preconditions.checkArgument(initWait > 0);
        Preconditions.checkArgument(interval > 0);
        Preconditions.checkArgument(timeUnit != null);
        return contextMap.computeIfAbsent(
            schemaName,
            s -> new PreemptiveMdlContextStamped(schemaName, initWait, interval, timeUnit)
        );
    }

    /**
     * Remove MdlContext before ServerConnection closed. Make sure
     * MdlContext.releaseAllTransactionalLocks() is called before removal.
     *
     * @param context context to be remove
     * @return context removed, return null if context not exists.
     */
    public static MdlContext removeContext(@NotNull MdlContext context) {
        return contextMap.remove(context.getConnId());
    }

    /**
     * release lock and set ticket to invalidate
     */
    public abstract void releaseLock(@NotNull MdlTicket ticket);

    /**
     * acquires the lock, block until lock available.
     *
     * @param request MdlRequest
     * @param context MdlContext, The Reference of result will also be added to MdlRequest.ticket
     * @return MdlTicket
     */
    public abstract MdlTicket acquireLock(@NotNull MdlRequest request, @NotNull MdlContext context);

    public abstract List<MdlTicket> getWaitFor(MdlKey mdlKey);

    protected static boolean isReadLock(@NotNull MdlType type) {
        boolean readLock;

        switch (type) {
        case MDL_EXCLUSIVE:
            readLock = false;
            break;
        case MDL_SHARED_WRITE:
            readLock = true;
            break;
        default:
            throw new RuntimeException("unsupported MDL type: " + type);
        }

        return readLock;
    }
}
