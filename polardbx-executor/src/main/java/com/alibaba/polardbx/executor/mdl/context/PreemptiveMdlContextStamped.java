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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 获取MDL的X锁时，如果超时，会通过kill connection的方式尝试抢占锁
 *
 * @author guxu
 */
public class PreemptiveMdlContextStamped extends MdlContextStamped {
    private static final Logger logger = LoggerFactory.getLogger(PreemptiveMdlContextStamped.class);

    private final String schemaName;
    private final PreemptiveTime preemptiveTime;

    public PreemptiveMdlContextStamped(String schemaName, PreemptiveTime preemptiveTime) {
        super(schemaName);
        this.schemaName = schemaName;
        this.preemptiveTime = preemptiveTime;
    }

    public PreemptiveMdlContextStamped(String schemaName, Long connId, PreemptiveTime preemptiveTime) {
        super(connId.toString());
        this.schemaName = schemaName;
        this.preemptiveTime = preemptiveTime;
    }

    @Override
    public MdlTicket acquireLock(@NotNull final MdlRequest request) {
        ScheduledExecutorService scheduler = null;
        try {
            ParamManager paramManager = OptimizerContext.getContext(schemaName).getParamManager();
            boolean enablePreemptiveMdl = paramManager.getBoolean(ConnectionParams.ENABLE_PREEMPTIVE_MDL);
            if (enablePreemptiveMdl && request.getType() == MdlType.MDL_EXCLUSIVE) {
                    PreemptiveTime.checkIntervalAndTimeUnit(preemptiveTime);
                scheduler = ExecutorUtil.createScheduler(1,
                    new NamedThreadFactory("Mdl-Preempt-Threads"),
                    new ThreadPoolExecutor.DiscardPolicy());
                scheduler.scheduleWithFixedDelay(() -> preemptMdlLock(request), preemptiveTime.getInit(), preemptiveTime.getInterval(), preemptiveTime.getTimeUnit());
            }
            return super.acquireLock(request);
        } finally {
            if (scheduler != null) {
                scheduler.shutdown();
            }
        }
    }

    private void preemptMdlLock(@NotNull final MdlRequest request) {
        try {
            logger.warn(String.format("start do preempt mdl by kill connections"));
            List<MdlTicket> blockerList = getWaitFor(request.getKey());
            for (MdlTicket blocker : blockerList) {
                if (blocker.getType() == MdlType.MDL_EXCLUSIVE) {
                    logger.warn(String.format("has another ddl hold the mdl. connIdStr:[%s]",
                        blocker.getContext().getConnId()));
                    continue;
                }
                if (!blocker.isValidate()) {
                    continue;
                }
                String connIdStr = blocker.getContext().getConnId();
                //check connId is long type
                Long connId = Longs.tryParse(connIdStr);
                //kill it
                if (connId == null) {
                    //this is not expected
                    logger.warn(
                        String.format("try parse frontend connId to Long but failed. connIdStr:[%s]", connIdStr));
                    continue;
                }
                logger.warn(String.format("Preempt mdl by kill connection: %s ", connIdStr));
                killByFrontendConnId(connId);
            }
        } catch (Throwable t) {
            logger.error("preemptMdlLock error", t);
        }
    }

    private static Class killSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    private void killByFrontendConnId(long frontendConnId) {
        ISyncAction killSyncAction;
        try {
            killSyncAction =
                (ISyncAction) killSyncActionClass.getConstructor(
                    String.class,
                    Long.TYPE,
                    Boolean.TYPE,
                    Boolean.TYPE,
                    ErrorCode.class
                ).newInstance(schemaName, frontendConnId, false, true, ErrorCode.ERR_TRANS_PREEMPTED_BY_DDL);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        SyncManagerHelper.sync(killSyncAction, schemaName, SyncScope.ALL);
    }
}
