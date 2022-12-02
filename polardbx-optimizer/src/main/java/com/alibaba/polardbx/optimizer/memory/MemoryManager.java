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

package com.alibaba.polardbx.optimizer.memory;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MppConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.io.FileUtils;

import java.util.Map;

/**
 * memory manager for single host of server or worker(mpp)
 *
 * @author chenghui.lch
 */
public class MemoryManager extends AbstractLifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(MemoryManager.class);

    protected static MemoryManager instance = new MemoryManager();

    protected GlobalMemoryPool globalMemoryPool;

    protected TpMemoryPool tpMemoryPool;

    protected ApMemoryPool apMemoryPool;

    protected MemoryPool cacheMemoryPool;

    private long procedureCacheLimit = MemorySetting.UNLIMITED_SIZE;

    private long functionCacheLimit = MemorySetting.UNLIMITED_SIZE;

    public static MemoryManager getInstance() {
        return instance;
    }

    protected MemoryManager() {
        init();
    }

    @Override
    protected void doInit() {
        globalMemoryPool =
            new GlobalMemoryPool(MemoryType.GLOBAL.getExtensionName(), MemorySetting.UNLIMITED_SIZE);
        cacheMemoryPool = globalMemoryPool
            .getOrCreatePool(MemoryType.CACHE.getExtensionName(), MemorySetting.UNLIMITED_SIZE, MemoryType.CACHE);
        tpMemoryPool = (TpMemoryPool) globalMemoryPool
            .getOrCreatePool(MemoryType.GENERAL_TP.getExtensionName(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.GENERAL_TP);
        apMemoryPool = (ApMemoryPool) globalMemoryPool
            .getOrCreatePool(MemoryType.GENERAL_AP.getExtensionName(), MemorySetting.UNLIMITED_SIZE,
                MemoryType.GENERAL_AP);
    }

    public void adjustMemoryLimit(long globalLimit) {
        //globalMemoryLimitRatio
        long newGlobalLimit = Math.round(globalLimit * MppConfig.getInstance().getGlobalMemoryLimitRatio());
        globalMemoryPool.setMaxLimit(newGlobalLimit);
        tpMemoryPool.setMaxLimit(newGlobalLimit);
        apMemoryPool.setMaxLimit(newGlobalLimit);
        cacheMemoryPool.setMaxLimit(newGlobalLimit);

        tpMemoryPool.setMinLimit(Math.round(newGlobalLimit * MemorySetting.TP_LOW_MEMORY_PROPORTION));
        tpMemoryPool.setMaxLimit(Math.round(newGlobalLimit * MemorySetting.TP_HIGH_MEMORY_PROPORTION));
        apMemoryPool.setMinLimit(Math.round(newGlobalLimit * MemorySetting.AP_LOW_MEMORY_PROPORTION));
        apMemoryPool.setMaxLimit(Math.round(newGlobalLimit * MemorySetting.AP_HIGH_MEMORY_PROPORTION));
        procedureCacheLimit = Math.round(newGlobalLimit * MemorySetting.PROCEDURE_CACHE_LIMIT);
        functionCacheLimit = Math.round(newGlobalLimit * MemorySetting.FUNCTION_CACHE_LIMIT);
        logger.info("The Global Memory Pool size is  " + FileUtils.byteCountToDisplaySize(newGlobalLimit));
    }

    @Deprecated
    public MemoryPool createQueryMemoryPool(boolean ap, String traceId, Map<String, Object> properties) {
        long queryMemoryLimit = GeneralUtil.getPropertyLong(
            properties, ConnectionProperties.PER_QUERY_MEMORY_LIMIT,
            MemorySetting.USE_DEFAULT_MEMORY_LIMIT_VALUE);
        return createQueryMemoryPool(ap, traceId, queryMemoryLimit);
    }

    private long checkMemoryLimit(long queryMemoryLimit) {
        if (queryMemoryLimit == MemorySetting.USE_DEFAULT_MEMORY_LIMIT_VALUE) {
            // use the default limit value set by drds
            // If not set by user, calculate a default value (1/4 of the general
            // pool size)
            long globalLimit = this.globalMemoryPool.getMaxLimit();
            // By default allow a single query to use up to 1/4 memory
            queryMemoryLimit = (long) (globalLimit * MemorySetting.DEFAULT_ONE_QUERY_MAX_MEMORY_PROPORTION);
        }
        return queryMemoryLimit;
    }

    public MemoryPool createQueryMemoryPool(boolean ap, String traceId, long queryMemoryLimit) {
        queryMemoryLimit = checkMemoryLimit(queryMemoryLimit);

        if (ap) {
            return apMemoryPool.getOrCreatePool(traceId, queryMemoryLimit, MemoryType.QUERY);
        } else {
            return tpMemoryPool.getOrCreatePool(traceId, queryMemoryLimit, MemoryType.QUERY);
        }
    }

    /**
     * stored procedure or function always use ap memory pool, because we can't estimate it's cost
     */
    public MemoryPool createStoredProcedureMemoryPool(String procedureId, long queryMemoryLimit,
                                                      MemoryPool parentPool) {
        if (parentPool == null) {
            return apMemoryPool.getOrCreatePool(procedureId, queryMemoryLimit, MemoryType.STORED_PROCEDURE);
        } else {
            return parentPool.getOrCreatePool(procedureId, queryMemoryLimit, MemoryType.STORED_PROCEDURE);
        }
    }

    public MemoryPool createStoredFunctionMemoryPool(String procedureId, long queryMemoryLimit, MemoryPool parentPool) {
        if (parentPool == null) {
            return apMemoryPool.getOrCreatePool(procedureId, queryMemoryLimit, MemoryType.STORED_FUNCTION);
        } else {
            return parentPool.getOrCreatePool(procedureId, queryMemoryLimit, MemoryType.STORED_FUNCTION);
        }
    }

    public TpMemoryPool getTpMemoryPool() {
        return tpMemoryPool;
    }

    public ApMemoryPool getApMemoryPool() {
        return apMemoryPool;
    }

    public MemoryPool getCacheMemoryPool() {
        return cacheMemoryPool;
    }

    public GlobalMemoryPool getGlobalMemoryPool() {
        return globalMemoryPool;
    }

    public long getProcedureCacheLimit() {
        return procedureCacheLimit;
    }

    public long getFunctionCacheLimit() {
        return functionCacheLimit;
    }
}
