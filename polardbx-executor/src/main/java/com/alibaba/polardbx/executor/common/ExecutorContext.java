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

package com.alibaba.polardbx.executor.common;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.gsi.GsiManager;
import com.alibaba.polardbx.executor.repo.RepositoryHolder;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author mengshi.sunmengshi 2013-12-4 下午6:16:32
 * @since 5.0.0
 */
public class ExecutorContext {

    private RepositoryHolder repositoryHolder = null;
    private TopologyHandler topologyHandler = null;
    private ITopologyExecutor topologyExecutor = null;
    private ITransactionManager transactionManager = null;
    private AbstractSequenceManager sequenceManager = null;
    private StorageInfoManager storageInfoManager = null;
    private GsiManager gsiManager = null;
    private NodeStatusManager nodeStatusManager;

    private static Map<String, ExecutorContext> executorContextMap = new ConcurrentHashMap<String, ExecutorContext>();

    public static ExecutorContext getContext(String schemaName) {
        if (schemaName == null || schemaName.isEmpty()) {
            throw new RuntimeException("schemaName is null!");
        }
        if (schemaName.equalsIgnoreCase(MetaDbSchema.NAME)) {
            schemaName = InformationSchema.NAME;
        }
        String schemaNameLowerCase = schemaName.toLowerCase();
        ExecutorContext executorContext = executorContextMap.get(schemaNameLowerCase);
        if (executorContext == null) {
            IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
            if ((!ConfigDataMode.isFastMock()) && serverConfigManager != null) {
                // Avoid dead loop in fast mock mode
                // When running unit test, ServerConfigManager could be null
                serverConfigManager.getAndInitDataSourceByDbName(schemaName);
            }
            executorContext = executorContextMap.get(schemaNameLowerCase);
        }
        return executorContext;
    }

    public static void setContext(String schemaName, ExecutorContext context) {
        executorContextMap.put(schemaName.toLowerCase(), context);
    }

    public static void clearContext(String schemaName) {
        executorContextMap.remove(schemaName.toLowerCase());
    }

    public RepositoryHolder getRepositoryHolder() {
        return repositoryHolder;
    }

    public void setRepositoryHolder(RepositoryHolder repositoryHolder) {
        this.repositoryHolder = repositoryHolder;
    }

    public ITopologyExecutor getTopologyExecutor() {
        return topologyExecutor;
    }

    public void setTopologyExecutor(ITopologyExecutor topologyExecutor) {
        this.topologyExecutor = topologyExecutor;
    }

    public TopologyHandler getTopologyHandler() {
        return topologyHandler;
    }

    public void setTopologyHandler(TopologyHandler topologyHandler) {
        this.topologyHandler = topologyHandler;
    }

    public ITransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(ITransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public AbstractSequenceManager getSequenceManager() {
        // try {
        // lazy启动,兼容老系统
        if (!this.sequenceManager.isInited()) {
            this.sequenceManager.init();
        }
        // } catch (TddlException e) {
        // throw GeneralUtil.nestedException(e);
        // }
        return this.sequenceManager;
    }

    public void setSeqeunceManager(AbstractSequenceManager sequenceManager) {
        this.sequenceManager = sequenceManager;
    }

    public StorageInfoManager getStorageInfoManager() {
        return storageInfoManager;
    }

    public void setStorageInfoManager(StorageInfoManager storageInfoManager) {
        this.storageInfoManager = storageInfoManager;
    }

    public GsiManager getGsiManager() {
        return gsiManager;
    }

    public void setGsiManager(GsiManager gsiManager) {
        this.gsiManager = gsiManager;
    }
}
