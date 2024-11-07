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

package com.alibaba.polardbx.executor.gsi;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;

import javax.sql.DataSource;
import java.util.EnumSet;


/**
 * @author chenmo.cm
 */
public class GsiManager extends AbstractLifecycle {

    private final TopologyHandler topologyHandler;
    private final StorageInfoManager storageInfoManager;

    public GsiManager(TopologyHandler topologyHandler, StorageInfoManager storageInfoManager) {
        this.topologyHandler = topologyHandler;
        this.storageInfoManager = storageInfoManager;
    }

    @Override
    protected void doInit() {
        getGsiMetaManager().init();
    }

    /**
     * Get meta of gsi with specified status from system table
     *
     * @param primaryOrIndexTableName primary or index table name
     * @param statusSet gsi status
     * @return gsi meta
     */
    public GsiMetaBean getGsiTableAndIndexMeta(String schema, String primaryOrIndexTableName,
                                               EnumSet<IndexStatus> statusSet) {
        return getGsiMetaManager().getTableAndIndexMeta(schema, primaryOrIndexTableName, statusSet);
    }

    public GsiMetaManager getGsiMetaManager() {
        DataSource gsiMgrDs = MetaDbDataSource.getInstance().getDataSource();
        return new GsiMetaManager(gsiMgrDs, topologyHandler.getSchemaName());
    }

    /**
     * Get meta of gsi with specified status from system table
     *
     * @param tableName primary table name
     * @param indexName index table name
     * @return gsi meta
     */
    public GsiMetaManager.GsiIndexMetaBean getGsiIndexMeta(String schema, String tableName, String indexName,
                                                           EnumSet<IndexStatus> statusSet) {
        return getGsiMetaManager().getIndexMeta(schema, tableName, indexName, statusSet);
    }
}
