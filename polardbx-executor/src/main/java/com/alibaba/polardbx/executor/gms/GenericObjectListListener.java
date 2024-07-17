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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.ConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.TableNamesRecord;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.topology.ConfigListenerDataIdRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class GenericObjectListListener extends AbstractLifecycle implements ConfigListener {

    protected static final ConfigManager CONFIG_MANAGER = MetaDbConfigManager.getInstance();

    protected static final long NOOP_OP_VERSION = 0L;

    protected final String schemaName;
    protected final Map<String, ConfigListener> objectListeners = new ConcurrentHashMap<>();

    public GenericObjectListListener(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    protected void doInit() {
        super.doInit();
        bindNewListeners(true);
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        unbindAllListeners();
    }

    @Override
    public void onHandleConfig(String dataId, long newOpVersion) {
        bindNewListeners(false);
        unbindExpiredListeners();
    }

    protected abstract List<SystemTableRecord> fetchTablesName();

    /**
     * Get an object data id.
     *
     * @param record An object record
     * @return The object data id
     */
    protected abstract String getDataId(String tableSchema, String tableName);

    /**
     * Get the prefix of data ids in current schema.
     *
     * @return The prefix of data ids
     */
    protected abstract String getDataIdPrefix();

    /**
     * Get an object listener according to the object record.
     *
     * @param record An object record
     * @return The object listener
     */
    protected abstract ConfigListener getObjectListener(String tableSchema, String tableName);

    private void bindNewListeners(boolean isInit) {
        // Bind newly registered dataIds and object listeners.
        List<SystemTableRecord> records = fetchTablesName();

        if (records != null && records.size() > 0) {
            for (SystemTableRecord record : records) {
                TableNamesRecord tablesNameRecord = (TableNamesRecord) record;
                String objectDataId = getDataId(schemaName, tablesNameRecord.tableName);

                if (isInit || !objectListeners.keySet().contains(objectDataId)) {
                    // New a specific listener for the object.
                    ConfigListener objectListener = getObjectListener(schemaName, tablesNameRecord.tableName);

                    // Bind them to enable timed task.
                    CONFIG_MANAGER.bindListener(objectDataId, objectListener);

                    if (!isInit) {
                        // Trigger table meta reload right now to avoid subsequent sync issue for table dataId.
                        objectListener.onHandleConfig(objectDataId, NOOP_OP_VERSION);
                    }

                    // Cache them to record which tables have been bound:.
                    objectListeners.put(objectDataId, objectListener);
                }
            }
        }
    }

    private void unbindExpiredListeners() {
        // Unbind unregistered dataIds and object listeners.
        String dataIdPrefix = getDataIdPrefix();
        Set<String> activeDataIds = fetchActiveDataIds(dataIdPrefix);

        Iterator<Entry<String, ConfigListener>> iterator = objectListeners.entrySet().iterator();

        while (iterator.hasNext()) {
            Entry<String, ConfigListener> entry = iterator.next();

            String cachedDataId = entry.getKey();

            if (!activeDataIds.contains(cachedDataId)) {
                ConfigListener cachedListener = entry.getValue();

                if (cachedListener != null) {
                    cachedListener.onHandleConfig(cachedDataId, NOOP_OP_VERSION);
                }

                // Unbind them since unregister has been called prior to this.
                CONFIG_MANAGER.unbindListener(cachedDataId);

                // Remove the table listener.
                iterator.remove();
            }
        }
    }

    private void unbindAllListeners() {
        for (Entry<String, ConfigListener> entry : objectListeners.entrySet()) {
            String objectDataId = entry.getKey();
            ConfigListener objectListener = entry.getValue();
            if (objectListener != null) {
                // Invalidate related caches.
                objectListener.onHandleConfig(objectDataId, NOOP_OP_VERSION);
            }
            // Unbind the object listener.
            CONFIG_MANAGER.unbindListener(objectDataId);
        }
        objectListeners.clear();
    }

    private Set<String> fetchActiveDataIds(String dataIdPrefix) {
        ConfigListenerAccessor accessor = new ConfigListenerAccessor();

        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            accessor.setConnection(metaDbConn);

            Set<String> activeDataIds = new HashSet<>();
            List<ConfigListenerDataIdRecord> records = accessor.getDataIdsOnlyByPrefix(dataIdPrefix);

            for (ConfigListenerDataIdRecord record : records) {
                activeDataIds.add(MetaDbDataIdBuilder.formatDataId(record.dataId));
            }

            return activeDataIds;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            accessor.setConnection(null);
        }
    }

}
