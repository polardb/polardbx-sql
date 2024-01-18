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

package com.alibaba.polardbx.optimizer.locality;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoAccessor;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author yijin
 * @since 2023/01
 */
public class StoragePoolManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(LocalityManager.class);
    private static Boolean mockMode = false;

    public static String DEFAULT_STORAGE_POOL_NAME = "_default";

    public static String RECYCLE_STORAGE_POOL_NAME = "_recycle";
    public static String ALL_STORAGE_POOL = "__all_storage_pool";
    private volatile Map<Long, StoragePoolInfo> storagePoolCache;

    public volatile Map<String, StoragePoolInfo> storagePoolCacheByName;

    public volatile Map<String, String> storagePoolMap;

    private static StoragePoolManager INSTANCE = new StoragePoolManager();

    private List<String> defaultStorageInstList;

    public static StoragePoolManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    private StoragePoolManager() {
    }

    public Boolean inValidStoragePoolName(String storagePool) {
        return !storagePoolCacheByName.containsKey(storagePool);
    }

    public StoragePoolInfo getStoragePoolInfo(String storagePool) {
        return storagePoolCacheByName.get(storagePool);
    }

    public LocalityDesc getDefaultLocalityDesc() {
        return LocalityInfoUtils.parse("dn=" + StringUtils.join(defaultStorageInstList, ","));
    }

    public LocalityDesc getDefaultStoragePoolLocalityDesc() {
        return LocalityInfoUtils.parse(LocalityDesc.STORAGE_POOL_PREFIX + "'" + DEFAULT_STORAGE_POOL_NAME + "'");
    }

    public Boolean isTriggered() {
        return !storagePoolCache.isEmpty();
    }

    @Override
    protected void doInit() {
        super.doInit();
        logger.info("init StoragePoolManager");
        if (mockMode) {
            this.storagePoolCache = new HashMap<>();
            this.storagePoolCacheByName = new HashMap<>();
            return;
        }
        setupConfigListener();
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoConfigListener listener = new StoragePoolInfoConfigListener();
            String dataId = MetaDbDataIdBuilder.getStoragePoolInfoDataId();

            MetaDbConfigManager.getInstance().register(dataId, conn);
            MetaDbConfigManager.getInstance().bindListener(dataId, listener);
            reloadStoragePoolInfoFromMetaDb();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup storage pool config_listener failed");
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    protected static class StoragePoolInfoConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            StoragePoolManager.getInstance().reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void addStoragePool(String storagePoolName, String dnIds, String undeletableDnId) {
        if (!storagePoolCacheByName.containsKey(storagePoolName)) {
            storeStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
            if (storagePoolCacheByName.containsKey(RECYCLE_STORAGE_POOL_NAME)) {
                // remove from recycle storage pool
                StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(RECYCLE_STORAGE_POOL_NAME);
                Set<String> dnIdList = Arrays.stream(dnIds.split(",")).collect(Collectors.toSet());
                Set<String> targetDnIdList = storagePoolInfo.getDnLists().stream().collect(Collectors.toSet());
                targetDnIdList.removeAll(dnIdList);
                String targetDnIdStr = StringUtils.join(targetDnIdList, ",");
                updateStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, targetDnIdStr, "");
            }
            reloadStoragePoolInfoFromMetaDb();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("duplicate storage pool name '%s' found! " + storagePoolName));
        }
    }

    public void convertDefaultStoragePool() {
        if (!storagePoolCacheByName.containsKey("")) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get("");
            String undeletableDnId = storagePoolInfo.getUndeletableDnId();
            String dnIds = storagePoolInfo.getDnIds();
            String storagePoolName = StoragePoolManager.DEFAULT_STORAGE_POOL_NAME;
            storeStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void deleteStoragePool(String storagePoolName) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            deleteStoragePoolInfo(storagePoolName);
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void updateStoragePoolName(String originalStoragePoolName, String targetStoragePoolName) {
        if (storagePoolCacheByName.containsKey(originalStoragePoolName)) {
            if (!storagePoolCacheByName.containsKey(targetStoragePoolName)) {
                updateStoragePoolInfoName(originalStoragePoolName, targetStoragePoolName);
            } else {
                StoragePoolInfo targetStoragePoolInfo = storagePoolCacheByName.get(targetStoragePoolName);
                StoragePoolInfo originalStoragePoolInfo = storagePoolCacheByName.get(originalStoragePoolName);
                String undeletableDnId = targetStoragePoolInfo.getUndeletableDnId();
                Set<String> dnIdList = originalStoragePoolInfo.getDnLists().stream().collect(Collectors.toSet());
                dnIdList.addAll(targetStoragePoolInfo.getDnLists());
                String dnIds = StringUtils.join(dnIdList, ",");
                updateStoragePoolInfo(targetStoragePoolName, dnIds, undeletableDnId);
                deleteStoragePool(originalStoragePoolName);
            }
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void deleteAllStoragePoolInfo() {
        truncateStoragePoolInfo();
        reloadStoragePoolInfoFromMetaDb();
    }

    public void shrinkStoragePoolSimply(String storagePoolName, String dnIds) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            String[] fullDnIds = storagePoolInfo.getDnIds().split(",");
            String[] removeDnIds = dnIds.split(",");
            Set<String> fullDnSet = Arrays.stream(fullDnIds).collect(Collectors.toSet());
            String undeletableDnId = storagePoolInfo.getUndeletableDnId();
            fullDnSet.removeAll(Arrays.asList(removeDnIds));
            String aftershrinkDnIds = StringUtils.join(fullDnSet, ",");
            updateStoragePoolInfo(storagePoolName, aftershrinkDnIds, undeletableDnId);
        }
        reloadStoragePoolInfoFromMetaDb();
    }

    public void appendStoragePool(String storagePoolName, String dnIds, String undeletableDnId) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            String afterAppendDnIds = dnIds;
            if (!StringUtils.isEmpty(storagePoolInfo.getDnIds())) {
                afterAppendDnIds = storagePoolInfo.getDnIds() + "," + dnIds;
            }
            String afterAppendUndeletableDnId = storagePoolInfo.getUndeletableDnId();
            if (!StringUtils.isEmpty(afterAppendUndeletableDnId)) {
                undeletableDnId = afterAppendUndeletableDnId;
            }
            updateStoragePoolInfo(storagePoolName, afterAppendDnIds, undeletableDnId);
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void shrinkStoragePool(String storagePoolName, String dnIds, String undeletableDnId) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            Set<String> recycleDnSet;
            Boolean recycleExists = true;
            if (storagePoolCacheByName.containsKey(RECYCLE_STORAGE_POOL_NAME)) {
                recycleDnSet = new HashSet<>(storagePoolCacheByName.get(RECYCLE_STORAGE_POOL_NAME).getDnLists());
            } else {
                recycleDnSet = new HashSet<>();
                recycleExists = false;
            }
            StoragePoolInfo recyclestoragePoolInfo = storagePoolCacheByName.get(RECYCLE_STORAGE_POOL_NAME);
            String[] fullDnIds = storagePoolInfo.getDnIds().split(",");
            String[] removeDnIds = dnIds.split(",");
            Arrays.stream(removeDnIds).forEach(o -> recycleDnSet.add(o));
            String recycleDnIds = StringUtils.join(recycleDnSet, ",");
            Set<String> fullDnList = Arrays.stream(fullDnIds).collect(Collectors.toSet());
            undeletableDnId = storagePoolInfo.getUndeletableDnId();
            Arrays.stream(removeDnIds).forEach(o -> fullDnList.remove(o));
            String aftershrinkDnIds = StringUtils.join(fullDnList, ",");
            if (fullDnList.isEmpty()) {
                undeletableDnId = "";
            }
            updateStoragePoolInfo(storagePoolName, aftershrinkDnIds, undeletableDnId);
            if (recycleExists) {
                updateStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, recycleDnIds, "");
            } else {
                storeStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, recycleDnIds, "");
            }
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void shrinkStoragePoolBack(String storagePoolName, String dnIds, String undeletableDnId) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            Set<String> recycleDnSet;
            Boolean recycleExists = true;
            if (storagePoolCacheByName.containsKey(RECYCLE_STORAGE_POOL_NAME)) {
                recycleDnSet = new HashSet<>(storagePoolCacheByName.get(RECYCLE_STORAGE_POOL_NAME).getDnLists());
            } else {
                recycleDnSet = new HashSet<>();
                recycleExists = false;
            }
            String[] fullDnIds = storagePoolInfo.getDnIds().split(",");
            String[] removeDnIds = dnIds.split(",");
            Arrays.stream(removeDnIds).forEach(o -> recycleDnSet.add(o));
            String recycleDnIds = StringUtils.join(recycleDnSet, ",");
            List<String> fullDnList = Arrays.stream(fullDnIds).collect(Collectors.toList());
            undeletableDnId = storagePoolInfo.getUndeletableDnId();
            Arrays.stream(removeDnIds).forEach(o -> fullDnList.remove(o));
            String aftershrinkDnIds = StringUtils.join(fullDnList, ",");
            updateStoragePoolInfo(storagePoolName, aftershrinkDnIds, undeletableDnId);
            if (recycleExists) {
                updateStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, recycleDnIds, "");
            } else {
                storeStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, recycleDnIds, "");
            }
            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void storeStoragePoolInfo(String storagePoolName, String dnIds, String undeletableDnId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.addNewStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteStoragePoolInfo(String storagePoolName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.deleteStoragePoolInfo(storagePoolName);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void truncateStoragePoolInfo() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.truncateStoragePoolInfo();
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

    }

    public void updateStoragePoolInfoName(String originalStoragePoolName, String targetStoragePoolName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.updateStoragePoolInfoName(originalStoragePoolName, targetStoragePoolName);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateStoragePoolInfo(String storagePoolName, String dnIds, String undeletableDnId) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.updateStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

    }

    /**
     * Get locality of database with inherited from default
     */
    /**
     * Load all records in system-table to in-memory cache.
     */
    public synchronized void reloadStoragePoolInfoFromMetaDb() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            Map<String, StoragePoolInfo> newCacheByName = new ConcurrentHashMap<>();
            Map<Long, StoragePoolInfo> newCache = new ConcurrentHashMap<>();
            Map<String, String> newStoragePoolMap = new ConcurrentHashMap<>();

            List<StoragePoolInfoRecord> records = accessor.getAllStoragePoolInfoRecord();
            Set<String> occupiedStorageIds = new HashSet<>();
            for (StoragePoolInfoRecord record : records) {
                StoragePoolInfo info = StoragePoolInfo.from(record);
                newCache.put(info.getId(), info);
                newCacheByName.put(info.getName(), info);
                Arrays.stream(info.getDnIds().split(",")).forEach(o -> newStoragePoolMap.put(o, info.getName()));
                occupiedStorageIds.addAll(Arrays.stream(info.getDnIds().split(",")).collect(Collectors.toList()));
                // setup system primary_zone
            }

            List<String> storageIds = new ArrayList<>();
            if (!newCacheByName.containsKey(DEFAULT_STORAGE_POOL_NAME)) {
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(conn);
                List<StorageInfoRecord> storageInfoRecords =
                    storageInfoAccessor.getStorageInfosByInstId(InstIdUtil.getInstId()).
                        stream().filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER)
                        .filter(o -> !occupiedStorageIds.contains(o.storageInstId)).collect(Collectors.toList());
                storageIds = storageInfoRecords.stream().map(o -> o.storageInstId).collect(Collectors.toList());
            } else {
                storageIds = newCacheByName.get(DEFAULT_STORAGE_POOL_NAME).getDnLists();
            }
            this.defaultStorageInstList = storageIds;
            this.storagePoolCache = newCache;
            this.storagePoolCacheByName = newCacheByName;
            this.storagePoolMap = newStoragePoolMap;

            logger.info("reload storage pool cache from metadb: ");
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

}
