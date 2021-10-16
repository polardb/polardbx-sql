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
import com.alibaba.polardbx.gms.locality.LocalityId;
import com.alibaba.polardbx.gms.locality.LocalityInfoAccessor;
import com.alibaba.polardbx.gms.locality.LocalityInfoRecord;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * @author moyi
 * @since 2020/03
 */
public class LocalityManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(LocalityManager.class);
    private volatile Map<LocalityId, LocalityInfo> localityCache;
    // TODO(moyi) hierarchy of primary_zone
    private volatile PrimaryZoneInfo systemPrimaryZone = new PrimaryZoneInfo();
    private static LocalityManager INSTANCE = new LocalityManager();

    private LocalityManager() {
    }

    @Override
    protected void doInit() {
        super.doInit();
        logger.info("init LocalityManager");
        setupConfigListener();
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            LocalityInfoConfigListener listener = new LocalityInfoConfigListener();
            String dataId = MetaDbDataIdBuilder.getLocalityInfoDataId();

            MetaDbConfigManager.getInstance().register(dataId, conn);
            MetaDbConfigManager.getInstance().bindListener(dataId, listener);

            reloadLocalityInfoFromMetaDB();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup locality config_listener failed");
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    public LocalityInfo getDefaultLocality() {
        LocalityId id =
            LocalityId.from(LocalityInfoRecord.LOCALITY_TYPE_DEFAULT, LocalityInfoRecord.LOCALITY_ID_DEFAULT);
        return this.localityCache.get(id);
    }

    public static LocalityManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    protected static class LocalityInfoConfigListener implements ConfigListener {

        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LocalityManager.getInstance().reloadLocalityInfoFromMetaDB();
        }
    }

    /**
     * Get locality of database with inherited from default
     */
    public LocalityInfo getLocalityOfDb(long dbId) {
        LocalityId id = LocalityId.from(LocalityInfoRecord.LOCALITY_TYPE_DATABASE, dbId);
        return localityCache.get(id);
    }

    public void setLocalityOfDb(long dbId, String locality) {
        LocalityId id = LocalityId.ofDatabase(dbId);
        LocalityInfo info = new LocalityInfo(id, locality);
        storeLocalityInDatabase(info);

        this.localityCache.put(id, info);
    }

    public void deleteLocalityOfDb(long dbId) {
        LocalityId id = LocalityId.ofDatabase(dbId);
        deleteLocalityInDatabase(id);
        localityCache.remove(id);
    }

    public Supplier<PrimaryZoneInfo> getSupplier() {
        return () -> this.systemPrimaryZone;
    }

    public void setSystemPrimaryZone(PrimaryZoneInfo primaryZoneInfo) {
        storePrimaryZoneInDatabase(LocalityId.defaultId(), primaryZoneInfo);
        this.systemPrimaryZone = primaryZoneInfo;
    }

    public PrimaryZoneInfo getSystemPrimaryZone() {
        return this.systemPrimaryZone;
    }

    public PrimaryZoneInfo getPrimaryZoneOfPartition(String dbName, String tableName, String partitionName) {
        throw new RuntimeException("TODO");
    }

    public LocalityInfo getLocalityOfTable(long tableId) {
        LocalityId id = LocalityId.from(LocalityInfoRecord.LOCALITY_TYPE_TABLE, tableId);
        return localityCache.get(id);
    }

    public void setLocalityOfTable(long tableId, String locality) {
        LocalityId id = LocalityId.ofTable(tableId);
        LocalityInfo info = new LocalityInfo(id, locality);
        storeLocalityInDatabase(info);

        this.localityCache.put(id, info);
    }

    public void deleteLocalityOfTable(long tableId) {
        LocalityId id = LocalityId.ofTable(tableId);
        deleteLocalityInDatabase(id);
        localityCache.remove(id);
    }

    public void setLocalityOfTableGroup(String schema, long tgId, String locality) {
        LocalityId id = LocalityId.ofTableGroup(tgId);
        LocalityInfo info = new LocalityInfo(id, locality);
        storeLocalityInDatabase(info);
        this.localityCache.put(id, info);
    }

    public LocalityInfo getLocalityOfTableGroup(String dbName, long tgId) {
        LocalityId id = LocalityId.from(LocalityInfoRecord.LOCALITY_TYPE_TABLEGROUP, tgId);
        return localityCache.get(id);
    }

    public LocalityInfo getLocalityOfPartition(String dbName, String tableName, String partitionName) {
        throw new RuntimeException("TODO");
    }

    public LocalityInfo getLocalityOfPartitionGroup(String dbName, long partitionGroupId) {
        throw new RuntimeException("TODO");
    }

    public void deleteLocalityOfTableGroup(long dbId, long tgId) {
        LocalityId id = LocalityId.ofTableGroup(tgId);
        localityCache.remove(id);
        deleteLocalityInDatabase(id);
    }

    /****************** Database access *********************/

    private void deleteLocalityInDatabase(LocalityId id) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            LocalityInfoAccessor accessor = new LocalityInfoAccessor();
            accessor.setConnection(conn);

            int affected = accessor.deleteLocality(id.objectType, id.objectId);

            // notify listener
            if (affected > 0) {
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLocalityInfoDataId(), conn);
                conn.commit();
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLocalityInfoDataId());
            } else {
                conn.commit();
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    private void storePrimaryZoneInDatabase(LocalityId id, PrimaryZoneInfo primaryZoneInfo) {
        if (!id.equals(LocalityId.defaultId())) {
            throw new RuntimeException("only default_id is supported");
        }

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            LocalityInfoAccessor accessor = new LocalityInfoAccessor();
            accessor.setConnection(conn);

            accessor.insertLocalityInfo(
                id.objectType,
                id.objectId,
                primaryZoneInfo.serialize(),
                "");

            // notify listener
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLocalityInfoDataId(), conn);
            conn.commit();

            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLocalityInfoDataId());
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    private void storeLocalityInDatabase(LocalityInfo localityInfo) {
        Objects.requireNonNull(localityInfo.getId());
        Objects.requireNonNull(localityInfo.getLocality());

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            conn.setAutoCommit(false);
            LocalityInfoAccessor accessor = new LocalityInfoAccessor();
            accessor.setConnection(conn);

            accessor.insertLocalityInfo(
                localityInfo.getId().objectType,
                localityInfo.getId().objectId,
                "",
                localityInfo.getLocality());

            // notify listener
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLocalityInfoDataId(), conn);
            conn.commit();

            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLocalityInfoDataId());
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Load all records in system-table to in-memory cache.
     */
    private void reloadLocalityInfoFromMetaDB() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            LocalityInfoAccessor accessor = new LocalityInfoAccessor();
            accessor.setConnection(conn);
            Map<LocalityId, LocalityInfo> newCache = new ConcurrentHashMap<>();

            List<LocalityInfoRecord> records = accessor.getAllLocality();
            for (LocalityInfoRecord record : records) {
                LocalityInfo info = LocalityInfo.from(record);
                newCache.put(info.getId(), info);

                // setup system primary_zone
                if (info.getId().equals(LocalityId.defaultId())) {
                    this.systemPrimaryZone = PrimaryZoneInfo.parse(record.primaryZone);
                    logger.info("reload primary_zone from metadb: " + record.primaryZone);
                }
            }

            this.localityCache = newCache;

            logger.info("reload locality cache from metadb: " + newCache);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

}
