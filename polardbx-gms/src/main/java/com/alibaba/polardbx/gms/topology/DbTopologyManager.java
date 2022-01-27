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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.util.AppNameUtil;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class DbTopologyManager {
    private final static Logger logger = LoggerFactory.getLogger(DbTopologyManager.class);

    public static final String CREATE_DB_IF_NOT_EXISTS_SQL_TEMPLATE = "create database if not exists `%s`";

    public static final String DROP_DB_IF_EXISTS_SQL_TEMPLATE = "drop database if exists `%s`";

    public static final String CHARSET_TEMPLATE = " character set `%s`";

    public static final String COLLATION_TEMPLATE = " collate `%s`";

    public static final String DEFAULT_DB_CHARACTER_SET = "utf8mb4";

    public static final int DEFAULT_SHARD_DB_COUNT_EACH_STORAGE_INST = 8;

    public static final int DEFAULT_MAX_LOGICAL_DB_COUNT = 32;

    public static final int DEFAULT_RENAME_PHY_DB_BATCH_SIZE = 1024;

    public static String defaultCharacterSetForCreatingDb = DEFAULT_DB_CHARACTER_SET;

    public static int maxLogicalDbCount = DbTopologyManager.DEFAULT_MAX_LOGICAL_DB_COUNT;

    protected static int shardDbCountEachStorageInst = DbTopologyManager.DEFAULT_SHARD_DB_COUNT_EACH_STORAGE_INST;

    protected static boolean enablePartitionManagement = false;

    protected static SchemaMetaCleaner schemaMetaCleaner = null;

    public static List<String> singleGroupStorageInstList = new ArrayList<>();

    public static void initDbTopologyManager(SchemaMetaCleaner cleanerImpl) {
        schemaMetaCleaner = cleanerImpl;
        refreshSingleGroupStorageInsts();

        // The dataId of metadb.lock is used to build meta db lock
        MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getMetadbLockDataId(), null);
    }

    public static void refreshSingleGroupStorageInsts() {
        // load singleGroupStorageInstList
        String propVal = MetaDbInstConfigManager.getInstance()
            .getInstProperty(ConnectionProperties.SINGLE_GROUP_STORAGE_INST_LIST);
        if (propVal != null && !propVal.isEmpty()) {
            String[] storageInstList = propVal.split(",");
            for (String storageInstId : storageInstList) {
                if (storageInstId != null && !storageInstId.isEmpty()) {
                    singleGroupStorageInstList.add(storageInstId);
                }
            }
        }
    }

    public static void resetShardDbCountEachStorageInst(int shardDbCountEachStorageInst) {
        DbTopologyManager.shardDbCountEachStorageInst = shardDbCountEachStorageInst;
    }

    public static int getNormalDbCountFromMetaDb() {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);

            int allNormalDbSize = 0;

            List<DbInfoRecord> partDbRecords =
                dbInfoAccessor.getDbInfoByType(DbInfoRecord.DB_TYPE_PART_DB);
            allNormalDbSize += partDbRecords.size();

            List<DbInfoRecord> newPartDbInfoRecords =
                dbInfoAccessor.getDbInfoByType(DbInfoRecord.DB_TYPE_NEW_PART_DB);
            allNormalDbSize += newPartDbInfoRecords.size();

            return allNormalDbSize;

        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static List<DbInfoRecord> getNewPartDbInfoFromMetaDb() {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            return dbInfoAccessor.getDbInfoByType(DbInfoRecord.DB_TYPE_NEW_PART_DB);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    /**
     * Check if db exist
     */
    public static boolean checkDbExists(String dbName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);

            DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbName(dbName);
            if (dbInfo == null || dbInfo.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                return false;
            }
            return true;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, "failed to chece exists for db, err is " +
                ex.getMessage());
        }
    }

    public static void createInternalSystemDbIfNeed(MetaDbDataSource metaDbDs, String dbName, String phyDbName,
                                                    String groupName, String charset, int dbType) {
        DataSource dataSource = metaDbDs.getDataSource();
        try (Connection metaDbConn = dataSource.getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);

            DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbName(dbName);
            if (dbInfo != null && dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                return;
            }

            CreateDbInfo createDbInfo = new CreateDbInfo();
            createDbInfo.dbName = dbName;
            createDbInfo.charset = charset;
            createDbInfo.dbType = dbType;
            createDbInfo.singleGroup = groupName;
            createDbInfo.defaultDbIndex = groupName;
            createDbInfo.isCreateIfNotExists = true;

            Map<String, String> groupAndPhyDbMap = Maps.newHashMap();
            groupAndPhyDbMap.put(createDbInfo.singleGroup, phyDbName);
            createDbInfo.groupPhyDbMap = groupAndPhyDbMap;

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            List<StorageInfoRecord> storageInfoRecords =
                storageInfoAccessor.getStorageInfosByInstKind(StorageInfoRecord.INST_KIND_META_DB);
            if (storageInfoRecords.size() == 0) {
                // no found any storage inst of meta db
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "No found any storage inst of metaDb");
            }
            StorageInfoRecord metaDbStorageInfo = storageInfoRecords.get(0);

            List<String> storageInstIdList = Lists.newArrayList();
            storageInstIdList.add(metaDbStorageInfo.storageInstId);
            createDbInfo.storageInstList = storageInstIdList;

            createDbInfo.groupLocator =
                new DefaultGroupLocator(createDbInfo.groupPhyDbMap, createDbInfo.storageInstList, new ArrayList<>());
            DbTopologyManager.createLogicalDb(createDbInfo);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, "failed to create default db, err is " +
                ex.getMessage());
        }
    }

    public static long createLogicalDb(CreateDbInfo createDbInfo) {
        long dbId = -1;

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {

            // acquire MetaDb Lock by for update, to avoiding concurrent create & drop databases
            metaDbLockConn.setAutoCommit(false);
            try {
                LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
            } catch (Throwable ex) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                    String.format("Get metaDb lock timeout during creating db[%s], please retry", createDbInfo.dbName));
            }

            // ---- check if logical db exists ----
            String dbName = createDbInfo.dbName;
            boolean createIfNotExist = createDbInfo.isCreateIfNotExists;
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbName(dbName);
            boolean hasDbConfig = false;
            if (dbInfo != null) {
                if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                    if (createIfNotExist) {
                        return dbId;
                    }
                    // throw exception
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Create db error, db[%s] has already exist", dbName));
                } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_CREATING) {
                    // ignore, just continue to create phy dbs
                    hasDbConfig = true;
                } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                    // call dropLogicalDb to clean env, and then continue to creating new db
                    dropLogicalDbWithConn(dbName, true, metaDbConn, metaDbLockConn, createDbInfo.socketTimeout);
                    hasDbConfig = false;
                }
            }

            if (!hasDbConfig) {
                // ---- if not exists, add topology info metadb for logical db by using trx, db_status=creating ----
                addDbTopologyConfig(createDbInfo);
                dbInfo = dbInfoAccessor.getDbInfoByDbName(dbName);
                dbId = dbInfo.id;
            }

            // ---- get lock by db_name for double check ----
            metaDbConn.setAutoCommit(false);
            DbInfoRecord dbInfoForUpdate = dbInfoAccessor.getDbInfoByDbNameForUpdate(dbName);
            if (DbInfoManager.isNormalDbType(dbInfoForUpdate.dbType)
                && dbInfoForUpdate.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Create db error, db[%s] has already exist", dbName));
            }

            // ---- create physical dbs by topology info loading from metadb ----
            String instId = InstIdUtil.getInstId();// make sure that instId is master inst id
            createPhysicalDbsIfNotExists(instId, dbInfo.dbName, dbInfo.charset, dbInfo.collation);

            // ---- update db_status to running by trx  ----
            dbInfoAccessor.updateDbStatusByDbName(dbName, DbInfoRecord.DB_STATUS_RUNNING);

            // ----- add the dataId info for new db----
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);

            // ----- notify other node to reload dbInfos ----
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbInfoDataId(), metaDbConn);

            // commit configs
            metaDbConn.commit();

            // sync other node to reload dbInfo quickly
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());

            // release meta db lock
            LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);
            return dbId;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected static void dropLogicalDbWithConn(String dbName, boolean isDropIfExists, Connection metaDbConn,
                                                Connection metaDbLockConn, long socketTimeout) throws SQLException {

        // acquire MetaDb Lock by for update, to avoiding concurrent create & drop databases
        metaDbLockConn.setAutoCommit(false);
        try {
            LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
        } catch (Throwable ex) {
            // throw exception
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Get metaDb lock timeout during drop db[%s], please retry", dbName));
        }

        // ---- check if logical db exists ----
        DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
        dbInfoAccessor.setConnection(metaDbConn);

        metaDbConn.setAutoCommit(false);
        DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbNameForUpdate(dbName);
        if (dbInfo != null) {
            if (SystemDbHelper.INFO_SCHEMA_DB_NAME.equalsIgnoreCase(dbInfo.dbName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DROP_INFORMATION_SCHEMA);
            } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                // when db_status==running, it means the db is normal
                // so just continue to drop phy dbs and remove db topology configs
            } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                // when db_status==dropping, it means the db is dropping,
                // ignore, just continue to drop phy dbs  and remove db topology configs
            } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_CREATING) {
                // when db_status==creating, it means the db has not finish creating phy dbs
                // so ignore, just continue to drop phy dbs and remove db topology configs
                if (!isDropIfExists) {
                    // throw exception
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Drop db error, db[%s] does not exist", dbName));
                }
            }
        } else {
            if (isDropIfExists) {
                return;
            }
            // throw exception
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("Drop db error, db[%s] does not exist", dbName));
        }

        // ---- update db_status=dropping to running by trx  ----
        dbInfoAccessor.updateDbStatusByDbName(dbName, DbInfoRecord.DB_STATUS_DROPPING);

        // ----- unregister the dataId for the removed db
        MetaDbConfigManager.getInstance().unregister(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);

        // ----- notify other node to reload dbInfos & release all resources of db ----
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbInfoDataId(), metaDbConn);

        metaDbConn.commit();

        // sync other node to reload dbInfo and release resources quickly
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());

        // ---- drop physical dbs according the topology info fo metadb ----
        String instId = InstIdUtil.getInstId();
        dropPhysicalDbsIfExists(instId, dbName, socketTimeout);

        // ---- remove topology config & group config from metadb by using trx ----
        metaDbConn.setAutoCommit(false);

        // ----- cleanup all schema meta infos & configs
        if (DbTopologyManager.schemaMetaCleaner != null) {
            schemaMetaCleaner.clearSchemaMeta(dbName, metaDbConn);
        }

        // ---- remove topology configs
        removeDbTopologyConfig(dbName, metaDbConn);
        metaDbConn.commit();

    }

    public static void dropLogicalDb(DropDbInfo dropDbInfo) {
        final String moveGroupTaskName = "ActionMoveGroup";

        String dbName = dropDbInfo.dbName;
        boolean isDropIfExists = dropDbInfo.isDropIfExists;
        long socketTimeout = dropDbInfo.socketTimeout;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {

            DdlEngineTaskAccessor accessor = new DdlEngineTaskAccessor();
            accessor.setConnection(metaDbConn);
            if (!dropDbInfo.isAllowDropInScale() && accessor.hasOngoingTask(dbName, moveGroupTaskName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "Drop database not allowed when scale-in/out");
            }

            dropLogicalDbWithConn(dbName, isDropIfExists, metaDbConn, metaDbLockConn, socketTimeout);

            // release meta db lock
            LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);

        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    /**
     * Create physical db for group
     */
    public static void createPhysicalDbsIfNotExists(String masterInstId, String dbName, String charset,
                                                    String collation) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);

            List<GroupDetailInfoRecord> groupDetailInfoRecords =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(masterInstId, dbName);

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);

            /**
             * <pre>
             *     key: storageInstId
             *     val: the map of grp and phydb
             *         map key: groupName
             *         map val: phyDbName
             *
             * </pre>
             */
            Map<String, Map<String, String>> storageInstGroupInfoListMap = Maps.newHashMap();
            for (int i = 0; i < groupDetailInfoRecords.size(); i++) {
                GroupDetailInfoRecord groupDetailInfoRecord = groupDetailInfoRecords.get(i);
                DbGroupInfoRecord dbGroupInfoRecord =
                    dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(groupDetailInfoRecord.dbName,
                        groupDetailInfoRecord.groupName);
                String phyDbName = dbGroupInfoRecord.phyDbName;
                String storageInstId = groupDetailInfoRecord.storageInstId;
                Map<String, String> groupDetailAndPhyDbNameMap =
                    storageInstGroupInfoListMap.get(storageInstId);
                if (groupDetailAndPhyDbNameMap == null) {
                    groupDetailAndPhyDbNameMap = new HashMap<>();
                    storageInstGroupInfoListMap.put(storageInstId, groupDetailAndPhyDbNameMap);
                }
                groupDetailAndPhyDbNameMap.put(groupDetailInfoRecord.groupName, phyDbName);
            }

            for (Map.Entry<String, Map<String, String>> storagePhyDbInfoItem : storageInstGroupInfoListMap
                .entrySet()) {
                createPhysicalDbInStorageInst(charset, collation, storagePhyDbInfoItem.getKey(),
                    storagePhyDbInfoItem.getValue());
            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                String.format("Failed to create physical db, dbName=[%s], instId=[%s]", dbName, masterInstId));
        }
    }

    /**
     * create physical dbName on the special storage inst
     *
     * @param charset the charset of phy dbs
     * @param collation the collation of phy dbs
     * @param storageInstId the storage inst id of phy dbs
     * @param grpAndPhyDbMap the map of groupName and phyDbName
     */
    public static void createPhysicalDbInStorageInst(String charset, String collation,
                                                     String storageInstId,
                                                     Map<String, String> grpAndPhyDbMap)
        throws SQLException {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String storageConnProps = haSwitchParams.storageConnPoolConfig.connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(-1, storageConnProps);
        try (Connection targetGroupConn = GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps)) {
            // create phy db for each group
            for (Map.Entry<String, String> grpAndPhyDbItem : grpAndPhyDbMap.entrySet()) {
                String phyDbName = grpAndPhyDbItem.getValue();
                String createDbSql = buildCreatePhyDbSql(charset, collation, phyDbName);
                Statement stmt = targetGroupConn.createStatement();
                stmt.execute(createDbSql);
                stmt.close();
            }
        } catch (SQLException ex) {
            throw ex;
        }
    }

    protected static String buildCreatePhyDbSql(String charset, String collation, String phyDbName) {
        String createDbSql = String.format(CREATE_DB_IF_NOT_EXISTS_SQL_TEMPLATE, phyDbName);
        if (charset != null) {
            createDbSql += String.format(CHARSET_TEMPLATE, charset);
        } else if (collation != null) {
            createDbSql += String.format(COLLATION_TEMPLATE, collation);
        }
        return createDbSql;
    }

    /**
     * Create topology config for new db
     * Notice: this method must call after finishing ddl of create all phy dbs
     */
    public static void addDbTopologyConfig(CreateDbInfo createDbInfo) {

        String dbName = createDbInfo.dbName;
        int dbType = createDbInfo.dbType;
        Map<String, String> groupPhyDbMap = createDbInfo.groupPhyDbMap;
        Map<String, List<String>> storageInstGrpListMap = new HashMap<>();
        Map<String, List<String>> singleGroupMap = new HashMap<>();
        createDbInfo.groupLocator.buildGroupLocationInfo(storageInstGrpListMap, singleGroupMap);
        singleGroupMap.forEach((k, list) ->
            storageInstGrpListMap.computeIfAbsent(k, x -> Lists.newArrayList()).addAll(list)
        );

        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // This instId may be polardbx master instId or polardbx slave instId,
            // but only polardbx master instId allow create db
            String instId = InstIdUtil.getInstId();
            assert instId != null;

            // ----begin trx----
            conn.setAutoCommit(false);

            // ----check db if exists----
            // check if exists the config of target db
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(conn);
            DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
            if (dbInfoRecord != null) {
                if (dbType == DbInfoRecord.DB_TYPE_DEFAULT_DB || dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB
                    || dbType == DbInfoRecord.DB_TYPE_CDC_DB) {
                    return;
                } else {
                    int dbStatus = dbInfoRecord.dbStatus;
                    if (dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format("Create db error by metadb, db[%s] already exists ", dbName));
                    } else if (dbStatus == DbInfoRecord.DB_STATUS_CREATING) {
                        return;
                    } else if (dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                        // Call method to drop database
                        // To be impl
                    }
                }
            }

            // ----add storage_inst_config for new db----
            int storageKind = StorageInfoRecord.INST_KIND_MASTER;
            if (dbType == DbInfoRecord.DB_TYPE_DEFAULT_DB || dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB) {
                storageKind = StorageInfoRecord.INST_KIND_META_DB;
            }
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(conn);

            // ----add group_detail_info for new db----
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);

            // batch get all storage configs (they ary connPool configs) by instId
            for (Map.Entry<String, List<String>> entry : storageInstGrpListMap.entrySet()) {
                String storageInstId = entry.getKey();
                List<String> grpListOfInst = entry.getValue();

                List<Pair<String, String>> instIdAndStorageInstIdPair = new ArrayList<>();

                // Add the pair for server master instId and storage master instId
                instIdAndStorageInstIdPair.add(new Pair<>(instId, storageInstId));

                // Add the pairs for all read-only instId and storage read-only instId
                // Find all storage-slave instIds and its server-slave instId for the master storageInstId
                List<StorageInfoRecord> storageSlaveInfos =
                    storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(storageInstId);
                for (int j = 0; j < storageSlaveInfos.size(); j++) {
                    StorageInfoRecord storageSlaveInfo = storageSlaveInfos.get(j);
                    instIdAndStorageInstIdPair.add(new Pair<>(storageSlaveInfo.instId, storageSlaveInfo.storageInstId));
                    if (DbInfoManager.isNormalDbType(createDbInfo.dbType)) {
                        instIdAndStorageInstIdPair
                            .add(new Pair<>(storageSlaveInfo.instId, storageSlaveInfo.storageInstId));
                    } else {
                        instIdAndStorageInstIdPair.add(new Pair<>(storageSlaveInfo.instId, storageInstId));
                    }
                }

                for (int k = 0; k < instIdAndStorageInstIdPair.size(); k++) {
                    Pair<String, String> instAndStoragePair = instIdAndStorageInstIdPair.get(k);
                    String serverInstIdVal = instAndStoragePair.getKey();
                    String storageInstIdVal = instAndStoragePair.getValue();

                    // Add all groups that locate on the curr storage inst
                    for (int j = 0; j < grpListOfInst.size(); j++) {
                        String grpName = grpListOfInst.get(j);
                        // add group detail
                        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                        groupDetailInfoAccessor.setConnection(conn);
                        groupDetailInfoAccessor
                            .addNewGroupDetailInfo(serverInstIdVal, dbName, grpName, storageInstIdVal);
                    }
                }
            }

            // ----add db_group_info for new db----
            for (Map.Entry<String, String> grpItem : groupPhyDbMap.entrySet()) {
                String grpKey = grpItem.getKey();
                String phyDbName = grpItem.getValue();
                dbGroupInfoAccessor.addNewDbAndGroup(dbName, grpKey, phyDbName, DbGroupInfoRecord.GROUP_TYPE_NORMAL);
                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, grpKey);
                MetaDbConfigManager.getInstance().register(grpDataId, conn);
            }

            // ----add db_info for new db----
            String appName = AppNameUtil.buildAppNameByInstAndDbName(instId, dbName);
            dbInfoAccessor
                .addNewDb(dbName, appName, createDbInfo.dbType, DbInfoRecord.DB_STATUS_CREATING, createDbInfo.charset,
                    createDbInfo.collation);

            // ----- add schemata meta data -----
            if (!TStringUtil.equalsIgnoreCase(createDbInfo.dbName, SystemDbHelper.DEFAULT_DB_NAME)) {
                SchemataAccessor schemataAccessor = new SchemataAccessor();
                schemataAccessor.setConnection(conn);

                SchemataRecord schemataRecord = new SchemataRecord();
                schemataRecord.schemaName = createDbInfo.dbName;
                schemataRecord.defaultCharSetName = createDbInfo.charset;
                schemataRecord.defaultCollationName = createDbInfo.collation;
                schemataRecord.defaultDbIndex = createDbInfo.defaultDbIndex;
                schemataAccessor.insert(schemataRecord);
            }

            // ---- commit trx ----
            conn.commit();

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create db[%s], err is %s", dbName, ex.getMessage())
            );
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }
    }
/*
    protected static boolean addDbTopologyConfig(DbInfoRecord dbInfoRecord) {

        String dbName = dbInfoRecord.dbName;
        int dbType = dbInfoRecord.dbType;
        Map<String, Pair<String, String>> storageInstGrpAndDbMap;

        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // This instId may be polardbx master instId or polardbx slave instId,
            // but only polardbx master instId allow create db
            String instId = InstIdUtil.getInstId();
            assert instId != null;

            // ----begin trx----
            conn.setAutoCommit(false);

            // ----check db if exists----
            // check if exists the config of target db
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(conn);
            if (dbInfoRecord != null) {
                if (dbType == DbInfoRecord.DB_TYPE_DEFAULT_DB || dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB) {
                    return false;
                } else {
                    int dbStatus = dbInfoRecord.dbStatus;
                    if (dbStatus != DbInfoRecord.DB_STATUS_RUNNING) {
                        return false;
                    }
                }
            } else {
                return false;
            }

            // ----add storage_inst_config for new db----
            int storageKind = StorageInfoRecord.INST_KIND_MASTER;
            if (dbType == DbInfoRecord.DB_TYPE_DEFAULT_DB || dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB) {
                storageKind = StorageInfoRecord.INST_KIND_META_DB;
            }
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(conn);
            // batch get all storage configs (they ary connPool configs) by instId
            List<String> storageInstIdList =
                storageInfoAccessor.getStorageIdListByInstIdAndInstKind(instId, storageKind);

            // ----add group_detail_info for new db----
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);

            List<DbGroupInfoRecord> dbGroupInfoRecords = dbGroupInfoAccessor
                .getDbGroupInfoByDbNameAndGroupType(dbName,
                    DbGroupInfoRecord.GROUP_TYPE_FOR_PARTITION_TABLES, false);
            if (GeneralUtil.isEmpty(dbGroupInfoRecords) || dbGroupInfoRecords.size() < storageInstIdList.size()) {
                // ---- update db_status to creating db for partition table by trx  ----
                storageInstGrpAndDbMap =
                    generateDbAndGroupConfigInfoForPartitionTables(dbGroupInfoRecords, storageInstIdList,
                        dbName, conn);
                dbInfoAccessor.updateDbStatusByDbName(dbName,
                    DbInfoRecord.DB_STATUS_CREATING_FOR_PARTITION_TABLES);
            } else {
                return false;
            }

            for (Map.Entry<String, Pair<String, String>> storageInstGrpAndDb : storageInstGrpAndDbMap.entrySet()) {
                String storageInstId = storageInstGrpAndDb.getKey();
                String groupName = storageInstGrpAndDb.getValue().getKey();
                String physicalDbName = storageInstGrpAndDb.getValue().getValue();

                List<Pair<String, String>> instIdAndStorageInstIdPair = new ArrayList<>();

                // Add the pair for server master instId and storage master instId
                instIdAndStorageInstIdPair.add(new Pair<>(instId, storageInstId));

                // Add the pairs for all read-only instId and storage read-only instId
                // Find all storage-slave instIds and its server-slave instId for the master storageInstId
                List<StorageInfoRecord> storageSlaveInfos =
                    storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(storageInstId);
                for (int j = 0; j < storageSlaveInfos.size(); j++) {
                    StorageInfoRecord storageSlaveInfo = storageSlaveInfos.get(j);
                    instIdAndStorageInstIdPair.add(new Pair<>(storageSlaveInfo.instId, storageSlaveInfo.storageInstId));
                    instIdAndStorageInstIdPair
                        .add(new Pair<>(storageSlaveInfo.instId, storageSlaveInfo.storageInstId));
                }

                for (int k = 0; k < instIdAndStorageInstIdPair.size(); k++) {
                    Pair<String, String> instAndStoragePair = instIdAndStorageInstIdPair.get(k);
                    String serverInstIdVal = instAndStoragePair.getKey();
                    String storageInstIdVal = instAndStoragePair.getValue();

                    // add group detail
                    GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                    groupDetailInfoAccessor.setConnection(conn);
                    groupDetailInfoAccessor
                        .addNewGroupDetailInfo(serverInstIdVal, dbName, groupName, storageInstIdVal);
                }
                // add db_group_info
                dbGroupInfoAccessor
                    .addNewDbAndGroup(dbName, groupName, physicalDbName,
                        DbGroupInfoRecord.GROUP_TYPE_FOR_PARTITION_TABLES);
                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, groupName);
                MetaDbConfigManager.getInstance().register(grpDataId, conn);
            }

            // ---- commit trx ----
            conn.commit();

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create db[%s], err is %s", dbName, ex.getMessage())
            );
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }
        return storageInstGrpAndDbMap != null && storageInstGrpAndDbMap.size() > 0;
    }
*/

    /**
     * Drop physical db for group
     */
    protected static void dropPhysicalDbsIfExists(String masterInstId, String dbName, long socketTimeout) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);

            Map<String, List<GroupDetailInfoRecord>> storageInstGroupInfoListMap = Maps.newHashMap();
            List<GroupDetailInfoRecord> groupDetailInfoRecords =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(masterInstId, dbName);
            for (int i = 0; i < groupDetailInfoRecords.size(); i++) {
                GroupDetailInfoRecord groupDetailInfoRecord = groupDetailInfoRecords.get(i);
                String storageInstId = groupDetailInfoRecord.storageInstId;
                List<GroupDetailInfoRecord> groupDetailList = storageInstGroupInfoListMap.get(storageInstId);
                if (groupDetailList == null) {
                    groupDetailList = new ArrayList<>();
                    storageInstGroupInfoListMap.put(storageInstId, groupDetailList);
                }
                groupDetailList.add(groupDetailInfoRecord);
            }

            for (Map.Entry<String, List<GroupDetailInfoRecord>> groupDetailListItem : storageInstGroupInfoListMap
                .entrySet()) {

                String storageInstId = groupDetailListItem.getKey();
                List<GroupDetailInfoRecord> grpList = groupDetailListItem.getValue();
                Map<String, String> grpPhyDbMap = new HashMap<>();
                // drop phy db for each group
                for (int i = 0; i < grpList.size(); i++) {
                    GroupDetailInfoRecord grpInfo = grpList.get(i);
                    DbGroupInfoRecord dbGroupInfoRecord =
                        dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(grpInfo.dbName, grpInfo.groupName);
                    String phyDbName = dbGroupInfoRecord.phyDbName;
                    grpPhyDbMap.put(grpInfo.groupName, phyDbName);
                }
                dropPhysicalDbsInStorageInst(storageInstId, grpPhyDbMap, socketTimeout);

            }
        } catch (Throwable e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, e,
                String.format("Failed to drop physical db, dbName=[%s], instId=[%s]", dbName, masterInstId));
        }
    }

    protected static void renameOnePhysicalDbInStorageInst(String storageInstId, String srcPhyDbName,
                                                           long socketTimeout)
        throws SQLException {

        // ----prepare conn of target inst----
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String storageConnProps = haSwitchParams.storageConnPoolConfig.connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(socketTimeout, storageConnProps);

        try (Connection targetGroupConn = GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps)) {
            // rename phy db
            renamePhyDbByDbName(srcPhyDbName, targetGroupConn);
        } catch (SQLException ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    protected static void renamePhyDbByDbName(String srcPhyDbName, Connection conn) throws SQLException {

        String showTableFromPhyDb = String.format("SHOW TABLES FROM `%s`", srcPhyDbName);
        String dropSrcPhyDbSql = String.format("DROP DATABASE IF EXISTS `%s`", srcPhyDbName);

        String tarPhyDbName = getTimeBasePostFixString(srcPhyDbName);
        String createTarPhyDbSql = String.format("CREATE DATABASE IF NOT EXISTS `%s`", tarPhyDbName);

        Statement stmt = conn.createStatement();
        stmt.execute(showTableFromPhyDb);
        ResultSet rs = stmt.getResultSet();
        List<String> phyTbList = new ArrayList<>();
        while (rs.next()) {
            String tmpTb = rs.getString(1);
            phyTbList.add(tmpTb);
        }
        rs.close();
        stmt.close();

        // create new renamed phy db
        Statement createDbStmt = conn.createStatement();
        createDbStmt.execute(createTarPhyDbSql);
        createDbStmt.close();

        // rename all the phy tb of src phy db to new renamed phy db
        List<String> phyTbListOfSrcDb = new ArrayList<>();
        List<String> phyTbListOfTarDb = new ArrayList<>();
        int allPhyTbCnt = phyTbList.size();
        int renamePhyTbBatchSize = DbTopologyManager.DEFAULT_RENAME_PHY_DB_BATCH_SIZE;
        int batchCnt =
            Double.valueOf(Math.ceil(Double.valueOf(allPhyTbCnt) / Double.valueOf(renamePhyTbBatchSize))).intValue();
        int curTbIdx = 0;
        int curTbCntInOneBatch = 0;
        List<String> phyTbListInOneBatch = new ArrayList<>();
        for (int i = 0; i < batchCnt; i++) {
            phyTbListOfSrcDb.clear();
            phyTbListOfTarDb.clear();
            phyTbListInOneBatch.clear();
            curTbCntInOneBatch = 0;
            while (curTbIdx < allPhyTbCnt) {
                String tbName = phyTbList.get(curTbIdx);
                String compTbForSrc = String.format("`%s`.`%s`", srcPhyDbName, tbName);
                String compTbForTar = String.format("`%s`.`%s`", tarPhyDbName, tbName);
                phyTbListOfSrcDb.add(compTbForSrc);
                phyTbListOfTarDb.add(compTbForTar);
                phyTbListInOneBatch.add(tbName);
                curTbIdx++;
                curTbCntInOneBatch++;
                if (curTbCntInOneBatch >= renamePhyTbBatchSize) {
                    break;
                }
            }
            renamePhyTbByBatch(conn, phyTbListInOneBatch, phyTbListOfSrcDb, phyTbListOfTarDb);
        }

        // drop src phy db
        Statement dropDbStmt = conn.createStatement();
        dropDbStmt.execute(dropSrcPhyDbSql);
        dropDbStmt.close();

    }

    private static void renamePhyTbByBatch(Connection conn, List<String> phyTbList,
                                           List<String> phyTbListOfSrcDb, List<String> phyTbListOfTarDb)
        throws SQLException {
        StringBuilder renameContent = new StringBuilder("");
        for (int i = 0; i < phyTbList.size(); i++) {
            String compTbForSrc = phyTbListOfSrcDb.get(i);
            String compTbForTar = phyTbListOfTarDb.get(i);
            if (i > 0) {
                renameContent.append(",");
            }
            renameContent.append(String.format("%s TO %s", compTbForSrc, compTbForTar));

        }
        String renameTbSql = String.format("RENAME TABLE %s", renameContent);
        if (phyTbList.size() > 0) {
            Statement renameDbStmt = conn.createStatement();
            renameDbStmt.execute(renameTbSql);
            renameDbStmt.close();
        }
    }

    protected static String getTimeBasePostFixString(String name) {
        Date now = new Date();
        String dateStr = new SimpleDateFormat("yyyyMMddHHmmss").format(now);
        if (name == null) {
            return dateStr;
        } else {
            return name + "_" + dateStr;
        }
    }

    protected static void dropPhysicalDbsInStorageInst(String storageInstId,
                                                       Map<String, String> grpPhyDbMap, long socketTimeout)
        throws SQLException {

        // ----prepare conn of target inst----
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String storageConnProps = haSwitchParams.storageConnPoolConfig.connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(socketTimeout, storageConnProps);
        try (Connection targetGroupConn = GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps)) {
            // drop phy db for each group
            for (Map.Entry<String, String> grpPhyDbItem : grpPhyDbMap.entrySet()) {
                String phyDbName = grpPhyDbItem.getValue();
                String dropDbSql = String.format(DROP_DB_IF_EXISTS_SQL_TEMPLATE, phyDbName);
                Statement stmt = targetGroupConn.createStatement();
                stmt.execute(dropDbSql);
                stmt.close();
            }
        } catch (SQLException ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    private static String getJdbcConnPropsFromAtomConnPropsForGroup(long socketTimeout, String connProps) {
        Map<String, String> connPropsMap = GmsJdbcUtil.getPropertiesMapFromAtomConnProps(connProps);
        String socketTimeoutStrOfConn = connPropsMap.get("socketTimeout");
        long socketTimeoutValOfConn = socketTimeoutStrOfConn != null ? Long.valueOf(socketTimeoutStrOfConn) : -1;
        if (socketTimeout >= 0) {
            socketTimeoutValOfConn = socketTimeout;
        }
        connProps = GmsJdbcUtil
            .getJdbcConnPropsFromPropertiesMap(GmsJdbcUtil.getDefaultConnPropertiesForGroup(socketTimeoutValOfConn));
        return connProps;
    }

    /**
     * remove all the topology config for db
     * Notice: this method must call after finishing ddl of drop all phy dbs
     */
    public static void removeDbTopologyConfig(String dbName, Connection conn) {

        try {

            // check if db exists
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(conn);
            DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
            if (dbInfoRecord == null) {
                return;
            }
            String appName = dbInfoRecord.appName;

            // clear group_detail_info
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(conn);

            // unregister all dataIds of groups in the db
            List<GroupDetailInfoRecord> groupDetailInfoRecords =
                groupDetailInfoAccessor.getGroupDetailInfoListByDbName(dbName);
            for (int i = 0; i < groupDetailInfoRecords.size(); i++) {
                GroupDetailInfoRecord groupDetailInfoRecord = groupDetailInfoRecords.get(i);
                String instId = groupDetailInfoRecord.instId;
                String grpName = groupDetailInfoRecord.groupName;
                String grpDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, grpName);
                MetaDbConfigManager.getInstance().unbindListener(grpDataId);
                MetaDbConfigManager.getInstance().unregister(grpDataId, conn);
            }

            String scaleOutDataId = MetaDbDataIdBuilder.getDbComplexTaskDataId(dbName);
            MetaDbConfigManager.getInstance().unbindListener(scaleOutDataId);
            MetaDbConfigManager.getInstance().unregister(scaleOutDataId, conn);

            // clear all group detail info
            groupDetailInfoAccessor.deleteGroupDetailInfoByDbName(dbName);

            // clear db_group_info
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);
            dbGroupInfoAccessor.deleteDbGroupInfoByDbName(dbName);

            // clear db_info
            dbInfoAccessor.deleteDbInfoByDbName(dbName);

            // Remove db info from schemata
            SchemataAccessor schemataAccessor = new SchemataAccessor();
            schemataAccessor.setConnection(conn);
            schemataAccessor.delete(dbName);

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to drop db[%s], err is %s", dbName, ex.getMessage())
            );
        }
    }

    public static List<GroupDetailInfoRecord> getGroupDetails(String dbName, String storageInstId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(metaDbConn);
            return accessor.getGroupDetailInfoByDbNameAndStorageInstId(dbName, storageInstId);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static CreateDbInfo initCreateDbInfo(String dbName, String charset, String collate,
                                                String locality,
                                                Predicate<StorageInfoRecord> localityFilter,
                                                int dbType,
                                                boolean isCreateIfNotExists,
                                                long socketTimeout,
                                                int shardDbCountEachStorageInstOfStmt) {

        int shardDbCountEachStorage = DbTopologyManager.shardDbCountEachStorageInst;
        if (shardDbCountEachStorageInstOfStmt > 0) {
            shardDbCountEachStorage = shardDbCountEachStorageInstOfStmt;
        }
        Map<String, String> grpAndPhyDbNameMap = Maps.newHashMap();
        CreateDbInfo createDbInfo = new CreateDbInfo();

        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            storageInfoAccessor.setConnection(metaDbConn);
            /* Filter storage instance by locality */
            List<StorageInfoRecord> storageList =
                storageInfoAccessor.getStorageInfosByInstIdAndKind(
                    InstIdUtil.getInstId(),
                    StorageInfoRecord.INST_KIND_MASTER);
            List<String> storageIdList = storageList.stream()
                .filter(x -> localityFilter == null || localityFilter.test(x))
                .map(StorageInfoRecord::getInstanceId)
                .distinct()
                .collect(Collectors.toList());

            if (storageIdList.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "No storage match the locality " + locality);
            }
            createDbInfo.setStorageInstList(storageIdList);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        int storageInstCount = createDbInfo.getStorageInstList().size();
        int shardDbCount = shardDbCountEachStorage * storageInstCount;

        createDbInfo.dbName = dbName;
        createDbInfo.charset = charset;
        createDbInfo.collation = collate;
        createDbInfo.dbType = dbType;

        String grpNameSingleGroup = "";
        if (createDbInfo.dbType == DbInfoRecord.DB_TYPE_PART_DB || createDbInfo.dbType == DbInfoRecord.DB_TYPE_CDC_DB) {
            grpNameSingleGroup = GroupInfoUtil.buildGroupName(dbName, -1, false);
            String phyDbNameSingle = GroupInfoUtil.buildPhyDbName(dbName, -1, false);
            grpAndPhyDbNameMap.put(grpNameSingleGroup, phyDbNameSingle);
            for (int i = 0; i < shardDbCount; i++) {
                String grpName = GroupInfoUtil.buildGroupName(dbName, i, false);
                String phyDbName = GroupInfoUtil.buildPhyDbName(dbName, i, false);
                grpAndPhyDbNameMap.put(grpName, phyDbName);
            }
        } else if (createDbInfo.dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB) {
            for (int i = 0; i < storageInstCount; i++) {
                String grpName = GroupInfoUtil.buildGroupName(dbName, i, true);
                String phyDbName = GroupInfoUtil.buildPhyDbName(dbName, i, true);
                grpAndPhyDbNameMap.put(grpName, phyDbName);
                if (StringUtils.isEmpty(grpNameSingleGroup)) {
                    grpNameSingleGroup = grpName;
                }
            }
        }

        createDbInfo.locality = locality;
        createDbInfo.groupPhyDbMap = grpAndPhyDbNameMap;
        createDbInfo.singleGroup = grpNameSingleGroup;
        createDbInfo.defaultDbIndex = grpNameSingleGroup;
        createDbInfo.groupLocator =
            new DefaultGroupLocator(createDbInfo.groupPhyDbMap, createDbInfo.storageInstList,
                DbTopologyManager.singleGroupStorageInstList);

        createDbInfo.isCreateIfNotExists = isCreateIfNotExists;
        createDbInfo.socketTimeout = socketTimeout;
        createDbInfo.shardDbCountEachInst = shardDbCountEachStorage;
        return createDbInfo;
    }

    public static String getStorageInstIdByGroupName(String dbName, String groupName) {
        return getStorageInstIdByGroupName(InstIdUtil.getInstId(), dbName, groupName);
    }

    // TODO why not cache it?
    public static String getStorageInstIdByGroupName(String instId, String dbName, String groupName) {

        String storageInstId = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            // Get storage info by group name
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            GroupDetailInfoRecord groupDetail =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, dbName, groupName);
            if (groupDetail != null) {
                storageInstId = groupDetail.storageInstId;
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return storageInstId;
    }

    /**
     * Add configs of new group into logical db
     *
     * @param dbName the dbName of logical db
     * @param storageInstId the storage_inst_id of new groups
     * @param newGroupName the new groupName to be added
     * @param newPhyDbName the new phyDbName of new group to be added
     * @param ifNotExists while skip the create phase when group exists
     */
    public static boolean addScaleOutGroupIntoDb(String dbName, String storageInstId,
                                                 String newGroupName, String newPhyDbName,
                                                 boolean ifNotExists) {

        String instId = InstIdUtil.getInstId();
        String dbCharset = null;
        String dbCollation = null;

        // create and add config into db topology
        // set group type to adding
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
            dbCharset = dbInfoRecord.charset;
            dbCollation = dbInfoRecord.collation;

            metaDbConn.setAutoCommit(false);
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);

            String grpKey = newGroupName;
            String phyDbName = newPhyDbName;

            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, grpKey);
            if (dbGroupInfoRecord != null) {

                switch (dbGroupInfoRecord.groupType) {

                case DbGroupInfoRecord.GROUP_TYPE_NORMAL: {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed add new group[%s] because the group has already exists", grpKey));
                }

                case DbGroupInfoRecord.GROUP_TYPE_ADDED: {
                    if (!ifNotExists) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String
                                .format("Failed add new group[%s] because the scaleout group has already exists",
                                    grpKey));
                    } else {
                        break;
                    }
                }

                case DbGroupInfoRecord.GROUP_TYPE_ADDING: {
                    // Ignore to update db_type to adding
                    break;
                }

                case DbGroupInfoRecord.GROUP_TYPE_REMOVING: {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed add new group[%s] because the group is removing", grpKey));
                }

                case DbGroupInfoRecord.GROUP_TYPE_SCALEOUT_FINISHED: {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed add new group[%s] because the scalout finished group has already exists",
                            grpKey));
                }

                }
            } else {
                // No find any info for new group, then go to adding
                dbGroupInfoAccessor
                    .addNewDbAndGroup(dbName, grpKey, phyDbName, DbGroupInfoRecord.GROUP_TYPE_ADDING);
                groupDetailInfoAccessor.addNewGroupDetailInfo(instId, dbName, grpKey, storageInstId);
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(metaDbConn);
                List<StorageInfoRecord> slaveStorageInfoRecords =
                    storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(storageInstId);
                for (int i = 0; i < slaveStorageInfoRecords.size(); i++) {
                    StorageInfoRecord slaveStorageInfo = slaveStorageInfoRecords.get(i);
                    String readOnlyInstId = slaveStorageInfo.instId;
                    String slaveStorageInstId = slaveStorageInfo.storageInstId;
                    groupDetailInfoAccessor.addNewGroupDetailInfo(readOnlyInstId, dbName, grpKey, slaveStorageInstId);
                }

            }
            metaDbConn.commit();

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // create phy database
        try {
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.putIfAbsent(newGroupName, newPhyDbName);
            createPhysicalDbInStorageInst(dbCharset, dbCollation, storageInstId,
                grpPhyDbMap);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // update group type to scale_out
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            metaDbConn.setAutoCommit(false);
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);
            String grpKey = newGroupName;
            dbGroupInfoAccessor.updateGroupTypeByDbAndGroup(dbName, grpKey, DbGroupInfoRecord.GROUP_TYPE_ADDED);
            MetaDbConfigManager.getInstance()
                .register(MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, grpKey), metaDbConn);
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);
            metaDbConn.commit();
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // sync db topology
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(dbName));

        // return success
        return true;
    }

    public static void addNewGroupIntoDb(String dbName, String storageInstId,
                                         String newGroupName, String newPhyDbName,
                                         boolean ifNotExists,
                                         Connection metaDbConn) {

        String instId = InstIdUtil.getInstId();
        String dbCharset = null;
        String dbCollation = null;

        // create and add config into db topology
        // set group type to adding

        DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
        dbInfoAccessor.setConnection(metaDbConn);
        DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
        dbCharset = dbInfoRecord.charset;
        dbCollation = dbInfoRecord.collation;

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConn);

        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);

        String grpKey = newGroupName;
        String phyDbName = newPhyDbName;

        try {
            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, grpKey);
            if (dbGroupInfoRecord != null) {
                if (dbGroupInfoRecord.groupType == DbGroupInfoRecord.GROUP_TYPE_ADDED && !ifNotExists) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed add new group[%s] because the group has already exists", grpKey));
                }
            } else {
                // No find any info for new group, then go to adding
                dbGroupInfoAccessor
                    .addNewDbAndGroup(dbName, grpKey, phyDbName, DbGroupInfoRecord.GROUP_TYPE_ADDED);
                groupDetailInfoAccessor.addNewGroupDetailInfo(instId, dbName, grpKey, storageInstId);
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(metaDbConn);
                List<StorageInfoRecord> slaveStorageInfoRecords =
                    storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(storageInstId);
                for (int i = 0; i < slaveStorageInfoRecords.size(); i++) {
                    StorageInfoRecord slaveStorageInfo = slaveStorageInfoRecords.get(i);
                    String readOnlyInstId = slaveStorageInfo.instId;
                    String slaveStorageInstId = slaveStorageInfo.storageInstId;
                    groupDetailInfoAccessor.addNewGroupDetailInfo(readOnlyInstId, dbName, grpKey, slaveStorageInstId);
                }
                MetaDbConfigManager.getInstance()
                    .register(MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, grpKey), metaDbConn);

                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        // create phy database
        try {
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.putIfAbsent(newGroupName, newPhyDbName);
            createPhysicalDbInStorageInst(dbCharset, dbCollation, storageInstId,
                grpPhyDbMap);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static void removeGroupByName(String schema, String groupName, Connection metaDbConn) {
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);
        GroupDetailInfoRecord detailRecord =
            groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(InstIdUtil.getInstId(), schema, groupName);

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConn);
        DbGroupInfoRecord groupRecord = dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(schema, groupName);

        removeNewGroupIntoDb(schema, detailRecord.getStorageInstId(), groupName, groupRecord.phyDbName, -1, metaDbConn);
    }

    public static void removeNewGroupIntoDb(String dbName,
                                            String storageInstId,
                                            String newGroupName,
                                            String newPhyDbName,
                                            long socketTimeout,
                                            Connection metaDbConn) {

        String instId = InstIdUtil.getInstId();

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConn);

        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);

        String grpKey = newGroupName;

        try {
            GroupDetailInfoRecord groupDetailInfoRecord =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, dbName, grpKey);
            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, grpKey);
            if (dbGroupInfoRecord != null) {
                // remove db_group_info and group_detail_info from db for target groups
                dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, newGroupName);
                groupDetailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, newGroupName);

                String instIdOfGroup = groupDetailInfoRecord.instId;
                int groupType = dbGroupInfoRecord.groupType;

                if (groupType == DbGroupInfoRecord.GROUP_TYPE_NORMAL) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String
                            .format("Failed to remove the group[%s] because the group is not scale-out group", grpKey));
                }

                String dataIdOfGroup = MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, dbName, newGroupName);
                MetaDbConfigManager.getInstance().unregister(dataIdOfGroup, metaDbConn);

                // upgrade op version  and notify other server nodes to reload topology by timer task
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);
            }

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        try {
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.put(newGroupName, newPhyDbName);
            dropPhysicalDbsInStorageInst(storageInstId, grpPhyDbMap, socketTimeout);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    /*
     * @param dropDatabaseAfterSwitch when true drop database directly after switch, else rename it
     */
    public static void removeScaleOutGroupFromDb(String dbName, String groupName, boolean allowRemoveScaleoutGroup,
                                                 boolean ifExists, long socketTimeout,
                                                 boolean dropDatabaseAfterSwitch) {

        String instId = InstIdUtil.getInstId();
        /**
         * key: storageInstId
         * val: Map of grp and phy dbInfo
         *        key: grpKey
         *        val: Pair of phyDbName and instIdOfGroup
         */
        Map<String, Map<String, Pair<String, String>>> groupStorageInstPhyDbNameMap = new HashMap<>();

        String instIdOfGroup = null;
        String storageInstId = null;
        String phyDbName = null;
        int groupType = -1;

        boolean isSuccess = false;
        // update group type to removing
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                GroupDetailInfoAccessor detailInfoAccessor = new GroupDetailInfoAccessor();
                detailInfoAccessor.setConnection(metaDbConn);

                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);

                String grpKey = groupName;
                metaDbConn.setAutoCommit(false);
                GroupDetailInfoRecord groupDetailInfoRecord =
                    detailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, dbName, grpKey);
                DbGroupInfoRecord dbGroupInfoRecord =
                    dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, grpKey);
                if (dbGroupInfoRecord == null) {
                    if (ifExists) {
                        return;
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed to drop group[%s] because the group doest NOT exists", grpKey));
                }
                if (groupDetailInfoRecord == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed to drop group[%s] because the group detail doest NOT exists", grpKey));
                }
                instIdOfGroup = groupDetailInfoRecord.instId;
                storageInstId = groupDetailInfoRecord.storageInstId;
                phyDbName = dbGroupInfoRecord.phyDbName;
                groupType = dbGroupInfoRecord.groupType;

                switch (groupType) {
                case DbGroupInfoRecord.GROUP_TYPE_NORMAL: {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String
                            .format("Failed to remove the group[%s] because the group is not scale-out group", grpKey));
                }

                case DbGroupInfoRecord.GROUP_TYPE_ADDED: {
                    if (!allowRemoveScaleoutGroup) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format("Failed to remove the group[%s] because the group is doing scale-out",
                                grpKey));
                    } else {
                        Map<String, Pair<String, String>> grpPhyDbMap = groupStorageInstPhyDbNameMap.get(storageInstId);
                        if (grpPhyDbMap == null) {
                            grpPhyDbMap = new HashMap<String, Pair<String, String>>();
                            groupStorageInstPhyDbNameMap.put(storageInstId, grpPhyDbMap);
                        }
                        grpPhyDbMap.put(grpKey, new Pair<>(phyDbName, instIdOfGroup));
                        dbGroupInfoAccessor
                            .updateGroupTypeByDbAndGroup(dbName, grpKey, DbGroupInfoRecord.GROUP_TYPE_REMOVING);
                        break;
                    }

                }

                case DbGroupInfoRecord.GROUP_TYPE_ADDING: {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed to remove the group[%s] because the group is adding", grpKey));
                }

                case DbGroupInfoRecord.GROUP_TYPE_REMOVING: {
                    // Ignore to update db_type to removing
                    break;
                }

                case DbGroupInfoRecord.GROUP_TYPE_SCALEOUT_FINISHED: {

                    Map<String, Pair<String, String>> grpPhyDbMap = groupStorageInstPhyDbNameMap.get(storageInstId);
                    if (grpPhyDbMap == null) {
                        grpPhyDbMap = new HashMap<String, Pair<String, String>>();
                        groupStorageInstPhyDbNameMap.put(storageInstId, grpPhyDbMap);
                    }
                    grpPhyDbMap.put(grpKey, new Pair<>(phyDbName, instIdOfGroup));
                    dbGroupInfoAccessor
                        .updateGroupTypeByDbAndGroup(dbName, grpKey, DbGroupInfoRecord.GROUP_TYPE_REMOVING);
                    break;
                }

                }

                // update dataId of group to REMOVED status
                String dataIdOfGroup = MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, dbName, groupName);
                MetaDbConfigManager.getInstance().unregister(dataIdOfGroup, metaDbConn);

                // upgrade op version  and notify other server nodes to reload topology by timer task
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);

                metaDbConn.commit();
                isSuccess = true;
            } finally {
                if (!isSuccess) {
                    metaDbConn.rollback();
                }
            }

            // sync others host to refresh db topology before removing scale out tmp group
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(dbName));

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        // rename phy database of scale out tmp group
        try {
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.put(groupName, phyDbName);
            if (dropDatabaseAfterSwitch) {
                dropPhysicalDbsInStorageInst(storageInstId, grpPhyDbMap, socketTimeout);
            } else {
                renameOnePhysicalDbInStorageInst(storageInstId, phyDbName, socketTimeout);
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                isSuccess = false;
                metaDbConn.setAutoCommit(false);

                // remove group configs from db topology
                GroupDetailInfoAccessor detailInfoAccessor = new GroupDetailInfoAccessor();
                detailInfoAccessor.setConnection(metaDbConn);
                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);

                // remove db_group_info and group_detail_info from db for target groups
                dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, groupName);
                detailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, groupName);

                // commit
                metaDbConn.commit();
                isSuccess = true;
            } finally {
                if (!isSuccess) {
                    metaDbConn.rollback();
                }
            }

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    /**
     * Switch storage insto for groups
     *
     * @param schemaName the schemaName of group
     * @param sourceGroupKey the src group key
     * @param srcOriginalStorageInstId the original storage inst id of the src group key
     * @param targetGroupKey the target group key
     * @param targetOriginalStorageInstId the original storage inst id of the target group key
     */
    public static void switchGroupStorageInfos(String schemaName,
                                               String sourceGroupKey,
                                               String srcOriginalStorageInstId,
                                               String targetGroupKey,
                                               String targetOriginalStorageInstId) {

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            metaDbConn.setAutoCommit(false);
            switchGroupStorageInfos(schemaName, sourceGroupKey, srcOriginalStorageInstId, targetGroupKey,
                targetOriginalStorageInstId, metaDbConn);
            metaDbConn.commit();

            // sync other server node of polardbx master inst
            String srcGrpDataId =
                MetaDbDataIdBuilder.getGroupConfigDataId(InstIdUtil.getInstId(), schemaName, sourceGroupKey);
            String targetGrpDataId =
                MetaDbDataIdBuilder.getGroupConfigDataId(InstIdUtil.getInstId(), schemaName, targetGroupKey);
            MetaDbConfigManager.getInstance().sync(srcGrpDataId);
            MetaDbConfigManager.getInstance().sync(targetGrpDataId);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

    }

    public static void switchGroupStorageInfos(String schemaName,
                                               String sourceGroupKey,
                                               String srcOriginalStorageInstId,
                                               String targetGroupKey,
                                               String targetOriginalStorageInstId,
                                               Connection metaDbConn) {

        // Make sure that instId must be the master inst of polardbx
        String instId = InstIdUtil.getInstId();

        // Get storage info from target group
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);

        DbGroupInfoAccessor dbGroupAccessor = new DbGroupInfoAccessor();
        dbGroupAccessor.setConnection(metaDbConn);

        // Find the src group detail info of polardbx master inst
        GroupDetailInfoRecord srcGroupDetail =
            groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, schemaName, sourceGroupKey);

        // Find the target group detail info of polardbx master inst
        GroupDetailInfoRecord targetGroupDetail =
            groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, schemaName, targetGroupKey);

        if (srcGroupDetail == null || targetGroupDetail == null) {
            return;
        }

        String srcGrpName = srcGroupDetail.groupName;
        String targetGrpName = targetGroupDetail.groupName;

        boolean needToUpdateMetaDb = true;
        if (srcGroupDetail.storageInstId.equalsIgnoreCase(targetOriginalStorageInstId)
            && targetGroupDetail.storageInstId.equalsIgnoreCase(srcOriginalStorageInstId)) {
            // no need to switch
            needToUpdateMetaDb = false;
        }

        // Find all polardbx inst_id (included polardbx master inst and polardbx slave inst)
        List<String> allInstIdListOfGroup =
            groupDetailInfoAccessor.getDistinctInstIdListByDbNameAndGroupName(schemaName, srcGrpName);

        if (needToUpdateMetaDb) {
            /**
             * The src groupKey and target groupKey is doing switching at first time, so need to update the metadb.
             */
            // switch the phyDbName of src group and target group for db_group_info table
            DbGroupInfoRecord srcDbGrpInfo =
                dbGroupAccessor.getDbGroupInfoByDbNameAndGroupName(schemaName, srcGrpName, true);
            DbGroupInfoRecord targetDbGrpInfo =
                dbGroupAccessor.getDbGroupInfoByDbNameAndGroupName(schemaName, targetGrpName, true);
            String srcPhyDbName = srcDbGrpInfo.phyDbName;
            String targetPhyDbName = targetDbGrpInfo.phyDbName;
            if (!srcPhyDbName.equalsIgnoreCase(targetPhyDbName)) {
                dbGroupAccessor.updatePhyDbNameByDbAndGroup(schemaName, srcGrpName, targetPhyDbName);
                dbGroupAccessor.updatePhyDbNameByDbAndGroup(schemaName, targetGrpName, srcPhyDbName);
            }
            for (int i = 0; i < allInstIdListOfGroup.size(); i++) {
                String instIdOfGroup = allInstIdListOfGroup.get(i);

                // Find the src group detail info of polardbx master inst
                srcGroupDetail =
                    groupDetailInfoAccessor
                        .getGroupDetailInfoByInstIdAndGroupName(instIdOfGroup, schemaName, sourceGroupKey);

                // Find the target group detail info of polardbx master inst
                targetGroupDetail =
                    groupDetailInfoAccessor
                        .getGroupDetailInfoByInstIdAndGroupName(instIdOfGroup, schemaName, targetGroupKey);

                // Find the storage_inst_id for src/target group of one polardbx inst (maybe master inst or slave inst)
                String srcStorageInstId = srcGroupDetail.storageInstId;
                String targetStorageInstId = targetGroupDetail.storageInstId;

                // switch the storage_inst_id between src group and target group
                groupDetailInfoAccessor
                    .updateStorageInstId(instIdOfGroup, schemaName, srcGrpName, targetStorageInstId);
                groupDetailInfoAccessor
                    .updateStorageInstId(instIdOfGroup, schemaName, targetGrpName, srcStorageInstId);
                String srcGrpDataId =
                    MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, schemaName, srcGrpName);
                String targetGrpDataId =
                    MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, schemaName, targetGrpName);

                // Notify other nodes of other polardbx inst to reload group configs
                MetaDbConfigManager.getInstance().notify(srcGrpDataId, metaDbConn);
                MetaDbConfigManager.getInstance().notify(targetGrpDataId, metaDbConn);
            }
        } else {

            /**
             * The src groupKey and target groupKey is doing switching at second time (such as retry),
             * so does NOT need to update the metadb, just upgrade their opVersions and do sync again.
             */
            for (int i = 0; i < allInstIdListOfGroup.size(); i++) {
                // Upgrade the opVersion
                String instIdOfGroup = allInstIdListOfGroup.get(i);
                String srcGrpDataId =
                    MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, schemaName, srcGrpName);
                String targetGrpDataId =
                    MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, schemaName, targetGrpName);

                // Notify other nodes of other polardbx inst to reload group configs
                MetaDbConfigManager.getInstance().notify(srcGrpDataId, metaDbConn);
                MetaDbConfigManager.getInstance().notify(targetGrpDataId, metaDbConn);
            }
        }

    }

    public static String getPhysicalDbNameByGroupKey(String dbName, String groupName) {

        if (SystemDbHelper.INFO_SCHEMA_DB_NAME.equalsIgnoreCase(dbName)
            && SystemDbHelper.INFO_SCHEMA_DB_GROUP_NAME.equalsIgnoreCase(groupName)) {
            return MetaDbDataSource.getInstance().getMetaDbName();
        }

        String phyDbName = null;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(metaDbConn);
            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, groupName);
            if (dbGroupInfoRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("No found any physical db info for [%s/%s]", dbName, groupName));
            }
            phyDbName = dbGroupInfoRecord.phyDbName;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
        return phyDbName;
    }

    public static void initGroupConfigsForReadOnlyInstIfNeed() {

        String instId = ServerInstIdManager.getInstance().getInstId();
        String masterInstId = ServerInstIdManager.getInstance().getMasterInstId();

        if (ServerInstIdManager.getInstance().isMasterInst()) {
            // if curr inst is master mode, then ignore to init topology configs for ro inst
            return;
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            metaDbConn.setAutoCommit(false);

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            List<Pair<String, String>> storageRoInfoList =
                storageInfoAccessor.getReadOnlyStorageInfoListByInstIdAndInstKind(instId);
            List<String> storageMetaDbInstIdList = storageInfoAccessor
                .getStorageIdListByInstIdAndInstKind(masterInstId, StorageInfoRecord.INST_KIND_META_DB);
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            if (storageRoInfoList.size() == 0) {
                return;
            }
            for (int i = 0; i < storageRoInfoList.size(); i++) {
                Pair<String, String> roAndRwPair = storageRoInfoList.get(i);
                String roInstId = roAndRwPair.getKey();
                String rwInstId = roAndRwPair.getValue();
                groupDetailInfoAccessor.addNewGroupDetailInfosForRoInst(instId, rwInstId,
                    roInstId, false);
            }
            for (int i = 0; i < storageMetaDbInstIdList.size(); i++) {
                groupDetailInfoAccessor.addNewGroupDetailInfosForRoInst(instId, storageMetaDbInstIdList.get(i),
                    storageMetaDbInstIdList.get(i), true);
            }
            metaDbConn.commit();
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

    }

    /**
     * add/create one physical database per storage instance for each logical database
     * this function will be executed by the polardb-x which is upgrade to partition management
     * feature supported version
     */
    /*
    public static void createPhysicalPartitionDbsForPerLogicalDb(DbInfoRecord dbInfo) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {

            String logicalDatabaseName = dbInfo.dbName;
            // acquire MetaDb Lock by for update, to avoiding concurrent create & drop databases
            metaDbLockConn.setAutoCommit(false);
            try {
                LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
            } catch (Throwable ex) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                    String.format("Get metaDb lock timeout during creating db[%s], please retry", logicalDatabaseName));
            }

            // ---- check if logical db exists ----
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            if (dbInfo != null && dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                // ---- update db_status to creating db for partition table by trx  ----
                boolean needCreatePhysicaDb = addDbTopologyConfig(dbInfo);
                if (!needCreatePhysicaDb) {
                    return;
                }
            } else if (dbInfo != null && dbInfo.dbStatus == DbInfoRecord.DB_STATUS_CREATING_FOR_PARTITION_TABLES) {
                // do nothing
            } else {
                return;
            }

            // ---- get lock by db_name for double check ----
            metaDbConn.setAutoCommit(false);
            DbInfoRecord dbInfoForUpdate = dbInfoAccessor.getDbInfoByDbNameForUpdate(logicalDatabaseName);
            if (DbInfoManager.isNormalDbType(dbInfoForUpdate.dbType)
                && dbInfoForUpdate.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Create db error, db[%s] has already exist", logicalDatabaseName));
            }

            // ---- create physical dbs by topology info loading from metadb ----
            String instId = InstIdUtil.getInstId();// make sure that instId is master inst id
            createPhysicalDbsIfNotExists(instId, dbInfo.dbName, dbInfo.charset, dbInfo.collation);

            // ---- update db_status to running by trx  ----
            dbInfoAccessor.updateDbStatusByDbName(logicalDatabaseName, DbInfoRecord.DB_STATUS_RUNNING);

            // ----- add the dataIfo for new db----
            MetaDbConfigManager.getInstance()
                .register(MetaDbDataIdBuilder.getDbTopologyDataId(logicalDatabaseName), metaDbConn);

            // ----- notify other node to reload dbInfos ----
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getDbInfoDataId(), metaDbConn);

            // commit configs
            metaDbConn.commit();

            // sync other node to reload dbInfo quickly
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());

            // release meta db lock
            LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }
    */
    private static Map<String, List<Pair<String, String>>> generateDbAndGroupConfigInfoForPartitionTables(
        List<DbGroupInfoRecord> dbGroupInfoRecords,
        List<String> storageInstIdList,
        String logicalDbName,
        Connection metaDbConn) {
        int physical_db_index = 0;
        StringBuilder groupNameInfo = new StringBuilder();
        int i = 0;
        Map<String, List<Pair<String, String>>> storageInstGrpAndDbMap = new HashMap<>();
        for (DbGroupInfoRecord dbGroupInfoRecord : dbGroupInfoRecords) {
            int len = dbGroupInfoRecord.phyDbName.length();
            //ref to com.taobao.tddl.gms.util.GroupInfoUtil.PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES
            String digitPart = dbGroupInfoRecord.phyDbName.substring(len - 5);
            if (!StringUtils.isEmpty(digitPart) && digitPart.matches("^\\d+$")) {
                int index = Integer.valueOf(digitPart);
                if (index >= physical_db_index) {
                    physical_db_index = index + 1;
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("the physical database name :[%s] is not compliant, the expect format is:[%s]",
                        dbGroupInfoRecord.phyDbName, GroupInfoUtil.PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES));
            }

        }
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);
        for (String storageInstId : storageInstIdList) {
            boolean exist = groupDetailInfoAccessor
                .checkGroupDetailInfoExistenceByStorageInstIdAndDbNameAndGroupNames(storageInstId, logicalDbName,
                    dbGroupInfoRecords.stream().map(o -> o.groupName).collect(Collectors.toList()));
            if (!exist) {
                String groupName = GroupInfoUtil.buildGroupName(logicalDbName, physical_db_index, true);
                String phsicalDdName = GroupInfoUtil.buildPhyDbName(logicalDbName, physical_db_index, true);
                physical_db_index++;
                Pair<String, String> pair = new Pair<>(groupName, phsicalDdName);
                List<Pair<String, String>> pairs = new ArrayList<>();
                pairs.add(pair);
                storageInstGrpAndDbMap.put(storageInstId, pairs);
            }
        }
        return storageInstGrpAndDbMap;
    }

    /**
     * Auto init phy db in new dn, unused code , should be removed
     */
    /*
    public static void initPhysicalDbForPartitionTableIfNeed() {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            List<DbInfoRecord> dbInfoRecords = dbInfoAccessor.getDbInfoByType(DbGroupInfoRecord.GROUP_TYPE_NORMAL);
            for (DbInfoRecord dbInfoRecord : dbInfoRecords) {
                createPhysicalPartitionDbsForPerLogicalDb(dbInfoRecord);
            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }
    */
    public static void copyTablesForNewGroup(List<String> phyTables, String srcDbName, String srcStorageInstId,
                                             String targetDbName,
                                             String targetStorageInstId) {
        Map<String, String> createSqls = new HashMap<>();
        try (Connection conn = getConnectionForStorage(srcStorageInstId)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + srcDbName);
                for (String tableName : phyTables) {
                    ResultSet resultSet = stmt.executeQuery("show create table " + tableName);
                    while (resultSet.next()) {
                        String createSql = resultSet.getString(2);
                        if (!createSql.startsWith("CREATE TABLE IF NOT EXISTS") && createSql
                            .startsWith("CREATE TABLE")) {
                            createSql = createSql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS ");
                        }
                        createSqls.put(tableName, createSql);
                    }
                }
            }
        } catch (SQLException ex) {
            throw GeneralUtil.nestedException(ex);
        }

        try (Connection conn = getConnectionForStorage(targetStorageInstId)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("use " + targetDbName);
                for (Map.Entry<String, String> entry : createSqls.entrySet()) {
                    stmt.execute(entry.getValue());
                }
            }
        } catch (SQLException ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static Connection getConnectionForStorage(String storageInstId) {
        HaSwitchParams haSwitchParams = StorageHaManager.getInstance().getStorageHaSwitchParams(storageInstId);
        if (haSwitchParams == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInstId));
        }
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(haSwitchParams.curAvailableAddr);
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();
        String user = haSwitchParams.userName;
        String passwdEnc = haSwitchParams.passwdEnc;
        String storageConnProps = haSwitchParams.storageConnPoolConfig.connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(-1, storageConnProps);
        return GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps);
    }

    public static Connection getConnectionForStorage(StorageInfoRecord storageInfoRecord) {
        if (storageInfoRecord == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format("no found the storage inst for %s", storageInfoRecord));
        }
        Preconditions.checkArgument(storageInfoRecord.instKind == 1, "Slave Storage only create connection!");
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(storageInfoRecord.getHostPort());
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();
        String user = storageInfoRecord.user;
        String passwdEnc = storageInfoRecord.passwdEnc;
        String storageConnProps = ConnPoolConfigManager.getInstance().getConnPoolConfig().connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(-1, storageConnProps);
        return GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps);
    }

    public static Connection getConnectionForStorage(StorageInstHaContext instHaContext) {
        String address = instHaContext.getCurrAvailableNodeAddr();
        String user = instHaContext.getUser();
        String passwdEnc = instHaContext.getEncPasswd();
        Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(address);
        String host = ipAndPort.getKey();
        int port = ipAndPort.getValue();

        String storageConnProps = ConnPoolConfigManager.getInstance().getConnPoolConfig().connProps;
        String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(-1, storageConnProps);
        return GmsJdbcUtil
            .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps);
    }

    /*
     * return key:instId, value:group/phyDb
     * */
    public static Map<String, List<Pair<String, String>>> generateDbAndGroupNewConfigInfo(String dbName) {

        Map<String, List<Pair<String, String>>> storageInstGrpAndDbMap;
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        Connection conn = null;
        try {
            conn = dataSource.getConnection();

            // This instId may be polardbx master instId or polardbx slave instId,
            // but only polardbx master instId allow create db
            String instId = InstIdUtil.getInstId();
            assert instId != null;

            // ----check db if exists----
            // check if exists the config of target db
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(conn);
            DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
            if (dbInfoRecord != null) {
                if (dbInfoRecord.dbType == DbInfoRecord.DB_TYPE_DEFAULT_DB
                    || dbInfoRecord.dbType == DbInfoRecord.DB_TYPE_SYSTEM_DB) {
                    return null;
                }
            } else {
                return null;
            }

            // ----add storage_inst_config for new db----
            int storageKind = StorageInfoRecord.INST_KIND_MASTER;

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(conn);
            // batch get all storage configs (they ary connPool configs) by instId
            List<String> storageInstIdList =
                storageInfoAccessor.getStorageIdListByInstIdAndInstKind(instId, storageKind);

            // ----add group_detail_info for new db----
            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            dbGroupInfoAccessor.setConnection(conn);

            List<DbGroupInfoRecord> dbGroupInfoRecords = dbGroupInfoAccessor
                .getDbGroupInfoByDbNameAndGroupType(dbName,
                    DbGroupInfoRecord.GROUP_TYPE_NORMAL, false);
            storageInstGrpAndDbMap =
                generateDbAndGroupConfigInfoForPartitionTables(dbGroupInfoRecords, storageInstIdList,
                    dbName, conn);

        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create db[%s], err is %s", dbName, ex.getMessage())
            );
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
            }
        }
        return storageInstGrpAndDbMap;
    }

    public static boolean isEnablePartitionManagement() {
        return enablePartitionManagement;
    }

    public static void setEnablePartitionManagement(boolean enablePartitionManagement) {
        DbTopologyManager.enablePartitionManagement = enablePartitionManagement;
    }

}
