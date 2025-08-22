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

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.config.impl.ConnPoolConfigManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.engine.FileStorageMetaStore;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoAccessor;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignAccessor;
import com.alibaba.polardbx.gms.metadb.foreign.ForeignRecord;
import com.alibaba.polardbx.gms.metadb.misc.PersistentReadWriteLock;
import com.alibaba.polardbx.gms.metadb.misc.ReadWriteLockRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.rebalance.RebalanceTarget;
import com.alibaba.polardbx.gms.util.AppNameUtil;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.SYS_SCHEMA_DB_NAME;

/**
 * @author chenghui.lch
 */
public class DbTopologyManager {
    private final static Logger logger = LoggerFactory.getLogger(DbTopologyManager.class);

    public static final String CREATE_DB_IF_NOT_EXISTS_SQL_TEMPLATE = "create database if not exists `%s`";

    public static final String DROP_DB_IF_EXISTS_SQL_TEMPLATE = "drop database if exists `%s`";

    public static final String CHARSET_TEMPLATE = " character set `%s`";

    public static final String COLLATION_TEMPLATE = " collate `%s`";

    public static final String DEFAULT_SERVER_COLLATION = "utf8mb4_general_ci";

    public static final int DEFAULT_SHARD_DB_COUNT_EACH_STORAGE_INST = 8;

    public static final int DEFAULT_MAX_LOGICAL_DB_COUNT = 32;

    public static final int DEFAULT_RENAME_PHY_DB_BATCH_SIZE = 1024;

    public static final long DEFAULT_PARTITION_COUNT_EACH_DN = 8;

    //public static String defaultCharacterSetForCreatingDb = DEFAULT_DB_CHARACTER_SET;

    public static String defaultCollationForCreatingDb = DEFAULT_SERVER_COLLATION;

    public static int maxLogicalDbCount = DbTopologyManager.DEFAULT_MAX_LOGICAL_DB_COUNT;

    protected static int shardDbCountEachStorageInst = DbTopologyManager.DEFAULT_SHARD_DB_COUNT_EACH_STORAGE_INST;

    protected static String defaultPartitionMode = DbInfoManager.MODE_DRDS;

    protected static SchemaMetaCleaner schemaMetaCleaner = null;

    public static List<String> singleGroupStorageInstList = new ArrayList<>();

    /**
     * A topology mapping from GroupKey to logicalDbName
     * <pre>
     *      key: The upperCase of GroupKey
     *      val: the name of logical dbName from db_info
     *  </pre>
     */
    private static final ReentrantLock groupTopologyMappingLock = new ReentrantLock();
    private static volatile Map<String, String> groupTopologyMapping =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    /**
     * <pre>
     *     connId -> ddlContext
     * </pre>
     */
    protected static final Map<Long, DatabaseDdlContext> connIdToDatabaseDdlContextMapping = new ConcurrentHashMap<>();

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
                    if (!singleGroupStorageInstList.contains(storageInstId)) {
                        singleGroupStorageInstList.add(storageInstId);
                    }
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

    public static List<DbInfoRecord> getNewPartDbInfoFromMetaDb(String schemaName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            return dbInfoAccessor.getDbInfoBySchAndType(schemaName, DbInfoRecord.DB_TYPE_NEW_PART_DB);
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
                                                    String groupName, String charset, String collation, int dbType) {
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
            createDbInfo.collation = collation;
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
                new DefaultGroupLocator(createDbInfo.dbType,
                    createDbInfo.groupPhyDbMap, createDbInfo.storageInstList, new LocalityDesc(), new ArrayList<>());
            DbTopologyManager.createLogicalDb(createDbInfo);

        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex, "failed to create default db, err is " +
                ex.getMessage());
        }
    }

    protected static void registerDdlContextIntoDbTopologyManager(DatabaseDdlContext ddlContext) {
        connIdToDatabaseDdlContextMapping.put(ddlContext.getConnId(), ddlContext);
    }

    protected static void unregisterDdlContextFromDbTopologyManager(DatabaseDdlContext ddlContext) {
        connIdToDatabaseDdlContextMapping.remove(ddlContext.getConnId());
    }

    public static void killDdlQueryByConnId(Long connId) {
        DatabaseDdlContext databaseDdlContext = getDdlContextFromDbTopologyManager(connId);
        if (databaseDdlContext != null) {
            databaseDdlContext.setInterrupted(true);
        }
    }

    public static DatabaseDdlContext getDdlContextFromDbTopologyManager(Long connId) {
        return connIdToDatabaseDdlContextMapping.get(connId);
    }

    protected static void initDatabaseDdlContextIfNeedForCreateDb(CreateDbInfo createDbInfo) {
        if (createDbInfo.getConnId() != null) {
            DatabaseDdlContext databaseDdlContext = new DatabaseDdlContext();
            databaseDdlContext.setConnId(createDbInfo.getConnId());
            databaseDdlContext.setTraceId(createDbInfo.getTraceId());
            createDbInfo.setDdlContext(databaseDdlContext);
            registerDdlContextIntoDbTopologyManager(databaseDdlContext);
        }
    }

    protected static void initDatabaseDdlContextIfNeedForDropDb(DropDbInfo dropDbInfo) {
        if (dropDbInfo.getConnId() != null) {
            DatabaseDdlContext databaseDdlContext = new DatabaseDdlContext();
            databaseDdlContext.setConnId(dropDbInfo.getConnId());
            databaseDdlContext.setTraceId(dropDbInfo.getTraceId());
            databaseDdlContext.setDropDb(true);
            dropDbInfo.setDdlContext(databaseDdlContext);
            registerDdlContextIntoDbTopologyManager(databaseDdlContext);
        }
    }

    public static long createLogicalDb(CreateDbInfo createDbInfo) {
        long dbId = -1;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {

            // register ddlCtx into DbTopologyManger
            initDatabaseDdlContextIfNeedForCreateDb(createDbInfo);

            // acquire MetaDb Lock by for update, to avoiding concurrent create & drop databases
            metaDbLockConn.setAutoCommit(false);

//            LockUtil.waitToAcquireMetaDbLock(String.format("Get metaDb lock interrupted during creating db[%s]",
//                createDbInfo.getDbName()), metaDbLockConn);

            LockUtil.waitToAcquireMetaDbLock(
                String.format("Get metaDb lock interrupted during creating db[%s], connId[%s], traceId[%s]",
                    createDbInfo.getDbName(), createDbInfo.getConnId(), createDbInfo.getTraceId()), metaDbLockConn,
                createDbInfo.getDdlContext());

            // ---- check if logical db exists ----
            String dbName = createDbInfo.dbName;

            // check db name
            if (Arrays.stream(SYS_SCHEMA_DB_NAME).anyMatch(dbName::equalsIgnoreCase)) {
                // throw exception
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Can't create database '%s', as it is a keyword", dbName));
            }

            boolean createIfNotExist = createDbInfo.isCreateIfNotExists;
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            dbInfoAccessor.setConnection(metaDbConn);
            DbInfoRecord dbInfo = dbInfoAccessor.getDbInfoByDbName(dbName);
            boolean hasDbConfig = false;
            if (dbInfo != null) {
                if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING
                    || dbInfo.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                    if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_RUNNING) {
                        if (createIfNotExist) {
                            return dbId;
                        }
                        // throw exception
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format("Create db error, db[%s] has already exist", dbName));
                    } else {
                        // throw exception
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                            String.format(
                                "Create db error, db[%s] has not finished dropping, please retry to drop it by using 'drop database if exists `%s`'",
                                dbName, dbName));
                    }

                } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_CREATING) {
                    // ignore, just continue to create phy dbs
                    hasDbConfig = true;
                } else if (dbInfo.dbStatus == DbInfoRecord.DB_STATUS_DROPPING) {
                    // call dropLogicalDb to clean env, and then continue to creating new db
                    long ts;
                    int BITS_LOGICAL_TIME = 22;
                    ts = System.currentTimeMillis() << BITS_LOGICAL_TIME;

                    dropLogicalDbWithConn(dbName, true, metaDbConn, metaDbLockConn, createDbInfo.socketTimeout, ts,
                        false, false, DEFAULT_DDL_VERSION_ID, createDbInfo.getDdlContext());
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

            // refresh local group topology of db topology manager
            DbTopologyManager.refreshGroupKeysIntoTopologyMapping(createDbInfo.groupNameList, new ArrayList<>(),
                createDbInfo.dbName);

            // sync other node to reload dbInfo quickly
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbInfoDataId());

            // process the hook func list for new created db(like cdc/locality e.g)
            handleHookFuncList(createDbInfo, dbId);

            // release meta db lock
            LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);
            return dbId;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        } finally {
            if (createDbInfo.getDdlContext() != null) {
                unregisterDdlContextFromDbTopologyManager(createDbInfo.getDdlContext());
            }
        }
    }

    private static void handleHookFuncList(CreateDbInfo createDbInfo, long dbId) {
        List<CreateDbInfo.CreatedDbHookFunc> createdDbHookFuncList = createDbInfo.getCreatedDbHookFuncList();
        for (int i = 0; i < createdDbHookFuncList.size(); i++) {
            CreateDbInfo.CreatedDbHookFunc hookFunc = createdDbHookFuncList.get(i);
            hookFunc.handle(dbId);
        }
    }

    protected static void dropLogicalDbWithConn(String dbName,
                                                boolean isDropIfExists,
                                                Connection metaDbConn,
                                                Connection metaDbLockConn,
                                                long socketTimeout,
                                                long ts,
                                                boolean allowDropForce,
                                                boolean reservePhyDb,
                                                long versionId,
                                                DatabaseDdlContext ddlContext)
        throws SQLException {

        // acquire MetaDb Lock by for update, to avoiding concurrent create & drop databases
        metaDbLockConn.setAutoCommit(false);
//        try {
//            LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
//        } catch (Throwable ex) {
//            // throw exception
//            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
//                String.format("Get metaDb lock timeout during drop db[%s], please retry", dbName));
//        }
        LockUtil.waitToAcquireMetaDbLock(
            String.format("Get metaDb lock timeout during drop db[%s], connId[%s], traceId[%s]", dbName,
                ddlContext.getConnId(), ddlContext.getTraceId()),
            metaDbLockConn, ddlContext);

        if (!allowDropForce) {
            if (checkDbExists(dbName)) {
                if (!tryAcquireLock(dbName)) {
                    String recommends = getAcquireRecommends(dbName);
                    if (StringUtils.isEmpty(recommends)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "Drop database not allowed when scale-in/out");
                    }
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "Drop database not allowed. " + recommends);
                }
            }
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

        // TODO(shengyu): delete cross schema ddl.
        fileStoreDestroy(dbInfo.dbName, ts);

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
        if (!reservePhyDb) {
            dropPhysicalDbsIfExists(instId, dbName, socketTimeout);
        }

        // ---- remove topology config & group config from metadb by using trx ----
        metaDbConn.setAutoCommit(false);

        // ----- cleanup all schema meta infos & configs
        if (DbTopologyManager.schemaMetaCleaner != null) {
            schemaMetaCleaner.clearSchemaMeta(dbName, metaDbConn, versionId);
        }

        // clear group_detail_info
        List<String> allGrpNames = getGroupNamesFromMetaDb(metaDbConn, instId, dbName);

        // ---- remove topology configs
        removeDbTopologyConfig(dbName, metaDbConn, versionId);
        metaDbConn.commit();

        // refresh local group topology of db topology manager of memory
        DbTopologyManager.unregisterGroupKeysIntoTopologyMapping(allGrpNames);

    }

    private static List<String> getGroupNamesFromMetaDb(Connection metaDbConn, String instId, String dbName) {
        List<String> allGrpNames = new ArrayList<>();
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);
        List<GroupDetailInfoRecord> grpInfos = groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId,
            dbName);
        for (int i = 0; i < grpInfos.size(); i++) {
            allGrpNames.add(grpInfos.get(i).getGroupName());
        }
        return allGrpNames;
    }

    public static void dropLogicalDb(DropDbInfo dropDbInfo) {
        final String moveGroupTaskName = "ActionMoveGroup";

        String dbName = dropDbInfo.dbName;
        boolean isDropIfExists = dropDbInfo.isDropIfExists;
        long socketTimeout = dropDbInfo.socketTimeout;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection();
            Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {

            initDatabaseDdlContextIfNeedForDropDb(dropDbInfo);

            dropLogicalDbWithConn(dbName, isDropIfExists, metaDbConn, metaDbLockConn, socketTimeout, dropDbInfo.getTs(),
                dropDbInfo.isAllowDropForce(), dropDbInfo.isReservePhyDb(), dropDbInfo.getVersionId(),
                dropDbInfo.getDdlContext());

            // release meta db lock
            LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        } finally {
            if (dropDbInfo.getDdlContext() != null) {
                DbTopologyManager.unregisterDdlContextFromDbTopologyManager(dropDbInfo.getDdlContext());
            }
        }
    }

    private static boolean tryAcquireLock(String dbName) {
        PersistentReadWriteLock readWriteLock = PersistentReadWriteLock.create();
        final String resourceName = LockUtil.genRebalanceResourceName(RebalanceTarget.DATABASE, dbName);
        final String resourceDBName = LockUtil.genForbidDropResourceName(dbName);
        final String owner = resourceName;
        return readWriteLock.tryWriteLockBatch(dbName, owner, Sets.newHashSet(resourceName, resourceDBName));
    }

    private static String getAcquireRecommends(String dbName) {
        PersistentReadWriteLock readWriteLock = PersistentReadWriteLock.create();
        final String resourceDBName = LockUtil.genForbidDropResourceName(dbName);
        List<ReadWriteLockRecord> records = readWriteLock.queryByResource(Sets.newHashSet(resourceDBName));
        return readWriteLock.toRecommend(records, Sets.newHashSet(resourceDBName));
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
        if (charset != null && !charset.isEmpty()) {
            createDbSql += String.format(CHARSET_TEMPLATE, charset.toLowerCase());
        }

        if (collation != null && !collation.isEmpty()) {
            createDbSql += String.format(COLLATION_TEMPLATE, collation.toLowerCase());
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
                    createDbInfo.collation, createDbInfo.encryption, createDbInfo.defaultSingle);

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
    public static void removeDbTopologyConfig(String dbName, Connection conn, long versionId) {

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

    public static List<DbGroupInfoRecord> getAllDbGroupInfoRecordByInstId(String dbName, String storageInstId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(metaDbConn);
            return accessor.getAllPhyDbNameByInstId(dbName, storageInstId);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static List<DbGroupInfoRecord> getAllDbGroupInfoRecordByInstIdAndPhyDbName(String phyDbName,
                                                                                      String storageInstId) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor accessor = new GroupDetailInfoAccessor();
            accessor.setConnection(metaDbConn);
            return accessor.getAllPhyDbNameByInstIdAndPhyDbName(phyDbName, storageInstId);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static CreateDbInfo initCreateDbInfo(String dbName, String charset, String collate,
                                                LocalityDesc locality,
                                                Predicate<StorageInfoRecord> localityFilter,
                                                int dbType,
                                                boolean isCreateIfNotExists,
                                                long socketTimeout,
                                                int shardDbCountEachStorageInstOfStmt) {
        return initCreateDbInfo(dbName, charset, collate, null, null, locality, localityFilter, dbType,
            isCreateIfNotExists,
            socketTimeout, shardDbCountEachStorageInstOfStmt);
    }

    public static CreateDbInfo initCreateDbInfo(String dbName,
                                                String charset,
                                                String collate,
                                                Boolean encryption,
                                                Boolean defaultSingle,
                                                LocalityDesc locality,
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
                .filter(x -> localityFilter == null || localityFilter.test(x)) // filter locality property
                .filter(StorageInfoRecord::isStatusReady) // create database on `READY` ata-nodes
                .map(StorageInfoRecord::getInstanceId)
                .distinct()
                .collect(Collectors.toList());

            if (storageIdList.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    "No storage match the locality " + locality.toString());
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
        createDbInfo.encryption = encryption;
        createDbInfo.dbType = dbType;

        String grpNameSingleGroup = "";
        List<String> groupNames = new ArrayList<>();
        if (createDbInfo.dbType == DbInfoRecord.DB_TYPE_PART_DB || createDbInfo.dbType == DbInfoRecord.DB_TYPE_CDC_DB) {
            grpNameSingleGroup = GroupInfoUtil.buildGroupName(dbName, -1, false);
            String phyDbNameSingle = GroupInfoUtil.buildPhyDbName(dbName, -1, false);
            grpAndPhyDbNameMap.put(grpNameSingleGroup, phyDbNameSingle);
            for (int i = 0; i < shardDbCount; i++) {
                String grpName = GroupInfoUtil.buildGroupName(dbName, i, false);
                String phyDbName = GroupInfoUtil.buildPhyDbName(dbName, i, false);
                grpAndPhyDbNameMap.put(grpName, phyDbName);
            }
            createDbInfo.defaultSingle = true;
        } else if (createDbInfo.dbType == DbInfoRecord.DB_TYPE_NEW_PART_DB) {
            for (int i = 0; i < storageInstCount; i++) {
                String grpName = GroupInfoUtil.buildGroupName(dbName, i, true);
                String phyDbName = GroupInfoUtil.buildPhyDbName(dbName, i, true);
                grpAndPhyDbNameMap.put(grpName, phyDbName);
                if (StringUtils.isEmpty(grpNameSingleGroup)) {
                    grpNameSingleGroup = grpName;
                }
            }
            createDbInfo.defaultSingle = false;
            if (defaultSingle != null) {
                createDbInfo.defaultSingle = defaultSingle;
            }
        }
        groupNames.addAll(grpAndPhyDbNameMap.keySet());

        createDbInfo.locality = locality;
        createDbInfo.groupPhyDbMap = grpAndPhyDbNameMap;
        createDbInfo.groupNameList = groupNames;
        createDbInfo.singleGroup = grpNameSingleGroup;
        createDbInfo.defaultDbIndex = grpNameSingleGroup;
        createDbInfo.groupLocator =
            new DefaultGroupLocator(createDbInfo.dbType, createDbInfo.groupPhyDbMap, createDbInfo.storageInstList,
                createDbInfo.locality,
                DbTopologyManager.singleGroupStorageInstList);

        createDbInfo.isCreateIfNotExists = isCreateIfNotExists;
        createDbInfo.socketTimeout = socketTimeout;
        createDbInfo.shardDbCountEachInst = shardDbCountEachStorage;

        return createDbInfo;
    }

    /**
     * used for import database
     */
    public static CreateDbInfo initCreateDbInfoForImportDatabase(String dbName, String charset, String collate,
                                                                 Boolean encryption,
                                                                 LocalityDesc locality,
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
                .filter(x -> localityFilter == null || localityFilter.test(x)) // filter locality property
                .filter(StorageInfoRecord::isStatusReady) // create database on `READY` ata-nodes
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

        createDbInfo.dbName = dbName;
        createDbInfo.charset = charset;
        createDbInfo.collation = collate;
        createDbInfo.encryption = encryption;
        createDbInfo.dbType = dbType;
        createDbInfo.defaultSingle = false;

        String grpNameSingleGroup = "";

        for (int i = 0; i < storageInstCount; i++) {
            String grpName = String.format(GroupInfoUtil.GROUP_NAME_FOR_IMPORTED_DATABASE, dbName).toUpperCase();
            String phyDbName = dbName;
            grpAndPhyDbNameMap.put(grpName, phyDbName);
            if (StringUtils.isEmpty(grpNameSingleGroup)) {
                grpNameSingleGroup = grpName;
            }
        }

        createDbInfo.locality = locality;
        createDbInfo.groupPhyDbMap = grpAndPhyDbNameMap;
        createDbInfo.singleGroup = grpNameSingleGroup;
        createDbInfo.defaultDbIndex = grpNameSingleGroup;
        createDbInfo.groupLocator =
            new DefaultGroupLocator(createDbInfo.dbType, createDbInfo.groupPhyDbMap, createDbInfo.storageInstList,
                createDbInfo.locality,
                DbTopologyManager.singleGroupStorageInstList);

        createDbInfo.isCreateIfNotExists = isCreateIfNotExists;
        createDbInfo.socketTimeout = socketTimeout;
        createDbInfo.shardDbCountEachInst = shardDbCountEachStorage;
        return createDbInfo;
    }

    public static String getStorageInstIdByGroupName(String dbName, String groupName) {
        return getStorageInstIdByGroupName(InstIdUtil.getInstId(), dbName, groupName);
    }

    public static Map<String, StorageInfoRecord> getStorageInfoMap(String instId) {
        Map<String, StorageInfoRecord> storageInfoMap = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            // Get storage info by group name
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            List<StorageInfoRecord> storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId)
                .stream().filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER).collect(Collectors.toList());
            Map<String, StorageInfoRecord> storageInfoRecordMap = new HashMap<>();
            for (StorageInfoRecord storageInfoRecord : storageInfoRecords) {
                if (!storageInfoMap.containsKey(storageInfoRecord.storageMasterInstId)) {
                    storageInfoMap.put(storageInfoRecord.storageMasterInstId, storageInfoRecord);
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return storageInfoMap;

    }

    public static Set<String> getAllAliveStorageInsts(String instId) {
        Set<String> allStorageIds = new HashSet<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            // Get storage info by group name
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            allStorageIds = storageInfoAccessor.getStorageInfosByInstId(instId)
                .stream().filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER)
                .filter(o -> o.status == StorageInfoRecord.STORAGE_STATUS_READY)
                .map(o -> o.storageInstId)
                .collect(Collectors.toSet());
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return allStorageIds;

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

    public static Map<String, String> getGroupNameToStorageInstIdMap(String dbName) {
        Map<String, String> groupNameStorageInstIdMap = new HashMap<>();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            // Get storage info by group name
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            String instId = InstIdUtil.getMasterInstId();
            List<GroupDetailInfoRecord> groupDetails =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId, dbName);
            if (groupDetails != null) {
                groupNameStorageInstIdMap =
                    groupDetails.stream().collect(Collectors.toMap(o -> o.groupName, o -> o.storageInstId));
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return groupNameStorageInstIdMap;

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

                if (dbGroupInfoRecord.groupType != DbGroupInfoRecord.GROUP_TYPE_ADDING) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Failed add new group[%s] because the group_type is %s",
                            grpKey, dbGroupInfoRecord.groupType));
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

    public static void cleanPhyDbOfRemovingDbGroup(String schema, String groupName, Connection metaDbConn,
                                                   long socketTimeout) {

        /**
         * Check if the meta info of the to-removed group is valid
         */
        String storageInstId = null;
        String phyDbName = null;
        try {
            String defaultGroup = TableInfoManager.getSchemaDefaultDbIndex(schema);
            if (StringUtils.equalsIgnoreCase(defaultGroup, groupName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                    String.format("Default group could not be removed: %s.%s", schema, groupName));
            }

            DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            dbGroupInfoAccessor.setConnection(metaDbConn);
            GroupDetailInfoRecord detailRecord =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(InstIdUtil.getInstId(), schema,
                    groupName);
            DbGroupInfoRecord dbGroupRecord = dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(schema, groupName);
            if (detailRecord == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("Group not exists: %s[%s] in ", schema, groupName));
            }
            if (dbGroupRecord != null) {
                if (!dbGroupRecord.isRemoving()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Could not clean non-removing group[%s] of type [%d]", groupName,
                            dbGroupRecord.getGroupTypeStr()));
                }
            }
            phyDbName = dbGroupRecord.phyDbName;
            storageInstId = detailRecord.storageInstId;
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }

        /**
         * Drop physical db for the to-removed group
         */
        dropPhyDbForRemovingGroup(storageInstId, groupName, phyDbName, socketTimeout);
    }

    public static void dropPhyDbForRemovingGroup(String storageInstId,
                                                 String newGroupName,
                                                 String newPhyDbName,
                                                 long socketTimeout) {
        try {
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.put(newGroupName, newPhyDbName);
            dropPhysicalDbsInStorageInst(storageInstId, grpPhyDbMap, socketTimeout);
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static void removeOldGroupFromDb(String dbName,
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

        try {
            GroupDetailInfoRecord groupDetailInfoRecord =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndGroupName(instId, dbName, newGroupName);
            DbGroupInfoRecord dbGroupInfoRecord =
                dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(dbName, newGroupName);
            if (dbGroupInfoRecord != null) {
                // remove db_group_info and group_detail_info from db for target groups
                dbGroupInfoAccessor.deleteDbGroupInfoByDbAndGroup(dbName, newGroupName);
                groupDetailInfoAccessor.deleteGroupDetailInfoByDbAndGroup(dbName, newGroupName);

                if (!dbGroupInfoRecord.isRemovable()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                        String.format("Could not remove group[%s] of type %d", newGroupName,
                            dbGroupInfoRecord.groupType));
                }

                String dataIdOfGroup =
                    MetaDbDataIdBuilder.getGroupConfigDataId(groupDetailInfoRecord.instId, dbName, newGroupName);
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

                case DbGroupInfoRecord.GROUP_TYPE_BEFORE_REMOVE:
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

    public static String getPhysicalDbNameByGroupKeyFromMetaDb(String dbName, String groupName) {

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

        if (ConfigDataMode.isMasterMode()) {
            // if curr inst is master mode, then ignore to init topology configs for ro inst
            return;
        }

        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            metaDbConn.setAutoCommit(false);

            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);

            //register group info for information_schema although the role is columnar
            List<String> storageMetaDbInstIdList = storageInfoAccessor
                .getStorageIdListByInstIdAndInstKind(masterInstId, StorageInfoRecord.INST_KIND_META_DB);
            for (int i = 0; i < storageMetaDbInstIdList.size(); i++) {
                groupDetailInfoAccessor.addNewGroupDetailInfosForRoInst(instId, storageMetaDbInstIdList.get(i),
                    storageMetaDbInstIdList.get(i), true);
            }

            //register group info
            if (ConfigDataMode.isRowSlaveMode()) {
                List<Pair<String, String>> storageRoInfoList =
                    storageInfoAccessor.getReadOnlyStorageInfoListByInstIdAndInstKind(instId);
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
            String digitPart = len >= 5 ?
                dbGroupInfoRecord.phyDbName.substring(len - 5)
                : null;
            if (!StringUtils.isEmpty(digitPart) && digitPart.matches("^\\d+$")) {
                int index = Integer.valueOf(digitPart);
                if (index >= physical_db_index) {
                    physical_db_index = index + 1;
                }
            } else {
                /**
                 * imported database has the incompliant physical name, so we ignore this error
                 * */
//                throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
//                    String.format("the physical database name :[%s] is not compliant, the expect format is:[%s]",
//                        dbGroupInfoRecord.phyDbName, GroupInfoUtil.PHY_DB_NAME_TEMPLATE_FOR_PARTITIONED_TABLES));
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
        return getConnectionForStorage(storageInstId, -1);
    }

    public static Connection getConnectionForStorage(String storageInstId, int socketTimeout) {
        return getConnectionForStorage(storageInstId, GmsJdbcUtil.DEFAULT_PHY_DB, socketTimeout);
    }

    public static Connection getConnectionForStorage(String storageInstId, String schema, int socketTimeout) {
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
        return GmsJdbcUtil
            .buildJdbcConnection(host, port, schema, user, passwdEnc, connProps);
    }

    public static Connection getConnectionForLearnerStorage(StorageInfoRecord storageInfoRecord) {
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

    public static Connection getFollowerConnectionForStorage(StorageInstHaContext storageInstHaContext) {
        StorageInfoRecord storageInfoRecord = storageInstHaContext.getFollowerNode();
        if (storageInfoRecord != null) {
            String address = storageInfoRecord.getHostPort();
            String user = storageInstHaContext.getUser();
            String passwdEnc = storageInstHaContext.getEncPasswd();
            Pair<String, Integer> ipAndPort = AddressUtils.getIpPortPairByAddrStr(address);
            String host = ipAndPort.getKey();
            int port = ipAndPort.getValue();
            String storageConnProps = ConnPoolConfigManager.getInstance().getConnPoolConfig().connProps;
            String connProps = getJdbcConnPropsFromAtomConnPropsForGroup(-1, storageConnProps);
            return GmsJdbcUtil
                .buildJdbcConnection(host, port, GmsJdbcUtil.DEFAULT_PHY_DB, user, passwdEnc, connProps);
        } else {
            return null;
        }
    }

    /*
     * return key:instId, value:group/phyDb
     * */
    public static Map<String, List<Pair<String, String>>> generateDbAndGroupNewConfigInfo(String dbName,
                                                                                          LocalityDesc localityDesc) {

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
            if (localityDesc.hasStoragePoolDefinition() && !localityDesc.holdEmptyDnList()) {
                storageInstIdList = storageInstIdList.stream().filter(localityDesc::fullMatchStorageInstance).collect(
                    Collectors.toList());
            } else if (!localityDesc.holdEmptyDnList()) {
                storageInstIdList = storageInstIdList.stream().filter(localityDesc::matchStorageInstance).collect(
                    Collectors.toList());
            }

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

    public static String getDefaultPartitionMode() {
        return defaultPartitionMode;
    }

    public static void setDefaultPartitionMode(String defaultPartitionMode) {
        DbTopologyManager.defaultPartitionMode = defaultPartitionMode;
    }

    public static long decideAutoPartitionCount() {
        long autoPartCount = DEFAULT_PARTITION_COUNT_EACH_DN;
        long rwDnCount = 0;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor dnInfoAccessor = new StorageInfoAccessor();
            dnInfoAccessor.setConnection(conn);
            rwDnCount = dnInfoAccessor.countAllRwStorageInsts();
        } catch (Throwable ex) {
            logger.warn("Failed to fetch the rw-dn count from metadb", ex);
        }
        if (rwDnCount > 0) {
            // Use dn * DEFAULT_PARTITION_COUNT_EACH_DN
            // as the default partition count of auto partitions
            // and save it into metadb after the first starting up.
            autoPartCount = rwDnCount * DEFAULT_PARTITION_COUNT_EACH_DN;
        }
        return autoPartCount;
    }

    public static boolean checkStorageInstDeletable(Set<String> nonDeletableStorageInstSet,
                                                    String targetDnId,
                                                    int dbInstKind) {
        // only the dnId is a rw-dn and it is non in the set of non-deletable dn set (default dn set and single_group dn set)
        // are allowed to be deleted (scale-in)
        boolean deletable = (dbInstKind == StorageInfoRecord.INST_KIND_MASTER) &&
            !nonDeletableStorageInstSet.contains(targetDnId);
        return deletable;

    }

    public static Set<String> getNonDeletableStorageInst(Connection metaDbConn) {

        // Storage instance contains default group is not deletable
        Map<String, String> schema2DefaultGroup = TableInfoManager.getAllDefaultDbIndex(metaDbConn);
        Set<String> allDefaultGroupSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        allDefaultGroupSet.addAll(schema2DefaultGroup.values());
        Set<String> allNonDeletableStorageInstIdSet = new HashSet<>();
        String instId = InstIdUtil.getInstId();
        GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
        StoragePoolInfoAccessor storagePoolInfoAccessor = new StoragePoolInfoAccessor();
        groupDetailInfoAccessor.setConnection(metaDbConn);
        storagePoolInfoAccessor.setConnection(metaDbConn);

        List<GroupDetailInfoRecord> allGrpDetailInfos = groupDetailInfoAccessor.getGroupDetailInfoByInstId(instId);
        for (int i = 0; i < allGrpDetailInfos.size(); i++) {
            GroupDetailInfoRecord groupDetail = allGrpDetailInfos.get(i);
            String grpName = groupDetail.getGroupName();
            String dnId = groupDetail.getStorageInstId();
            boolean isSingleGrp = GroupInfoUtil.isSingleGroup(grpName);
            /**
             * Collect all the dnId of default_group and single_group
             */
            if (allDefaultGroupSet.contains(grpName) || isSingleGrp) {
                if (!allNonDeletableStorageInstIdSet.contains(dnId)) {
                    allNonDeletableStorageInstIdSet.add(dnId);
                }
            }
        }
        List<StoragePoolInfoRecord> storagePoolInfoRecords = storagePoolInfoAccessor.getAllStoragePoolInfoRecord();
        for (int i = 0; i < storagePoolInfoRecords.size(); i++) {
            String undeletableDnId = storagePoolInfoRecords.get(i).undeletableDnId;
            if (StringUtils.isNotEmpty(undeletableDnId) && !allDefaultGroupSet.contains(undeletableDnId)) {
                allNonDeletableStorageInstIdSet.add(undeletableDnId);
            }
        }

        if (!DbTopologyManager.singleGroupStorageInstList.isEmpty()) {
            List<String> singleGrpStorageInstListConfig = DbTopologyManager.singleGroupStorageInstList;
            for (int i = 0; i < singleGrpStorageInstListConfig.size(); i++) {
                String targetDnId = singleGrpStorageInstListConfig.get(i);
                if (!allNonDeletableStorageInstIdSet.contains(targetDnId)) {
                    allNonDeletableStorageInstIdSet.add(targetDnId);
                }
            }
        }
        return allNonDeletableStorageInstIdSet;
    }

    private static void fileStoreDestroy(String schemaName, long ts) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            tableInfoManager.unBindingByArchiveSchemaName(schemaName);
            List<FilesRecord> filesRecordList = tableInfoManager.queryFilesByLogicalSchema(schemaName);
            for (FilesRecord filesRecord : filesRecordList) {
                if (filesRecord.getCommitTs() != null && filesRecord.getRemoveTs() == null
                    && filesRecord.fileName != null) {
                    FileStorageMetaStore fileStorageMetaStore =
                        new FileStorageMetaStore(Engine.of(filesRecord.getEngine()));
                    fileStorageMetaStore.setConnection(connection);
                    fileStorageMetaStore.updateFileRemoveTs(filesRecord.fileName, ts);
                }
            }
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public static String getDefaultCollationForCreatingDb() {
        return defaultCollationForCreatingDb;
    }

    public static void setDefaultCollationForCreatingDb(String newServerDefaultCollation) {
        DbTopologyManager.defaultCollationForCreatingDb = newServerDefaultCollation;

    }

    public static void checkRefForeignKeyWhenDropDatabase(String dbName) {
        // not allow to drop ref database first even check_foreign_key is off
        try (Connection metaConn = MetaDbDataSource.getInstance().getConnection()) {
            DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
            ForeignAccessor foreignAccessor = new ForeignAccessor();
            dbInfoAccessor.setConnection(metaConn);
            foreignAccessor.setConnection(metaConn);
            if (dbInfoAccessor.getDbInfoByDbNameForUpdate(dbName) != null) {
                List<ForeignRecord> fks = foreignAccessor.queryReferencedForeignKeys(dbName);
                for (ForeignRecord fk : fks) {
                    String schemaName = fk.schemaName;
                    if (!schemaName.equalsIgnoreCase(dbName)) {
                        String tableName = fk.tableName;
                        String constraint = fk.constraintName;
                        throw new TddlRuntimeException(ErrorCode.ERR_DROP_TABLE_FK_CONSTRAINT,
                            fk.refTableName, constraint, schemaName, tableName);
                    }
                }

            }
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static void refreshGroupKeysIntoTopologyMapping(List<String> groupKeysToBeAdded,
                                                           List<String> groupKeysToBeRemoved,
                                                           String schemaName) {
        try {
            groupTopologyMappingLock.lock();
            Map<String, String> newGroupTopologyMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            newGroupTopologyMapping.putAll(DbTopologyManager.groupTopologyMapping);
            for (String grp : groupKeysToBeAdded) {
                newGroupTopologyMapping.put(grp, schemaName);
            }
            for (String grp : groupKeysToBeRemoved) {
                newGroupTopologyMapping.remove(grp);
            }
            DbTopologyManager.groupTopologyMapping = newGroupTopologyMapping;
        } finally {
            groupTopologyMappingLock.unlock();
        }
    }

    public static void unregisterGroupKeysIntoTopologyMapping(List<String> groupKeys) {
        try {
            groupTopologyMappingLock.lock();
            Map<String, String> newGroupTopologyMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            newGroupTopologyMapping.putAll(DbTopologyManager.groupTopologyMapping);
            for (String grp : groupKeys) {
                newGroupTopologyMapping.remove(grp);
            }
            DbTopologyManager.groupTopologyMapping = newGroupTopologyMapping;
        } finally {
            groupTopologyMappingLock.unlock();
        }
    }

    public static String getDbNameByGroupKey(String groupKey) {
        Map<String, String> groupTopologyMapping = DbTopologyManager.groupTopologyMapping;
        return groupTopologyMapping.get(groupKey);
    }

}
