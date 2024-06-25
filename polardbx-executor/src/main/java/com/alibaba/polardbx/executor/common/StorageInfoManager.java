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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Group.GroupType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.GLOBAL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.MDL_DEADLOCK;
import static com.alibaba.polardbx.executor.utils.transaction.DeadlockParser.NO_DEADLOCKS_DETECTED;

/**
 * @author chenmo.cm
 */
public class StorageInfoManager extends AbstractLifecycle {
    private static final Logger logger = LoggerFactory.getLogger(StorageInfoManager.class);

    private final Map<String, StorageInfo> storageInfos;
    private final TopologyHandler topologyHandler;
    private volatile boolean supportXA;
    private volatile boolean supportTso;
    private volatile boolean supportTsoHeartbeat;
    private volatile boolean supportPurgeTso;
    private volatile boolean supportCtsTransaction;
    private volatile boolean supportAsyncCommit;
    private volatile boolean supportLizard1PCTransaction;
    private volatile boolean supportDeadlockDetection;
    private volatile boolean supportMdlDeadlockDetection;
    private volatile boolean supportsBloomFilter;
    private volatile boolean supportOpenSSL;
    private volatile boolean supportSharedReadView;
    private volatile boolean supportsReturning;
    private volatile boolean supportsBackfillReturning;
    private volatile boolean supportsAlterType;
    private boolean readOnly;
    private boolean lowerCaseTableNames;
    private volatile boolean supportHyperLogLog;
    private volatile boolean lessMy56Version;
    private boolean supportXxHash;
    private volatile boolean isMysql80;

    /**
     * FastChecker: generate checksum on xdb node
     * Since: 5.4.12 fix
     * Requirement: XDB supports HASHCHECK function
     */
    /**
     * Record the latest global deadlock log and global MDL deadlock log
     */
    private static final ConcurrentMap<String, String> deadlockLogMap;

    static {
        deadlockLogMap = new ConcurrentHashMap<>(2);
        deadlockLogMap.put(GLOBAL_DEADLOCK, NO_DEADLOCKS_DETECTED);
        deadlockLogMap.put(MDL_DEADLOCK, NO_DEADLOCKS_DETECTED);
    }

    /**
     * FastChecker: generate checksum on xdb node
     * Since: 5.4.13 fix
     * Requirement: XDB supports HASHCHECK function
     */
    private volatile boolean supportFastChecker = false;

    private volatile boolean supportChangeSet = false;

    private volatile boolean supportXOptForAutoSp = false;

    private volatile boolean supportXRpc = false;

    private volatile boolean supportMarkDistributed = false;

    private volatile boolean supportXOptForPhysicalBackfill = false;

    private volatile boolean support2pcOpt = false;

    public StorageInfoManager(TopologyHandler topologyHandler) {
        storageInfos = new ConcurrentHashMap<>();
        supportXA = false;
        supportsBloomFilter = false;
        supportsReturning = false;
        supportsBackfillReturning = false;

        Preconditions.checkNotNull(topologyHandler);
        this.topologyHandler = topologyHandler;
    }

    /**
     * Get deadlock information
     */
    public static String getDeadlockInfo() {
        return deadlockLogMap.get(GLOBAL_DEADLOCK);
    }

    /**
     * Get MDL deadlock information
     */
    public static String getMdlDeadlockInfo() {
        return deadlockLogMap.get(MDL_DEADLOCK);
    }

    /**
     * Update deadlock information
     */
    public static void updateDeadlockInfo(String newDeadlockInfo) {
        deadlockLogMap.put(GLOBAL_DEADLOCK, newDeadlockInfo);
    }

    /**
     * Update MDL deadlock information
     */
    public static void updateMdlDeadlockInfo(String newMdlDeadlockInfo) {
        deadlockLogMap.put(MDL_DEADLOCK, newMdlDeadlockInfo);
    }

    public static String getMySqlVersion(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT @@version")) {
            boolean hasNext = rs.next();
            assert hasNext;
            return rs.getString(1);
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to get MySQL version: " + ex.getMessage());
        }
    }

    public static boolean checkSupportTso(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_commit_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportTsoHeartbeat(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_heartbeat_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportPurgeTso(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_purge_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check TSO support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportCtsTransaction(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_cts_transaction'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check innodb_cts_transaction support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportLizard1PCTransaction(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_current_snapshot_seq'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check innodb_cts_transaction support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportAsyncCommit(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'polarx_distributed_trx_id'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS, ex,
                "Failed to check async commit support: " + ex.getMessage());
        }
    }

    public static boolean checkIsXEngine(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'xengine_datadir'")) {
            boolean hasNext = rs.next();
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex, "Failed to check xengine: " + ex.getMessage());
        }
    }

    public static boolean checkSupportPerformanceSchema(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'performance_schema'")) {
            boolean hasNext = rs.next();
            return hasNext && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check performance_schema support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportSharedReadView(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_transaction_group'")) {
            boolean hasNext = rs.next();
            // 该变量只需要存在就支持，默认为OFF
            return hasNext;
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check shared read view support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportReturning(DataSource dataSource) {
        if (XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportReturning = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportReturning |= "dbms_trans".equalsIgnoreCase(schemaName) && "returning".equalsIgnoreCase(procName);
                if (supportReturning) {
                    break;
                }
            }
            return supportReturning;
        } catch (SQLException ex) {
            final boolean ER_SP_DOES_NOT_EXIST =
                "42000".equalsIgnoreCase(ex.getSQLState()) && 1305 == ex.getErrorCode() && ex.getMessage()
                    .contains("does not exist");
            if (ER_SP_DOES_NOT_EXIST) {
                logger.warn("PROCEDURE dbms_admin.show_native_procedure does not exist");
                return false;
            }

            final boolean ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 3130 == ex.getErrorCode() && ex.getMessage()
                    .contains("Command not supported by pluggable protocols");
            if (ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED) {
                logger.warn("Do not support call dbms_amdin procedures within XPotocol");
                return false;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check returning support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportBackfillReturning(DataSource dataSource) {
        if (!ConfigDataMode.isPolarDbX() || XConfig.GALAXY_X_PROTOCOL) {
            return false;
        }
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_admin.show_native_procedure()")) {
            boolean supportReturning = false;
            while (rs.next()) {
                final String schemaName = rs.getString(1);
                final String procName = rs.getString(2);
                supportReturning |= "dbms_trans".equalsIgnoreCase(schemaName) && "backfill".equalsIgnoreCase(procName);
                if (supportReturning) {
                    break;
                }
            }
            return supportReturning;
        } catch (SQLException ex) {
            final boolean ER_SP_DOES_NOT_EXIST =
                "42000".equalsIgnoreCase(ex.getSQLState()) && 1305 == ex.getErrorCode() && ex.getMessage()
                    .contains("does not exist");
            if (ER_SP_DOES_NOT_EXIST) {
                logger.warn("PROCEDURE dbms_admin.show_native_procedure does not exist");
                return false;
            }

            final boolean ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 3130 == ex.getErrorCode() && ex.getMessage()
                    .contains("Command not supported by pluggable protocols");
            if (ER_PLUGGABLE_PROTOCOL_COMMAND_NOT_SUPPORTED) {
                logger.warn("Do not support call dbms_amdin procedures within XPotocol");
                return false;
            }

            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check returning support: " + ex.getMessage());
        }
    }

    public static boolean checkSupportAlterType(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select alter_type(1)")) {
            return true;
        } catch (SQLException ex) {
            final boolean INCORRECT_ARGS =
                "HY000".equalsIgnoreCase(ex.getSQLState()) && 1210 == ex.getErrorCode() && ex.getMessage()
                    .contains("Incorrect arguments to alter_type");
            if (INCORRECT_ARGS) {
                return true;
            }
            return false;
        }
    }

    public static boolean checkRDS80(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select version()")) {
            if (rs.next() && rs.getString(1).startsWith("8.0")) {
                return true;
            }
        } catch (SQLException ex) {
        }
        return false;
    }

    public static boolean checkMetaDataLocksSelectPrivilege(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            stmt.executeQuery("SELECT\n"
                + "  `pt`.`PROCESSLIST_ID` AS `waiting`,\n"
                + "  `gt`.`PROCESSLIST_ID` AS `blocking` \n"
                + "FROM\n"
                + "  (((((\n"
                + "            `performance_schema`.`metadata_locks` `g`\n"
                + "            JOIN `performance_schema`.`metadata_locks` `p` ON (((\n"
                + "                  `g`.`OBJECT_TYPE` = `p`.`OBJECT_TYPE` \n"
                + "                  ) \n"
                + "                AND ( `g`.`OBJECT_SCHEMA` = `p`.`OBJECT_SCHEMA` ) \n"
                + "                AND ( `g`.`OBJECT_NAME` = `p`.`OBJECT_NAME` ) \n"
                + "                AND ( `g`.`LOCK_STATUS` = 'GRANTED' ) \n"
                + "              AND ( `p`.`LOCK_STATUS` = 'PENDING' ))))\n"
                + "          JOIN `performance_schema`.`threads` `gt` ON ((\n"
                + "              `g`.`OWNER_THREAD_ID` = `gt`.`THREAD_ID` \n"
                + "            )))\n"
                + "        JOIN `performance_schema`.`threads` `pt` ON ((\n"
                + "            `p`.`OWNER_THREAD_ID` = `pt`.`THREAD_ID` \n"
                + "          )))\n"
                + "      LEFT JOIN `performance_schema`.`events_statements_current` `gs` ON ((\n"
                + "          `g`.`OWNER_THREAD_ID` = `gs`.`THREAD_ID` \n"
                + "        )))\n"
                + "    LEFT JOIN `performance_schema`.`events_statements_current` `ps` ON ((\n"
                + "        `p`.`OWNER_THREAD_ID` = `ps`.`THREAD_ID` \n"
                + "      ))) \n"
                + "WHERE\n"
                + "  ( `g`.`OBJECT_TYPE` = 'TABLE' ) \n"
                + "  AND `pt`.`PROCESSLIST_ID` != `gt`.`PROCESSLIST_ID`"
                + "  AND FALSE"
            );
            return true;
        } catch (SQLException ex) {
            logger.error("Failed to check performance_schema select privilege: " + ex.getMessage());
            return false;
        }
    }

    public static boolean checkMetaDataLocksEnable(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select enabled,timed from performance_schema.setup_instruments "
                    + "WHERE NAME = 'wait/lock/metadata/sql/mdl' ")) {
            boolean hasNext = rs.next();
            return hasNext && StringUtils.equalsIgnoreCase(rs.getString(1), "YES");
        } catch (SQLException ex) {
            logger.error("Failed to check performance_schema.metadata_locks: " + ex.getMessage());
            return false;
        }
    }

    /**
     * @see <a href="https://dev.mysql.com/doc/refman/5.7/en/server-status-variables.html#statvar_Rsa_public_key">Rsa_public_key</a>
     */
    public static boolean checkSupportOpenSSL(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW STATUS LIKE 'Rsa_public_key'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check openssl support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXxHash(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'udf_bloomfilter_xxhash'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check  support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXOptForAutoSp(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'auto_savepoint_opt'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-protocol optimized for auto savepoint support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXRpc(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'new_rpc'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-rpc support: " + ex.getMessage());
        }
    }

    private static boolean checkSupportMarkDistributed(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'innodb_mark_distributed'")) {
            return rs.next();
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check innodb_mark_distributed support: " + ex.getMessage());
        }
    }

    private static boolean checkSupport2pcOpt(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("call dbms_xa.find_by_xid('1', '', 1)")) {
            return true;
        } catch (SQLException ex) {
            logger.warn("Get support 2pc opt failed.", ex);
            // Not support.
            return false;
        }
    }

    public static int getLowerCaseTableNames(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT @@global.lower_case_table_names;")) {
            boolean hasNext = rs.next();
            assert hasNext;
            return rs.getInt(1);
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to get variable lower_case_table_names: " + ex.getMessage());
        }
    }

    private static boolean checkSupportXOptForPhysicalBackfill(IDataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW VARIABLES LIKE 'physical_backfill_opt'")) {
            return rs.next() && StringUtils.equalsIgnoreCase(rs.getString(2), "ON");
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_OTHER, ex,
                "Failed to check x-protocol for physical backfill support: " + ex.getMessage());
        }
    }

    @Override
    protected void doInit() {
        boolean tmpSupportXA = true;
        boolean tmpSupportTso = true;
        boolean tmpSupportTsoHeartbeat = true;
        boolean tmpSupportPurgeTso = true;
        boolean tmpSupportDeadlockDetection = true;
        boolean tmpSupportMdlDeadlockDetection = true;
        boolean tmpSupportsBloomFilter = true;
        boolean tmpSupportsReturning = true;
        boolean tmpSupportsBackfillReturning = true;
        boolean tmpSupportsAlterType = true;
        boolean tmpLowerCaseTableNames = true;
        boolean tmpSupportOpenSSL = true;
        boolean tmpSupportSharedReadView = true;
        boolean tmpSupportCtsTransaction = true;
        boolean tmpSupportAsyncCommit = true;
        boolean tmpSupportLizard1PCTransaction = true;
        boolean tmpSupportHyperLogLog = true;
        boolean tmpSupportXxHash = true;
        boolean lessMysql56 = false;
        boolean tmpSupportFastChecker = true;
        boolean tmpRDS80 = true;
        boolean tmpSupportChangeSet = true;
        boolean tmpSupportXOptForAutoSp = true;
        boolean tmpSupportXRpc = true;
        boolean tmpSupportXOptForPhysicalBackfill = true;
        boolean tmpSupportMarkDistributed = true;

        boolean storageInfoEmpty = true;
        boolean tmpSupport2pcOpt = true;
        for (Group group : topologyHandler.getMatrix().getGroups()) {
            if (group.getType() != GroupType.MYSQL_JDBC || !DbGroupInfoManager.isNormalGroup(group)) {
                continue;
            }

            IGroupExecutor groupExecutor = topologyHandler.get(group.getName());

            final StorageInfo storageInfo = initStorageInfo(group, groupExecutor.getDataSource());
            if (storageInfo != null) {
                storageInfoEmpty = false;
                tmpSupportXA &= supportXA(storageInfo);
                lessMysql56 = lessMysql56 || lessMysql56Version(storageInfo);
                tmpSupportTso &= storageInfo.supportTso;
                tmpSupportTsoHeartbeat &= storageInfo.supportTsoHeartbeat;
                tmpSupportPurgeTso &= storageInfo.supportPurgeTso;
                tmpSupportCtsTransaction &= storageInfo.supportCtsTransaction;
                tmpSupportAsyncCommit &= storageInfo.supportAsyncCommit;
                tmpSupportLizard1PCTransaction &= storageInfo.supportLizard1PCTransaction;
                tmpSupportDeadlockDetection &= supportDeadlockDetection(storageInfo);
                tmpSupportMdlDeadlockDetection &= supportMdlDeadlockDetection(storageInfo);
                tmpSupportsBloomFilter &= storageInfo.supportsBloomFilter;
                tmpSupportOpenSSL &= storageInfo.supportOpenSSL;
                tmpSupportHyperLogLog &= storageInfo.supportHyperLogLog;
                tmpSupportsReturning &= storageInfo.supportsReturning;
                tmpSupportsBackfillReturning &= storageInfo.supportsBackfillReturning;
                tmpSupportsAlterType &= storageInfo.supportsAlterType;
                tmpLowerCaseTableNames &= enableLowerCaseTableNames(storageInfo);
                tmpSupportSharedReadView &= storageInfo.supportSharedReadView;
                tmpSupportFastChecker &= storageInfo.supportFastChecker;
                tmpRDS80 &= isRDS80(storageInfo);
                tmpSupportXxHash &= storageInfo.supportXxHash;
                tmpSupportChangeSet &= storageInfo.supportChangeSet;
                tmpSupportXOptForAutoSp &= storageInfo.supportXOptForAutoSp;
                tmpSupportXRpc &= storageInfo.supportXRpc;
                tmpSupportXOptForPhysicalBackfill &= storageInfo.supportXOptForPhysicalBackfill;
                tmpSupportMarkDistributed &= storageInfo.supportMarkDistributed;
                tmpSupport2pcOpt &= storageInfo.support2pcOpt;
            }
        }

        this.readOnly = !ConfigDataMode.needInitMasterModeResource() && !ConfigDataMode.isFastMock();

        // Do not enable XA transaction in read-only instance
        this.supportXA = tmpSupportXA && !readOnly;
        this.supportsBloomFilter = tmpSupportsBloomFilter;
        this.supportsReturning = tmpSupportsReturning;
        this.supportsBackfillReturning = tmpSupportsBackfillReturning;
        this.supportsAlterType = tmpSupportsAlterType;
        this.supportTso = tmpSupportTso && (metaDbUsesXProtocol() || tmpRDS80);
        this.supportTsoHeartbeat = tmpSupportTsoHeartbeat && metaDbUsesXProtocol();
        this.supportPurgeTso = tmpSupportPurgeTso && metaDbUsesXProtocol();
        this.supportCtsTransaction = tmpSupportCtsTransaction;
        this.supportAsyncCommit = tmpSupportAsyncCommit;
        this.supportLizard1PCTransaction = tmpSupportLizard1PCTransaction;
        this.supportSharedReadView = tmpSupportSharedReadView;
        this.supportDeadlockDetection = tmpSupportDeadlockDetection;
        this.supportMdlDeadlockDetection = tmpSupportMdlDeadlockDetection;
        this.supportOpenSSL = tmpSupportOpenSSL;
        this.lowerCaseTableNames = tmpLowerCaseTableNames;
        this.supportHyperLogLog = tmpSupportHyperLogLog;
        this.lessMy56Version = lessMysql56;
        this.supportFastChecker = tmpSupportFastChecker;
        this.supportXxHash = tmpSupportXxHash;
        this.isMysql80 = tmpRDS80;
        this.supportChangeSet = tmpSupportChangeSet;
        this.supportXOptForAutoSp = tmpSupportXOptForAutoSp && tmpSupportXRpc;
        this.supportXRpc = tmpSupportXRpc;
        this.supportXOptForPhysicalBackfill = tmpSupportXOptForPhysicalBackfill && tmpSupportXRpc;
        this.supportMarkDistributed = tmpSupportMarkDistributed;
        this.support2pcOpt = tmpSupport2pcOpt;

        if (!storageInfoEmpty) {
            InstanceVersion.setMYSQL80(this.isMysql80);
        }
    }

    private boolean metaDbUsesXProtocol() {
        try {
            return MetaDbDataSource.getInstance().getDataSource().isWrapperFor(XDataSource.class);
        } catch (SQLException ex) {
            return false;
        }
    }

    private boolean isRDS80(StorageInfo storageInfo) {
        return storageInfo.version.startsWith("8.0");
    }

    private boolean supportXA(StorageInfo storageInfo) {
        return null == storageInfo
            || (!storageInfo.version.startsWith("5.6") && !storageInfo.version.startsWith("5.5")
            && !storageInfo.isXEngine);
    }

    private boolean lessMysql56Version(StorageInfo storageInfo) {
        return null != storageInfo
            && (storageInfo.version.startsWith("5.6") || storageInfo.version.startsWith("5.5"));
    }

    private boolean supportDeadlockDetection(StorageInfo storageInfo) {
        return null == storageInfo || storageInfo.version.startsWith("5.");
    }

    private boolean supportMdlDeadlockDetection(StorageInfo storageInfo) {
        return null == storageInfo ||
            storageInfo.supportPerformanceSchema
                && storageInfo.hasMetaDataLocksSelectPrivilege
                && storageInfo.isMetaDataLocksEnable;
    }

    private boolean enableLowerCaseTableNames(StorageInfo storageInfo) {
        return null == storageInfo || storageInfo.lowerCaseTableNames != 0;
    }

    @Override
    protected void doDestroy() {
        storageInfos.clear();
        supportXA = false;
        supportsBloomFilter = false;
        supportsReturning = false;
        supportsBackfillReturning = false;
    }

    private StorageInfo initStorageInfo(Group group, IDataSource dataSource) {

        if (!ConfigDataMode.needDNResource() && !SystemDbHelper.isDBBuildInExceptCdc(group.getSchemaName())) {
            return null;
        }

        if (group.getType() != GroupType.MYSQL_JDBC) {
            return null;
        }
        StorageInfo storageInfo = StorageInfo.create(dataSource);
        storageInfos.put(group.getName(), storageInfo);

        return storageInfo;
    }

    public boolean supportXA() {
        if (!isInited()) {
            init();
        }

        return supportXA;
    }

    public boolean supportTso() {
        if (!isInited()) {
            init();
        }

        return supportTso;
    }

    public boolean supportPurgeTso() {
        if (!isInited()) {
            init();
        }

        return supportPurgeTso;
    }

    public boolean isLessMy56Version() {
        if (!isInited()) {
            init();
        }

        return lessMy56Version;
    }

    public boolean isMysql80() {
        if (!isInited()) {
            init();
        }

        return isMysql80;
    }

    public String getDnVersion() {
        if (!isInited()) {
            init();
        }
        String version = null;
        Iterator<StorageInfo> iterator = storageInfos.values().iterator();
        if (iterator.hasNext()) {
            StorageInfo storageInfo = iterator.next();
            version = storageInfo.version;
        }
        return version;
    }

    public boolean supportTsoHeartbeat() {
        if (!isInited()) {
            init();
        }

        return supportTsoHeartbeat;
    }

    public boolean supportCtsTransaction() {
        if (!isInited()) {
            init();
        }

        return supportCtsTransaction;
    }

    public boolean supportAsyncCommit() {
        if (!isInited()) {
            init();
        }

        return supportAsyncCommit;
    }

    public boolean supportLizard1PCTransaction() {
        if (!isInited()) {
            init();
        }

        return supportLizard1PCTransaction;
    }

    public boolean supportDeadlockDetection() {
        if (!isInited()) {
            init();
        }

        return supportDeadlockDetection;
    }

    public boolean supportMdlDeadlockDetection() {
        if (!isInited()) {
            init();
        }
        return supportMdlDeadlockDetection;
    }

    public boolean supportsBloomFilter() {
        if (!isInited()) {
            init();
        }

        return supportsBloomFilter;
    }

    public boolean supportSharedReadView() {
        if (!isInited()) {
            init();
        }

        return supportSharedReadView;
    }

    public boolean supportOpenSSL() {
        if (!isInited()) {
            init();
        }

        return supportOpenSSL;
    }

    public boolean supportsHyperLogLog() {
        if (!isInited()) {
            init();
        }

        return supportHyperLogLog;
    }

    public boolean supportsXxHash() {
        if (!isInited()) {
            init();
        }

        return supportXxHash;
    }

    public boolean supportsReturning() {
        if (!isInited()) {
            init();
        }

        return supportsReturning;
    }

    public boolean supportsBackfillReturning() {
        if (!isInited()) {
            init();
        }

        return supportsBackfillReturning;
    }

    public boolean supportsAlterType() {
        if (!isInited()) {
            init();
        }

        return supportsAlterType;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isLowerCaseTableNames() {
        return lowerCaseTableNames;
    }

    public boolean supportFastChecker() {
        if (!isInited()) {
            init();
        }
        return supportFastChecker;
    }

    public boolean supportChangeSet() {
        if (!isInited()) {
            init();
        }
        return supportChangeSet;
    }

    public boolean supportXOptForAutoSp() {
        if (!isInited()) {
            init();
        }
        return supportXOptForAutoSp;
    }

    public boolean supportXRpc() {
        if (!isInited()) {
            init();
        }
        return supportXRpc;
    }

    public boolean isSupportMarkDistributed() {
        if (!isInited()) {
            init();
        }
        return supportMarkDistributed;
    }

    public boolean supportXOptForPhysicalBackfill() {
        if (!isInited()) {
            init();
        }
        return supportXOptForPhysicalBackfill;
    }

    public boolean support2pcOpt() {
        if (!isInited()) {
            init();
        }
        return support2pcOpt;
    }

    public static class StorageInfo {

        public final String version;
        public final boolean supportTso;
        private volatile boolean supportTsoHeartbeat;
        public final boolean supportPurgeTso;
        public final boolean supportCtsTransaction;
        public final boolean supportAsyncCommit;
        public final boolean supportLizard1PCTransaction;
        public final boolean supportsBloomFilter;
        public final boolean supportsReturning;
        public final boolean supportsBackfillReturning;
        public final boolean supportsAlterType;
        public final int lowerCaseTableNames;
        public final boolean supportPerformanceSchema;
        public final boolean isXEngine;
        public final boolean supportSharedReadView;
        boolean hasMetaDataLocksSelectPrivilege;
        boolean isMetaDataLocksEnable;
        public final boolean supportOpenSSL;
        boolean supportHyperLogLog;
        boolean supportFastChecker;
        boolean supportXxHash;
        boolean supportChangeSet;
        boolean supportXOptForAutoSp;
        boolean supportXRpc;
        boolean supportXOptForPhysicalBackfill;
        boolean supportMarkDistributed;
        boolean support2pcOpt;

        public StorageInfo(
            String version,
            boolean supportTso,
            boolean supportTsoHeartbeat,
            boolean supportPurgeTso,
            boolean supportCtsTransaction,
            boolean supportAsyncCommit,
            boolean supportLizard1PCTransaction,
            boolean supportsBloomFilter,
            boolean supportsReturning,
            boolean supportsBackfillReturning,
            boolean supportsAlterType,
            int lowerCaseTableNames,
            boolean supportPerformanceSchema,
            boolean isXEngine,
            boolean supportSharedReadView,
            boolean hasMetaDataLocksSelectPrivilege,
            boolean isMetaDataLocksEnable,
            boolean supportHyperLogLog,
            boolean supportOpenSSL,
            boolean supportFastChecker,
            boolean supportXxHash,
            boolean supportChangeSet,
            boolean supportXOptForAutoSp,
            boolean supportXRpc,
            boolean supportXOptForPhysicalBackfill,
            boolean supportMarkDistributed,
            boolean support2pcOpt
        ) {
            this.version = version;
            this.supportTso = supportTso;
            this.supportTsoHeartbeat = supportTsoHeartbeat;
            this.supportPurgeTso = supportPurgeTso;
            this.supportCtsTransaction = supportCtsTransaction;
            this.supportAsyncCommit = supportAsyncCommit;
            this.supportLizard1PCTransaction = supportLizard1PCTransaction;
            this.supportsBloomFilter = supportsBloomFilter;
            this.supportsReturning = supportsReturning;
            this.supportsBackfillReturning = supportsBackfillReturning;
            this.supportsAlterType = supportsAlterType;
            this.lowerCaseTableNames = lowerCaseTableNames;
            this.supportPerformanceSchema = supportPerformanceSchema;
            this.isXEngine = isXEngine;
            this.supportSharedReadView = supportSharedReadView;
            this.hasMetaDataLocksSelectPrivilege = hasMetaDataLocksSelectPrivilege;
            this.isMetaDataLocksEnable = isMetaDataLocksEnable;
            this.supportOpenSSL = supportOpenSSL;
            this.supportHyperLogLog = supportHyperLogLog;
            this.supportFastChecker = supportFastChecker;
            this.supportXxHash = supportXxHash;
            this.supportChangeSet = supportChangeSet;
            this.supportXOptForAutoSp = supportXOptForAutoSp;
            this.supportXRpc = supportXRpc;
            this.supportXOptForPhysicalBackfill = supportXOptForPhysicalBackfill && supportXRpc;
            this.supportMarkDistributed = supportMarkDistributed;
            this.support2pcOpt = support2pcOpt;
        }

        public static StorageInfo create(IDataSource dataSource) {
            // mock storage version 5.7
            if (ConfigDataMode.isFastMock()) {
                return new StorageInfo(
                    "5.7",
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    1,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false,
                    false
                );
            }

            String version = getMySqlVersion(dataSource);
            boolean supportTso = checkSupportTso(dataSource);
            boolean supportTsoHeartbeat = checkSupportTsoHeartbeat(dataSource);
            boolean supportPurgeTso = checkSupportPurgeTso(dataSource);
            boolean supportPerformanceSchema = checkSupportPerformanceSchema(dataSource);
            boolean isXEngine = checkIsXEngine(dataSource);

            Optional<PolarxUDFInfo> polarxUDFInfo = PolarxUDFInfo.build(dataSource);
            boolean supportsBloomFilter = polarxUDFInfo.map(PolarxUDFInfo::supportsBloomFilter).orElse(false);
            boolean supportsReturning = checkSupportReturning(dataSource);
            boolean supportsBackfillReturning = checkSupportBackfillReturning(dataSource);
            boolean supportsAlterType = checkSupportAlterType(dataSource);
            boolean supportCtsTransaction = checkSupportCtsTransaction(dataSource);
            boolean supportAsyncCommit = checkSupportAsyncCommit(dataSource);
            boolean supportLizard1PCTransaction = checkSupportLizard1PCTransaction(dataSource);
            final int lowerCaseTableNames = getLowerCaseTableNames(dataSource);
            final boolean supportSharedReadView = checkSupportSharedReadView(dataSource);

            boolean hasMetaDataLocksSelectPrivilege = checkMetaDataLocksSelectPrivilege(dataSource);
            boolean isMetaDataLocksEnable = checkMetaDataLocksEnable(dataSource);
            boolean supportHyperLogLog = polarxUDFInfo.map(PolarxUDFInfo::supportsHyperLogLog).orElse(false);
            boolean supportOpenSSL = checkSupportOpenSSL(dataSource);
            boolean supportFastChecker = polarxUDFInfo.map(PolarxUDFInfo::supportFastChecker).orElse(false);
            boolean supportXxHash = checkSupportXxHash(dataSource);
            boolean supportChangeSet = polarxUDFInfo.map(PolarxUDFInfo::supportChangeSet).orElse(false);
            boolean supportXOptForAutoSp = checkSupportXOptForAutoSp(dataSource);
            boolean supportXRpc = checkSupportXRpc(dataSource);
            boolean supportXoptForPhysicalBackfill = checkSupportXOptForPhysicalBackfill(dataSource);
            boolean supportMarkDistributed = checkSupportMarkDistributed(dataSource);
            boolean support2pcOpt = checkSupport2pcOpt(dataSource);

            return new StorageInfo(
                version,
                supportTso,
                supportPurgeTso,
                supportTsoHeartbeat,
                supportCtsTransaction,
                supportAsyncCommit,
                supportLizard1PCTransaction,
                supportsBloomFilter,
                supportsReturning,
                supportsBackfillReturning,
                supportsAlterType,
                lowerCaseTableNames,
                supportPerformanceSchema,
                isXEngine,
                supportSharedReadView,
                hasMetaDataLocksSelectPrivilege,
                isMetaDataLocksEnable,
                supportHyperLogLog,
                supportOpenSSL,
                supportFastChecker,
                supportXxHash,
                supportChangeSet,
                supportXOptForAutoSp,
                supportXRpc,
                supportXoptForPhysicalBackfill,
                supportMarkDistributed,
                support2pcOpt);
        }
    }

    public static class PolarxUDFInfo {
        private static final String STATUS_ACTIVE = "ACTIVE";

        private static final String PLUGIN_NAME = "polarx_udf";
        private static final String VAR_FUNCTION_LIST = "polarx_udf_function_list";
        private static final String UDF_BLOOM_FILTER = "bloomfilter";
        private static final String UDF_HYPERLOGLOG = "hyperloglog";
        private static final String UDF_HASHCHECK = "hashcheck";
        private static final String UDF_CHANGESET = "changeset";
        private static final String VAR_CHANGESET = "enable_changeset";

        private final int majorVersion;
        private final int minorVersion;
        private final String status;
        private final Set<String> functions;

        private PolarxUDFInfo(int majorVersion, int minorVersion, String status,
                              Set<String> functions) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.status = status;
            this.functions = functions;
        }

        public static Optional<PolarxUDFInfo> build(DataSource dataSource) {
            try (Connection conn = dataSource.getConnection();
                Statement stmt = conn.createStatement()) {
                String pluginSql = "select `PLUGIN_VERSION`, `PLUGIN_STATUS` "
                    + " from information_schema.plugins "
                    + " where `PLUGIN_NAME` = '" + PLUGIN_NAME + "';";

                int[] versionParts = new int[2];
                String status;
                Set<String> udfFunctions = new HashSet<>();
                try (ResultSet rs = stmt.executeQuery(pluginSql)) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    if (!parseVersion(rs.getString(1), versionParts)) {
                        return Optional.empty();
                    }
                    status = rs.getString(2);
                }

                String changesetSql = "SHOW VARIABLES LIKE '" + VAR_CHANGESET + "';";
                try (ResultSet rs = stmt.executeQuery(changesetSql)) {
                    if (rs.next()) {
                        udfFunctions.add(UDF_CHANGESET);
                    }
                } catch (SQLException e) {
                    logger.error("dn do not support changeset produce");
                }

                String functionListSql = "SHOW VARIABLES LIKE '" + VAR_FUNCTION_LIST + "';";
                try (ResultSet rs = stmt.executeQuery(functionListSql)) {
                    if (!rs.next()) {
                        return Optional.empty();
                    }

                    String functionListString = rs.getString(2);
                    udfFunctions.addAll(Arrays.stream(functionListString.split(",")).collect(Collectors.toList()));
                }

                return Optional.of(new PolarxUDFInfo(versionParts[0], versionParts[1], status, udfFunctions));
            } catch (Exception ex) {
                logger.warn("Failed to check polar udf info", ex);
                return Optional.empty();
            }
        }

        private static boolean parseVersion(String versionString, int[] version) {
            String[] parts = versionString.split("\\.");
            if (parts.length < 2) {
                return false;
            }

            try {
                version[0] = Integer.parseInt(parts[0]);
                version[1] = Integer.parseInt(parts[1]);
                return true;
            } catch (Exception e) {
                logger.warn("Failed to parse polarx udf version string: " + versionString);
                return false;
            }
        }

        public boolean supportsBloomFilter() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_BLOOM_FILTER);
        }

        public boolean supportsHyperLogLog() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_HYPERLOGLOG);
        }

        public boolean supportFastChecker() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_HASHCHECK);
        }

        public boolean supportChangeSet() {
            return majorVersion >= 1
                && minorVersion >= 1
                && STATUS_ACTIVE.equals(status)
                && functions.contains(UDF_CHANGESET);
        }
    }
}
