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

package com.alibaba.polardbx.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cdc.entity.LogicMeta;
import com.alibaba.polardbx.cdc.entity.StorageChangeEntity;
import com.alibaba.polardbx.cdc.entity.StorageRemoveRequest;
import com.alibaba.polardbx.common.cdc.CdcDDLContext;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.cdc.TableMode;
import com.alibaba.polardbx.common.cdc.TablesExtInfo;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord;
import com.alibaba.polardbx.gms.metadb.limit.LimitValidator;
import com.alibaba.polardbx.gms.metadb.table.TablesExtAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.partition.TablePartitionConfig;
import com.alibaba.polardbx.gms.partition.TablePartitionConfigUtil;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.VirtualTableRuleMatcher;
import com.alibaba.polardbx.rule.gms.TddlRuleGmsConfig;
import com.alibaba.polardbx.rule.model.Field;
import com.alibaba.polardbx.rule.model.MatcherResult;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.rmi.UnexpectedException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.CdcDbLock.acquireCdcDbLockByForUpdate;
import static com.alibaba.polardbx.cdc.CdcDbLock.releaseCdcDbLockByCommit;
import static com.alibaba.polardbx.cdc.CdcStorageUtil.isStorageContainsGroup;
import static com.alibaba.polardbx.cdc.MetaBuilder.checkLogicDbMeta;
import static com.alibaba.polardbx.cdc.MetaBuilder.checkLogicTableMeta;
import static com.alibaba.polardbx.cdc.SQLHelper.checkToString;
import static com.alibaba.polardbx.cdc.SQLHelper.filterColumns;
import static com.alibaba.polardbx.common.cdc.ICdcManager.InstructionType.StorageInstChange;
import static com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil.tryAttachImplicitTableGroup;
import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcSqlUtils.SQL_PARSE_FEATURES;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.INITIAL;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.READY;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_STATUS.SUCCESS;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_TYPE.ADD_STORAGE;
import static com.alibaba.polardbx.gms.metadb.cdc.BinlogCommandRecord.COMMAND_TYPE.REMOVE_STORAGE;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

/**
 * Cdc打标管理器实现类
 *
 * @author ziyang.lb 2020-12-05
 **/
@Activate(order = 1)
public class CdcManager extends AbstractLifecycle implements ICdcManager {

    private final static Logger logger = LoggerFactory.getLogger(CdcManager.class);

    /**
     * cdc的逻辑需要在独立的线程执行，避免误用调用线程的ThreadLocal变量
     */
    private final ExecutorService managerCoreExecutor;

    /**
     * 当Storage发生新增或移除时，需要感知到此变化，并变更CDC系统库的拓扑
     */
    private final ScheduledExecutorService checkStorageChangeExecutor;

    /**
     * 命令扫描执行器
     */
    private final ScheduledExecutorService commandScanExecutor;

    private final Set<String> orginalAcceptSet;

    public CdcManager() {
        managerCoreExecutor = Executors.newCachedThreadPool(
            new NamedThreadFactory("cdc-core-executor", true));
        checkStorageChangeExecutor = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("cdc-storage-check-executor", true));
        commandScanExecutor = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("cdc-command-scan-executor", true));
        orginalAcceptSet = Sets.newHashSet();
        orginalAcceptSet.addAll(Arrays.asList(SqlKind.CREATE_TABLE.name(),
            SqlKind.ALTER_TABLE.name(),
            SqlKind.DROP_TABLE.name(),
            SqlKind.CREATE_INDEX.name(),
            SqlKind.RENAME_TABLE.name(),
            SqlKind.ALTER_RENAME_INDEX.name(),
            SqlKind.DROP_INDEX.name()));
    }

    @Override
    protected void doInit() {
        if (ConfigDataMode.isPolarDbX() && ConfigDataMode.isMasterMode()) {
            CdcTableUtil.getInstance().prepareCdcSysTables();

            // init storage check scheduler
            final int checkStorageChangeInterval = 2000;
            checkStorageChangeExecutor.scheduleWithFixedDelay(() -> {
                try {
                    if (!ExecUtils.hasLeadership(null)) {
                        return;
                    }
                    synchronized (CdcManager.this) {
                        checkStorageChange();
                    }
                } catch (Throwable t) {
                    MetaDbLogUtil.META_DB_LOG.error(t);
                    logger.error("something goes wrong when do storage check for cdc db.", t);
                }
            }, checkStorageChangeInterval * 3, checkStorageChangeInterval, TimeUnit.MILLISECONDS);

            // init command scanner
            final int commandScanInterval = 2000;
            final CommandScanner commandScanner = new CommandScanner(this);
            commandScanner.init();
            commandScanExecutor.scheduleWithFixedDelay(() -> {
                try {
                    commandScanner.scan();
                } catch (Throwable t) {
                    MetaDbLogUtil.META_DB_LOG.error(t);
                    logger.error("something goes wrong when do storage check for cdc db.", t);
                }
            }, commandScanInterval * 3, commandScanInterval, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    protected void doDestroy() {
        if (managerCoreExecutor != null) {
            managerCoreExecutor.shutdownNow();
        }
        if (checkStorageChangeExecutor != null) {
            checkStorageChangeExecutor.shutdownNow();
        }
        if (commandScanExecutor != null) {
            commandScanExecutor.shutdownNow();
        }
    }

    @Override
    public void sendInstruction(InstructionType instructionType, String instructionId, String instructionContent) {
        if (isCdcDisabled()) {
            logger.warn("cdc is disabled , send instruction is skipped.");
            return;
        }

        Future<?> future = managerCoreExecutor.submit(() -> {
            CdcManager.this.checkState();
            synchronized (CdcManager.this) {
                sendInstructionInternal(instructionType, instructionId, instructionContent);
            }
        });

        try {
            future.get();
        } catch (Throwable t) {
            MetaDbLogUtil.META_DB_LOG.error(t);
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, t, t.getMessage());
        }
    }

    @Override
    public List<CdcDdlRecord> getDdlRecord(CdcDDLContext cdcDDLContext) {
        //init parameter
        final String schemaName = cdcDDLContext.getSchemaName();
        final String tableName = cdcDDLContext.getTableName();
        final String sqlKind = cdcDDLContext.getSqlKind();
        final Long jobId = cdcDDLContext.getJobId();
        final Long versionId = cdcDDLContext.getVersionId();

        final boolean jobIdSpecified = (null != jobId);

        try (Connection connection = prepareConnection()) {
            if (jobIdSpecified) {
                return queryDdlByJobId(connection, cdcDDLContext);
            } else {
                return queryDdl(connection, cdcDDLContext);
            }
        } catch (Throwable t) {
            String errorMsg = String.format(
                "get ddl record error , the detail info as below : \n"
                    + " schemaName is : %s \n"
                    + " tableName is : %s \n"
                    + " sqlKind is : %s \n "
                    + " versionId is : %s \n "
                    + " job is : %s ",
                schemaName == null ? "" : schemaName,
                tableName == null ? "" : tableName,
                sqlKind == null ? "" : sqlKind,
                versionId == null ? "" : versionId,
                jobId == null ? "" : jobId);
            logger.error(errorMsg, t);
            MetaDbLogUtil.META_DB_LOG.error(errorMsg, t);
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, t, t.getMessage());
        }
    }

    @Override
    public void notifyDdl(CdcDDLContext cdcDDLContext) {
        //init parameter
        String schemaName = cdcDDLContext.getSchemaName();
        String tableName = cdcDDLContext.getTableName();
        String sqlKind = cdcDDLContext.getSqlKind();
        String ddlSql = cdcDDLContext.getDdlSql();
        CdcDdlMarkVisibility visibility = cdcDDLContext.getVisibility();
        Map<String, Object> extendParams = cdcDDLContext.getExtendParams();

        // 对于if not exists 和 if exists，外部没有对长度进行判断，此处进行判断
        checkLength(schemaName, tableName);

        boolean isGSI = isDdlOnGsi(cdcDDLContext, extendParams);

        extendParams.put(CDC_IS_GSI, isGSI);

        // check if ignore
        if (!checkDdl(schemaName, ddlSql, cdcDDLContext, extendParams)) {
            return;
        }

        if (!orginalAcceptSet.contains(sqlKind)) {
            // 目前只考虑 table 和 索引的DDL使用原始SQL
            extendParams.put(CDC_ORIGINAL_DDL, "");
        }

        checkSleep(extendParams);

        Future<?> future = managerCoreExecutor.submit(() -> {
            CdcManager.this.checkState();
            synchronized (CdcManager.this) {
                int retry = 0;
                while (true) {
                    if (Thread.interrupted()) {
                        throw new RuntimeException("cdc ddl mark is interrupted");
                    }
                    try (Connection connection = prepareConnection()) {
                        // 虽然checkStorageChangeExecutor可以定时扫描Storage的变化，但这是一个异步操作，一旦Storage插入Storage_info表之后，就会
                        // 立即生效，所以，在checkStorageChangeExecutor扫描到变化之前，在新的Storage上可能已经发生了逻辑ddl和dml操作，所以这里加入
                        // 一个Barrier，每次ddl打标前，调用一下checkStorageChange方法，保证下游先收到storage的变化，再收到新的逻辑Schema变化
                        checkStorageChange();
                        recordDdl(connection, schemaName, tableName, sqlKind, ddlSql, cdcDDLContext, visibility,
                            extendParams);
                        break;
                    } catch (Throwable t) {
                        if (retry < 3) {
                            try {
                                Thread.sleep(1000);
                                retry++;
                            } catch (InterruptedException ignore) {
                            }
                        } else {
                            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, t, t.getMessage());
                        }
                    }
                }
            }
        });

        try {
            future.get();
        } catch (Throwable t) {
            String errorMsg = String.format("notify ddl error , the detail info as below : \n"
                    + " schemaName is : %s \n"
                    + " tableName is : %s \n"
                    + " sqlKind is : %s \n "
                    + " visibility is : %s \n"
                    + " ddlSql is : %s \n "
                    + " job is : %s \n "
                    + " extendParams is : %s", schemaName, tableName, sqlKind, visibility, ddlSql,
                cdcDDLContext.getJobId() == null ? "" : cdcDDLContext.getJobId(),
                JSONObject.toJSONString(extendParams));
            logger.error(errorMsg, t);
            MetaDbLogUtil.META_DB_LOG.error(errorMsg, t);
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, t, t.getMessage());
        }
    }

    @Override
    public void checkCdcBeforeStorageRemove(Set<String> storageInstIds, String identifier) {
        if (isCdcDisabled()) {
            logger.warn("cdc is disabled, check storage removing stage is skipped.");
            return;
        }
        CdcStorageUtil.checkCdcBeforeStorageRemove(storageInstIds, identifier);
    }

    private void checkState() {
        // 我们无法完全保证在CdcManager初始化的过程中，不会收到操作请求，但我们知道这种概率非常低，所以引入一个状态检测机制
        // 当发现Manager还处在初始化状态时，进行轮询等待，超过最大等待时间抛异常处理
        long startTime = System.currentTimeMillis();
        while (!isInited()) {
            try {
                logger.warn("Cdc manager is in initializing state, will wait and retry.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            if (System.currentTimeMillis() - startTime > 30 * 1000) {
                throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC,
                    "Wait for cdc manager to complete initialization timeout.");
            }
        }
    }

    private boolean checkDdl(String schemaName, String ddlSql, CdcDDLContext cdcDDLContext,
                             Map<String, Object> extendParams) {
        if (!ConfigDataMode.isMasterMode()) {
            logger.warn("notifyCdc method is not supported when server is not in master mode.");
            return false;
        }

        if (isCdcDisabled()) {
            logger.warn("cdc is disabled, ddl mark is ignored.");
            return false;
        }

        // cdc系统库表，不需要进行ddl打标，直接返回，该逻辑必须放到synchronized外面，否则会有死锁问题，描述如下：prepareTable方法内创建表
        // __drds_cdc_ddl_record__时，也会触发notifyDdl方法，如果该逻辑放到线程池内，将导致提交到线程池的两个任务相互等待(前一个任务的
        // prepareTable方法和后一个任务的notifyDdl方法相互等待)，出现死锁
        if (StringUtils.equalsIgnoreCase(schemaName, SystemDbHelper.CDC_DB_NAME)) {
            return false;
        }

        // 全局二级索引(即：GSI)，如果没有特殊标识，忽略打标
        Boolean isGSI = (Boolean) extendParams.get(CDC_IS_GSI);
        if (isGSI == null) {
            isGSI = false;
        }
        if (!extendParams.containsKey(ICdcManager.NOT_IGNORE_GSI_JOB_TYPE_FLAG) && isGSI) {
            logger.warn(String.format("ddl sql is related to global index, is ignored, ddl sql is : %s", ddlSql));
            return false;
        }
        SqlKind sqlKind = SqlKind.valueOf(cdcDDLContext.getSqlKind());

        if (sqlKind == SqlKind.FLUSH_LOGS) {
            return true;
        }

        if (StringUtils.isEmpty(ddlSql)) {
            logger.warn("ddl sql is empty, ddl job id is:" + cdcDDLContext.getJobId());
            return false;
        }

        SQLHelper.checkToString(ddlSql);

        // 存储引擎为OSS/S3等的表暂不支持同步
        if (StringUtils.isNotBlank(schemaName) && StringUtils.isNotBlank(cdcDDLContext.getTableName())
            && CdcTableUtil.getInstance().isFileStoreTable(schemaName, cdcDDLContext.getTableName())) {
            return false;
        }

        // 存储引擎为OSS/S3等的表暂不支持同步
        if (StringUtils.isNotBlank(schemaName) && StringUtils.isNotBlank(cdcDDLContext.getTableName())
            && CdcTableUtil.getInstance().isFileStoreTable(schemaName, cdcDDLContext.getTableName())) {
            return false;
        }

        // 针对mysql系统库，过滤
        if (StringUtils.equalsIgnoreCase(schemaName, "mysql") && StringUtils.equalsIgnoreCase("DROP_DATABASE",
            sqlKind.toString())) {
            return false;
        }

        return true;
    }

    private void checkSleep(Map<String, Object> extendParams) {
        Object value = extendParams.get(ConnectionProperties.SLEEP_TIME_BEFORE_NOTIFY_DDL);
        if (value != null) {
            int time = Integer.parseInt(value.toString());
            try {
                Thread.sleep(time * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

    private boolean isDdlOnGsi(CdcDDLContext cdcDDLContext, Map<String, Object> extendParams) {
        if (extendParams.containsKey(ConnectionProperties.DDL_ON_PRIMARY_GSI_TYPE)) {
            String type = (String) extendParams.get(ConnectionProperties.DDL_ON_PRIMARY_GSI_TYPE);
            return DdlConstants.TYPE_ON_GSI.equals(type);
        }

        DdlType ddlType = cdcDDLContext.getDdlType();
        if (cdcDDLContext.getJobId() != null && (ddlType == DdlType.CREATE_GLOBAL_INDEX
            || ddlType == DdlType.ALTER_GLOBAL_INDEX || ddlType == DdlType.DROP_GLOBAL_INDEX
            || ddlType == DdlType.RENAME_GLOBAL_INDEX || ddlType == DdlType.CHECK_GLOBAL_INDEX
            || ddlType == DdlType.CHECK_COLUMNAR_INDEX)) {
            return true;
        }

        Boolean isGsi = (Boolean) extendParams.get(ICdcManager.CDC_IS_GSI);
        if (isGsi != null && isGsi) {
            return true;
        }
        return isGsiTable(cdcDDLContext);
    }

    private boolean isGsiTable(CdcDDLContext cdcDDLContext) {
        String schemaName = cdcDDLContext.getSchemaName();
        String tableName = cdcDDLContext.getTableName();
        return CBOUtil.isGsi(schemaName, tableName);
    }

    private Connection prepareConnection() throws SQLException {
        return new InnerConnection(SystemDbHelper.CDC_DB_NAME);
    }

    private List<CdcDdlRecord> queryDdl(Connection connection, CdcDDLContext context) throws SQLException {
        return CdcTableUtil.getInstance().queryDdlRecord(
            connection,
            context.getSchemaName(),
            context.getTableName(),
            context.getSqlKind(),
            context.getVersionId());
    }

    private List<CdcDdlRecord> queryDdlByJobId(Connection connection, CdcDDLContext context) throws SQLException {
        return CdcTableUtil.getInstance().queryDdlRecordByJobId(
            connection,
            context.getJobId());
    }

    private void recordDdl(Connection connection, String schema, String tableName, String sqlKind, String ddlSql,
                           CdcDDLContext cdcDDLContext,
                           CdcDdlMarkVisibility visibility,
                           Map<String, Object> extendParams)
        throws SQLException {
        if (cdcDDLContext.getJobId() != null) {
            // 如果job不为空，则需要进行幂等判断，防止重复执行
            // 以下几种情形，job为空
            //  1. create database ...
            //  2. drop database ...
            //  3. create table if not exists ...
            //  4. drop table if exists ...
            boolean isDuplicateJobId = CdcTableUtil.getInstance().isDdlRecordExistForJobId(
                connection, getJobId(cdcDDLContext), cdcDDLContext.getTaskId(), getTaskSubSeq(extendParams));
            if (isDuplicateJobId) {
                logger.warn("ddl record for job_id " + getJobId(cdcDDLContext) + " is already existed, ignore it.");
                return;
            }
        }

        if (cdcDDLContext.getJobId() != null) {
            logger.warn("insert ddl record for job_id " + getJobId(cdcDDLContext));
        }

        DDLExtInfo extInfo = new DDLExtInfo();
        extInfo.setTaskId(cdcDDLContext.getTaskId());
        extInfo.setCreateSql4PhyTable(tryBuildCreateSql4PhyTable(schema, tableName, extendParams, cdcDDLContext));
        if (extendParams.containsKey(POLARDBX_SERVER_ID)) {
            extInfo.setServerId(extendParams.get(POLARDBX_SERVER_ID).toString());
        }
        if (extendParams.containsKey(USE_OMC)) {
            extInfo.setUseOMC(Boolean.parseBoolean(extendParams.get(USE_OMC).toString()));
        }
        if (extendParams.containsKey(ICdcManager.TASK_MARK_SEQ)) {
            extInfo.setTaskSubSeq(Long.parseLong(extendParams.get(ICdcManager.TASK_MARK_SEQ).toString()));
        }
        if (extendParams.containsKey(CDC_MARK_SQL_MODE)) {
            extInfo.setSqlMode(extendParams.get(CDC_MARK_SQL_MODE).toString());
        }
        if (extendParams.containsKey(CDC_ORIGINAL_DDL)) {
            extInfo.setOriginalDdl(extendParams.get(CDC_ORIGINAL_DDL).toString());
            if (StringUtils.isNotBlank(extInfo.getOriginalDdl())) {
                checkToString(extInfo.getOriginalDdl());
            }
        }
        if (extendParams.containsKey(CDC_IS_GSI)) {
            extInfo.setGsi((Boolean) extendParams.get(CDC_IS_GSI));
        }
        if (extendParams.containsKey(CDC_GROUP_NAME)) {
            extInfo.setGroupName(extendParams.get(CDC_GROUP_NAME).toString());
        }
        if (extendParams.containsKey(FOREIGN_KEYS_DDL)) {
            extInfo.setForeignKeysDdl(Boolean.parseBoolean(extendParams.get(FOREIGN_KEYS_DDL).toString()));
        }
        if (extendParams.containsKey(DDL_ID)) {
            extInfo.setDdlId(Long.parseLong(extendParams.get(DDL_ID).toString()));
        }

        StringBuilder flags2Builder = new StringBuilder();
        if (extendParams.containsKey(ConnectionParams.FOREIGN_KEY_CHECKS.getName()) && StringUtils.equalsAnyIgnoreCase(
            String.valueOf(extendParams.get(ConnectionParams.FOREIGN_KEY_CHECKS.getName())), "OFF", "0", "FALSE")) {
            flags2Builder.append("OPTION_NO_FOREIGN_KEY_CHECKS,");
        }
        final String uniqueCheckVariableName = "unique_checks";
        if (extendParams.containsKey(uniqueCheckVariableName) && StringUtils.equalsAnyIgnoreCase(
            String.valueOf(extendParams.get(uniqueCheckVariableName)), "OFF", "0", "FALSE")) {
            flags2Builder.append("OPTION_RELAXED_UNIQUE_CHECKS");
        }

        if (flags2Builder.length() > 0) {
            extInfo.setFlags2(flags2Builder.toString());
        }

        if (extendParams.containsKey(CDC_TABLE_GROUP_MANUAL_CREATE_FLAG)) {
            extInfo.setManuallyCreatedTableGroup((Boolean) extendParams.get(CDC_TABLE_GROUP_MANUAL_CREATE_FLAG));
        }

        if (extendParams.containsKey(CDC_DDL_SCOPE)) {
            extInfo.setDdlScope(((DdlScope) extendParams.get(CDC_DDL_SCOPE)).getValue());
        }

        tryAttachImplicitTableGroupInfo(schema, tableName, ddlSql, extInfo, extendParams);
        doFinalMark(connection, getJobId(cdcDDLContext), sqlKind, schema, tableName, ddlSql,
            buildMetaInfo(cdcDDLContext.isRefreshTableMetaInfo(),
                schema,
                tableName,
                sqlKind,
                extendParams,
                cdcDDLContext.getNewTableTopology(),
                cdcDDLContext.getTablesExtInfoPair()),
            visibility,
            JSONObject.toJSONString(extInfo));
    }

    private void doFinalMark(Connection connection, Long jobId, String sqlKind, String schema, String tableName,
                             String ddlSql, String metaInfo, CdcDdlMarkVisibility visibility, String ext) {
        CdcDdlMarkSyncAction syncAction = new CdcDdlMarkSyncAction(
            jobId, sqlKind, schema, tableName, ddlSql, metaInfo, visibility, ext);
        if (ExecUtils.hasLeadership(null)) {
            syncAction.doSync(connection);
        } else {
            boolean success = false;
            try {
                String leaderKey = ExecUtils.getLeaderKey(null);
                SyncManagerHelper.sync(syncAction, SystemDbHelper.CDC_DB_NAME, leaderKey);
                success = true;
            } catch (Throwable e) {
                logger.error(
                    "Hit sync failure (" + e.getMessage() + ") when sending a cdc ddl mark action to the leader. "
                        + " will retry in local node , " + ddlSql, e);
            }

            if (!success) {
                syncAction.doSync(connection);
            }
        }
    }

    private void tryAttachImplicitTableGroupInfo(String schema, String tableName, String ddlSql,
                                                 DDLExtInfo ddlExtInfo, Map<String, Object> extendParams) {
        ParamManager paramManager = new ParamManager(extendParams);
        boolean enableImplicitTg = paramManager.getBoolean(ConnectionParams.ENABLE_IMPLICIT_TABLE_GROUP);
        if (!enableImplicitTg) {
            return;
        }
        ddlExtInfo.setEnableImplicitTableGroup(true);

        if (extendParams.containsKey(EXCHANGE_NAMES_MAPPING)) {
            Map<String, String> exchangeNamesMapping = (Map<String, String>) extendParams.get(EXCHANGE_NAMES_MAPPING);
            ImplicitTableGroupUtil.exchangeNamesMapping.set(exchangeNamesMapping);
        }

        if (StringUtils.isNotBlank(ddlExtInfo.getOriginalDdl())) {
            String sql = tryAttachImplicitTableGroup(schema, tableName, ddlExtInfo.getOriginalDdl());
            if (!StringUtils.equals(sql, ddlExtInfo.getOriginalDdl())) {
                ddlExtInfo.setOriginalDdl(tryReAttacheDdlVersionId(sql, ddlExtInfo));
            }
        } else {
            String sql = tryAttachImplicitTableGroup(schema, tableName, ddlSql);
            if (!StringUtils.equals(sql, ddlSql)) {
                ddlExtInfo.setOriginalDdl(tryReAttacheDdlVersionId(sql, ddlExtInfo));
            }
        }
    }

    private String tryReAttacheDdlVersionId(String ddlSql, DDLExtInfo ddlExtInfo) {
        if (ddlExtInfo.getDdlId() != null && ddlExtInfo.getDdlId() > 0) {
            String hints = CdcMarkUtil.buildVersionIdHint(ddlExtInfo.getDdlId());
            if (!StringUtils.containsIgnoreCase(ddlSql, hints)) {
                return hints + ddlSql;
            }
        }
        return ddlSql;
    }

    private String tryBuildCreateSql4PhyTable(String schema, String tableName, Map<String, Object> extendParams,
                                              CdcDDLContext cdcDDLContext)
        throws SQLException {
        if (shouldRebuildCreatSql4PhyTable(cdcDDLContext, extendParams)) {
            MetaInfo metaInfo = buildMetaForTable(schema, tableName, extendParams,
                tryBuildTargetDBs(cdcDDLContext.getNewTableTopology()), cdcDDLContext.getTablesExtInfoPair());
            LogicMeta.LogicTableMeta tableMeta = metaInfo.getLogicTableMeta();
            String phyCreateSql = MetaBuilder.getPhyCreateSql(schema, tableMeta);

            List<SQLStatement> phyStatementList =
                SQLParserUtils.createSQLStatementParser(phyCreateSql, DbType.mysql, SQL_PARSE_FEATURES)
                    .parseStatementList();
            MySqlCreateTableStatement phyCreateStmt = (MySqlCreateTableStatement) phyStatementList.get(0);
            phyCreateStmt.setTableName("`" + MetaBuilder.escape(tableName) + "`");
            filterColumns(phyCreateStmt, schema, tableName);
            return phyCreateStmt.toUnformattedString();
        }
        return "";
    }

    private boolean shouldRebuildCreatSql4PhyTable(CdcDDLContext cdcDDLContext, Map<String, Object> extendParams) {
        Object refreshCreateSql4PhyFlag = extendParams.get(REFRESH_CREATE_SQL_4_PHY_TABLE);
        return "true".equals(refreshCreateSql4PhyFlag) || cdcDDLContext.isRefreshTableMetaInfo() ||
            ((SqlKind.valueOf(cdcDDLContext.getSqlKind()) == SqlKind.ALTER_TABLE
                || SqlKind.valueOf(cdcDDLContext.getSqlKind()) == SqlKind.ALTER_TABLE_SET_TABLEGROUP) && extendParams
                .containsKey(ICdcManager.ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG));
    }

    private void sendInstructionInternal(InstructionType instructionType, String instructionId,
                                         String instructionContent) {
        try (Connection connection = prepareConnection()) {
            logger.warn("prepare to send instruction, instructionType is " + instructionType + ", "
                + "instructionId is " + instructionId + ", size of instructionContent is " +
                instructionContent.getBytes().length + ".");

            CdcTableUtil.getInstance()
                .insertInstruction(connection, instructionType, instructionId, instructionContent);

            logger.warn("successfully send instruction, instructionType is " + instructionType + ", "
                + "instructionId is " + instructionId + ", size of instructionContent is " +
                instructionContent.getBytes().length + ".");
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_CDC_GENERIC, t, t.getMessage());
        }
    }

    private Long getJobId(CdcDDLContext cdcDDLContext) {
        Long jobId = cdcDDLContext.getJobId();
        return (jobId == null || jobId == 0) ? null : jobId;
    }

    private Long getTaskSubSeq(Map<String, Object> extendParams) {
        if (extendParams.containsKey(ICdcManager.TASK_MARK_SEQ)) {
            return Long.parseLong(extendParams.get(ICdcManager.TASK_MARK_SEQ).toString());
        }
        return null;
    }

    private String buildMetaInfo(boolean refreshTableMetaInfo, String schemaName, String tableName, String sqlKind,
                                 Map<String, Object> extendParams, Map<String, Set<String>> newTableTopology,
                                 Pair<String, TablesExtInfo> tableMetaRecords)
        throws SQLException {
        SqlKind kind = SqlKind.valueOf(sqlKind);

        List<TargetDB> targetDBList = tryBuildTargetDBs(newTableTopology);

        // 如果外部显示指定了需要刷新元数据，则直接构建即可
        if (refreshTableMetaInfo) {
            return JSONObject.toJSONString(
                buildMetaForTable(schemaName, tableName, extendParams, targetDBList, tableMetaRecords));
        }

        if (kind == SqlKind.CREATE_DATABASE || kind == SqlKind.MOVE_DATABASE) {
            return JSONObject.toJSONString(buildMetaForDb(schemaName));
        } else if (kind == SqlKind.CREATE_TABLE || kind == SqlKind.RENAME_TABLE) {
            return JSONObject
                .toJSONString(buildMetaForTable(schemaName, tableName, extendParams, targetDBList, tableMetaRecords));
        } else if ((kind == SqlKind.ALTER_TABLE || kind == SqlKind.ALTER_TABLE_SET_TABLEGROUP) && extendParams
            .containsKey(ICdcManager.ALTER_TRIGGER_TOPOLOGY_CHANGE_FLAG)) {
            return JSONObject
                .toJSONString(buildMetaForTable(schemaName, tableName, extendParams, targetDBList, tableMetaRecords));
        } else {
            return null;
        }
    }

    private List<TargetDB> tryBuildTargetDBs(Map<String, Set<String>> newTableTopology) {
        if (newTableTopology != null && !newTableTopology.isEmpty()) {
            return newTableTopology.entrySet().stream().map(e -> {
                TargetDB targetDB = new TargetDB();
                targetDB.setDbIndex(e.getKey());
                final Map<String, Field> tables = new HashMap<>();
                e.getValue().forEach(i -> tables.put(i, null));
                targetDB.setTableNames(tables);
                return targetDB;
            }).collect(Collectors.toList());
        }
        return Lists.newArrayList();
    }

    private MetaInfo buildMetaForDb(String schemaName) throws SQLException {
        MetaInfo metaInfo = new MetaInfo();
        metaInfo.logicDbMeta = MetaBuilder.buildLogicDbMeta(schemaName, null);
        checkLogicDbMeta(schemaName, metaInfo.logicDbMeta);
        return metaInfo;
    }

    private MetaInfo buildMetaForTable(String schemaName, String tableName,
                                       Map<String, Object> extendParams,
                                       List<TargetDB> assignedTargetDBList,
                                       Pair<String, TablesExtInfo> tableMetaRecords)
        throws SQLException {
        MetaInfo metaInfo = new MetaInfo();

        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            List<TargetDB> targetDbList = !assignedTargetDBList.isEmpty() ? assignedTargetDBList :
                getTargetDBs(schemaName, tableName, extendParams);
            metaInfo.logicTableMeta =
                MetaBuilder.buildLogicTableMeta(TableMode.SHARDING, schemaName, tableName, targetDbList,
                    tableMetaRecords);
        } else {
            List<TargetDB> targetDbList = !assignedTargetDBList.isEmpty() ? assignedTargetDBList :
                getTargetDBs(schemaName, tableName, extendParams);
            metaInfo.logicTableMeta =
                MetaBuilder.buildLogicTableMeta(TableMode.PARTITION, schemaName, tableName, targetDbList,
                    tableMetaRecords);
        }

        if (extendParams.containsKey(ICdcManager.TABLE_NEW_NAME)) {
            metaInfo.logicTableMeta.setTableName(extendParams.get(ICdcManager.TABLE_NEW_NAME).toString());
        }

        checkLogicTableMeta(schemaName, metaInfo.logicTableMeta);
        return metaInfo;
    }

    private List<TargetDB> getTargetDBs(String schemaName, String tableName,
                                        Map<String, Object> extendParams) throws SQLException {
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return getTargetDBForShardingTables(schemaName, tableName, extendParams);
        } else {
            return getTargetDBForPartitioningTables(schemaName, tableName, extendParams);
        }
    }

    private List<TargetDB> getTargetDBForShardingTables(String schemaName, String tableName,
                                                        Map<String, Object> extendParams) throws SQLException {
        final OptimizerContext context = OptimizerContext.getContext(schemaName);
        assert context != null;

        final Map<String, DataType> dataTypeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Map<String, Object> calcParams = new HashMap<>();
        calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);

        Partitioner partitioner = context.getPartitioner();
        VirtualTableRuleMatcher matcher = new VirtualTableRuleMatcher();
        TableRule tbRule = buildTableRule(schemaName, tableName, extendParams);

        MatcherResult result = matcher.match(
            tableName,
            new ComparativeMapChoicer() {
                @Override
                public Map<String, Comparative> getColumnsMap(List<Object> arguments, Set<String> colNameSet) {
                    Map<String, Comparative> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (String str : colNameSet) {
                        Comparative c = getColumnComparative(arguments, str);
                        if (c != null) {
                            map.put(str, c);
                        }
                    }
                    return map;
                }

                @Override
                public Comparative getColumnComparative(List<Object> arguments, String colName) {
                    return partitioner.getComparativeByFetcher(tbRule, null, colName, null, dataTypeMap, calcParams);
                }
            },
            Lists.newArrayList(),
            tbRule, true, true, calcParams);

        return result.getCalculationResult();
    }

    private List<TargetDB> getTargetDBForPartitioningTables(String schemaName, String tableName,
                                                            Map<String, Object> extendParams) throws SQLException {
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        TablePartitionConfig tbPartConf =
            TablePartitionConfigUtil.getTablePartitionConfig(schemaName, tableName, false);
        PartitionInfoManager.PartInfoCtx partCtx =
            new PartitionInfoManager.PartInfoCtx(partitionInfoManager, tableName, tbPartConf.getTableConfig().groupId);
        partCtx.setIncludeNonPublic(true);
        PartitionInfo partitionInfo = partCtx.getPartInfo();

        Map<String, List<PhysicalPartitionInfo>> topology = partitionInfo.getPhysicalPartitionTopology(null);
        return PartitionPrunerUtils.buildTargetDbsByTopologyInfos(tableName, topology);
    }

    private TableRule buildTableRule(String schemaName, String tableName, Map<String, Object> extendParams)
        throws SQLException {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TablesExtAccessor accessor = new TablesExtAccessor();
            accessor.setConnection(metaDbConn);

            TablesExtRecord tablesExtRecord = accessor.query(schemaName, tableName, false);
            if (extendParams.containsKey(ICdcManager.TABLE_NEW_PATTERN)) {
                tablesExtRecord.tbNamePattern = extendParams.get(ICdcManager.TABLE_NEW_PATTERN).toString();
            }
            TddlRuleGmsConfig config = new TddlRuleGmsConfig();
            return config.initTableRule(tablesExtRecord);
        }
    }

    private void checkStorageChange() throws Exception {
        // 系统启动的时候，会对cdc db进行初始化，但不排除出现一些异常情况导致cdc db不复存在，所以此处增加判断
        if (!isCdcDbExist()) {
            logger.warn("cdc db is not exist, please check the reason.");
            return;
        }

        processCommands();

        String instId = InstIdUtil.getInstId();
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            Set<String> allStorageInfos =
                new HashSet<>(storageInfoAccessor.
                    getStorageIdListByInstIdAndInstKind(instId, StorageInfoRecord.INST_KIND_MASTER));

            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            Set<String> cdcDbStorageInfos =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId, CDC_DB_NAME)
                    .stream()
                    .map(d -> d.storageInstId).collect(Collectors.toSet());

            // 取差集
            List<String> toBeAddedStorageInfoList =
                allStorageInfos.stream().filter(s -> !cdcDbStorageInfos.contains(s))
                    .collect(Collectors.toList());
            List<String> toBeRemovedStorageInfoList =
                cdcDbStorageInfos.stream().filter(s -> !allStorageInfos.contains(s))
                    .collect(Collectors.toList());

            // 执行添加
            if (!toBeAddedStorageInfoList.isEmpty()) {
                onStorageInstAdd(toBeAddedStorageInfoList);
            }

            // 执行删除
            if (!toBeRemovedStorageInfoList.isEmpty()) {
                //删除操作不再通过对比差异的方式进行判断，不太合理，移到CdcStorageUtil进行主动的remove
                //onStorageInstRemove(toBeRemovedStorageInfoList);
            }
        }
    }

    private void onStorageInstAdd(List<String> storageInstIdListAdded) {
        try (Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {
            // acquire Cdc Lock by for update, to avoiding concurrent update cdc meta info
            metaDbLockConn.setAutoCommit(false);
            try {
                acquireCdcDbLockByForUpdate(metaDbLockConn);
                logger.warn("acquire cdc db lock for processing storage added.");
            } catch (Throwable ex) {
                throw new TddlRuntimeException(com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_GMS_GENERIC, ex,
                    "get metaDb lock timeout during update cdc group info, please retry");
            }

            BinlogCommandRecord commandParameter = new BinlogCommandRecord();
            commandParameter.cmdId = UUID.randomUUID().toString();
            commandParameter.cmdType = ADD_STORAGE.getValue();
            commandParameter.cmdStatus = INITIAL.getValue();
            commandParameter.cmdRequest = JSONObject.toJSONString(storageInstIdListAdded);

            List<String> actualProcessedInsts = doAdd(commandParameter, storageInstIdListAdded);
            if (!actualProcessedInsts.isEmpty()) {
                BinlogCommandRecord commandRecord =
                    getBinlogCommandRecordByTypeAndCmdId(commandParameter.cmdType, commandParameter.cmdId);
                sendStorageChangeInstruction(commandRecord, Sets.newHashSet());
                notifyCommandSuccess(commandRecord.id);
            }

            releaseCdcDbLockByCommit(metaDbLockConn);
            logger.warn("release cdc db lock for processing storage add.");
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    private List<String> doAdd(BinlogCommandRecord commandParameter, List<String> storageInstIdListAdded) {
        String dbName = CDC_DB_NAME;
        String instId = InstIdUtil.getInstId();
        storageInstIdListAdded = storageInstIdListAdded.stream().sorted().collect(Collectors.toList());
        List<String> actualProcessedInsts = new ArrayList<>();
        for (String storageInstId : storageInstIdListAdded) {
            String dbCharset;
            String dbCollation;
            List<Pair<String, String>> newGroups;

            // validate and prepare
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                // 必须进行幂等判断
                if (isStorageContainsGroup(metaDbConn, storageInstId)) {
                    logger.warn(
                        String.format("There are already some groups in storage %s for db %s.", storageInstId, dbName));
                    continue;
                }

                newGroups = generateNewGroupMeta(metaDbConn);

                DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
                dbInfoAccessor.setConnection(metaDbConn);
                DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(dbName);
                dbCharset = dbInfoRecord.charset;
                dbCollation = dbInfoRecord.collation;
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            // create phy database & tables
            try {
                createDbAndTables(instId, dbName, dbCharset, dbCollation, storageInstId, newGroups);
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            // add group&detail info
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                metaDbConn.setAutoCommit(false);
                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);
                GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
                groupDetailInfoAccessor.setConnection(metaDbConn);

                for (Pair<String, String> pair : newGroups) {
                    dbGroupInfoAccessor
                        .addNewDbAndGroup(dbName, pair.getKey(), pair.getValue(),
                            DbGroupInfoRecord.GROUP_TYPE_NORMAL);
                    groupDetailInfoAccessor.addNewGroupDetailInfo(instId, dbName, pair.getKey(), storageInstId);

                    StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                    storageInfoAccessor.setConnection(metaDbConn);
                    List<StorageInfoRecord> slaveStorageInfoRecords =
                        storageInfoAccessor.getSlaveStorageInfosByMasterStorageInstId(storageInstId);
                    for (StorageInfoRecord slaveStorageInfo : slaveStorageInfoRecords) {
                        String readOnlyInstId = slaveStorageInfo.instId;
                        String slaveStorageInstId = slaveStorageInfo.storageInstId;
                        groupDetailInfoAccessor
                            .addNewGroupDetailInfo(readOnlyInstId, dbName, pair.getKey(), slaveStorageInstId);
                    }

                    MetaDbConfigManager.getInstance()
                        .register(MetaDbDataIdBuilder.getGroupConfigDataId(instId, dbName, pair.getKey()),
                            metaDbConn);
                }

                BinlogCommandAccessor commandAccessor = new BinlogCommandAccessor();
                commandAccessor.setConnection(metaDbConn);
                commandAccessor.insertIgnoreBinlogCommandRecord(commandParameter);

                MetaDbConfigManager.getInstance()
                    .notify(MetaDbDataIdBuilder.getDbTopologyDataId(dbName), metaDbConn);
                metaDbConn.commit();

                // try inject trouble
                tryInjectTrouble(storageInstId);

                // sync db topology
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(dbName));
            } catch (Throwable ex) {
                throw GeneralUtil.nestedException(ex);
            }

            actualProcessedInsts.add(storageInstId);
            logger.warn("cdc db has successfully adjusted for newly added storage :" + storageInstId);
        }

        return actualProcessedInsts;
    }

    private boolean isCdcDbExist() {
        try {
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                DbInfoAccessor dbInfoAccessor = new DbInfoAccessor();
                dbInfoAccessor.setConnection(metaDbConn);
                DbInfoRecord dbInfoRecord = dbInfoAccessor.getDbInfoByDbName(CDC_DB_NAME);
                return dbInfoRecord != null;
            }
        } catch (Throwable t) {
            throw GeneralUtil.nestedException(t);
        }
    }

    private List<Pair<String, String>> generateNewGroupMeta(Connection metaDbConn) {
        String dbName = CDC_DB_NAME;
        List<Pair<String, String>> result = null;

        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConn);
        Set<String> existedGroups =
            dbGroupInfoAccessor.queryDbGroupByDbName(dbName).stream().map(g -> g.groupName).collect(
                Collectors.toSet());

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            String grpName = GroupInfoUtil.buildGroupName(dbName, i, false);
            if (!existedGroups.contains(grpName)) {
                String phyDbName = GroupInfoUtil.buildPhyDbName(dbName, i, false);
                result = Lists.newArrayList(new Pair<>(grpName, phyDbName));
                break;
            }
        }

        return result;
    }

    private void createDbAndTables(String instId, String dbName, String dbCharset, String dbCollation,
                                   String storageInstId,
                                   List<Pair<String, String>> newGroups) throws SQLException {
        for (Pair<String, String> pair : newGroups) {
            //create database
            Map<String, String> grpPhyDbMap = new HashMap<>();
            grpPhyDbMap.putIfAbsent(pair.getKey(), pair.getValue());
            DbTopologyManager.createPhysicalDbInStorageInst(dbCharset, dbCollation, storageInstId,
                grpPhyDbMap);

            //create tables
            try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
                // 从已有的group中随便选一个，作为table copy的参照标准
                GroupDetailInfoAccessor detailInfoAccessor = new GroupDetailInfoAccessor();
                detailInfoAccessor.setConnection(metaDbConn);
                GroupDetailInfoRecord srcGroupDetail =
                    detailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(instId, dbName).get(0);

                // 获取group对应的物理库名
                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);
                DbGroupInfoRecord srcDbGroupInfo =
                    dbGroupInfoAccessor.getDbGroupInfoByGroupName(srcGroupDetail.groupName);

                // cdc系统库没有拆分表，tbNamePattern即为真实的物理表名
                TablesExtAccessor tablesExtAccessor = new TablesExtAccessor();
                tablesExtAccessor.setConnection(metaDbConn);
                List<String> phyTables = tablesExtAccessor.query(dbName).stream().map(t -> t.tbNamePattern).collect(
                    Collectors.toList());

                if (!phyTables.isEmpty()) {
                    DbTopologyManager
                        .copyTablesForNewGroup(phyTables, srcDbGroupInfo.phyDbName, srcGroupDetail.storageInstId,
                            pair.getValue(), storageInstId);
                }
            }
        }
    }

    private void sendStorageChangeInstruction(BinlogCommandRecord commandRecord, Set<String> excludeStorages)
        throws SQLException {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            Set<String> storageList =
                groupDetailInfoAccessor.getGroupDetailInfoByInstIdAndDbName(InstIdUtil.getInstId(), CDC_DB_NAME)
                    .stream()
                    .map(d -> d.storageInstId)
                    .filter(i -> !excludeStorages.contains(i)).collect(Collectors.toSet());

            StorageChangeEntity entity = new StorageChangeEntity();
            entity.setStorageInstList(storageList);
            // 不要直接调用sendInstruction方法，否则会有死锁问题
            sendInstructionInternal(StorageInstChange, commandRecord.cmdId, JSONObject.toJSONString(entity));
            logger.warn("instruction is successfully send for command " + commandRecord);
        }
    }

    private void processCommands() throws Exception {
        // 对add storage进行recover
        List<BinlogCommandRecord> addStorageCommands = getAddStorageCommandsInInitial();
        if (!addStorageCommands.isEmpty()) {
            CdcDbLock.processInLock(() -> {
                processAddStorageCommands(addStorageCommands);
                return null;
            });
        }

        List<BinlogCommandRecord> readyCommands = getRemoveStorageCommandsByStatus(READY.getValue());
        if (readyCommands.size() > 1) {
            throw new UnexpectedException("unexpected ready remove storage commands num: " + readyCommands.size());
        }
        if (!readyCommands.isEmpty()) {
            processRemoveStorageCommands(readyCommands);
        }

        // 对remove storage进行处理(可能是首次操作，也可能是recover)
        List<BinlogCommandRecord> initialCommands = getRemoveStorageCommandsByStatus(INITIAL.getValue());
        if (!initialCommands.isEmpty()) {
            processRemoveStorageCommands(initialCommands);
        }
    }

    private void processAddStorageCommands(List<BinlogCommandRecord> addStorageCommands) {
        try {
            //在对command进行恢复时，当前节点和生成command的节点可能是不同的，需要对cdc系统库元数据强制进行一次sync，保证一致性
            logger.warn("sync cdc topology info before process add storage commands.");
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(CDC_DB_NAME));

            for (BinlogCommandRecord c : addStorageCommands) {
                logger.warn("recover for add_storage_command : " + c);
                boolean flag = CdcTableUtil.getInstance().isInstructionExists(StorageInstChange, c.cmdId);
                if (!flag) {
                    logger.warn("instruction for add_storage_command " + c.cmdId + " is not found,"
                        + " will retry to send.");
                    sendStorageChangeInstruction(c, Sets.newHashSet());
                } else {
                    logger.warn(
                        "instruction for add_storage_command " + c.cmdId + " has existed, will do reply.");
                    notifyCommandSuccess(c.id);
                }
            }
        } catch (SQLException se) {
            throw new TddlNestableRuntimeException("error in processing add storage command.", se);
        }
    }

    private void processRemoveStorageCommands(List<BinlogCommandRecord> removeStorageCommands) {
        try {
            //在对command进行恢复时，当前节点和生成command的节点可能是不同的，需要对cdc系统库元数据强制进行一次sync，保证一致性
            logger.warn("sync cdc topology info before process remove storage commands.");
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getDbTopologyDataId(CDC_DB_NAME));

            for (BinlogCommandRecord c : removeStorageCommands) {
                logger.warn("start to process remove_storage_command :" + c);
                if (c.cmdStatus == INITIAL.getValue()) {
                    boolean flag = CdcTableUtil.getInstance().isInstructionExists(StorageInstChange, c.cmdId);
                    if (!flag) {
                        logger.warn("instruction for remove_storage_command " + c.cmdId + " is not found, "
                            + "will try to send.");
                        StorageRemoveRequest removeRequest =
                            JSONObject.parseObject(c.cmdRequest, StorageRemoveRequest.class);
                        sendStorageChangeInstruction(c, removeRequest.getToRemoveStorageInstIds());

                        notifyCommandReady(c.id);
                    } else {
                        logger.warn(
                            "instruction for remove_storage_command " + c.cmdId + " has existed, will do reply.");
                        notifyCommandReady(c.id);
                    }

                    waitCommandSuccess(c.id);
                } else if (c.cmdStatus == READY.getValue()) {
                    // fix #55184772 串行处理缩容任务
                    waitCommandSuccess(c.id);
                }
            }
        } catch (Exception se) {
            throw new TddlNestableRuntimeException("error in processing remove storage command.", se);
        }
    }

    private List<BinlogCommandRecord> getAddStorageCommandsInInitial() throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor accessor = new BinlogCommandAccessor();
            accessor.setConnection(connection);
            return accessor.getBinlogCommandRecordByTypeAndStatus(ADD_STORAGE.getValue(), INITIAL.getValue());
        }
    }

    private List<BinlogCommandRecord> getRemoveStorageCommandsByStatus(int status) throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor accessor = new BinlogCommandAccessor();
            accessor.setConnection(connection);
            return accessor.getBinlogCommandRecordByTypeAndStatus(REMOVE_STORAGE.getValue(), status);
        }
    }

    private BinlogCommandRecord getBinlogCommandRecordByTypeAndCmdId(String commandType, String commandId)
        throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor accessor = new BinlogCommandAccessor();
            accessor.setConnection(connection);
            List<BinlogCommandRecord> list = accessor.getBinlogCommandRecordByTypeAndCmdId(commandType, commandId);
            return list.isEmpty() ? null : list.get(0);
        }
    }

    private void updateCommandStatusAndReply(Long primaryKey, int status, String reply) throws SQLException {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor accessor = new BinlogCommandAccessor();
            accessor.setConnection(conn);
            accessor.updateBinlogCommandStatusAndReply(status, reply, primaryKey);
        }
    }

    private void notifyCommandReady(Long primaryKey) throws SQLException {
        updateCommandStatusAndReply(primaryKey, READY.getValue(), "");
    }

    private void notifyCommandSuccess(Long primaryKey) throws SQLException {
        updateCommandStatusAndReply(primaryKey, SUCCESS.getValue(), "");
    }

    private void waitCommandSuccess(Long primaryKey) throws Exception {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            BinlogCommandAccessor accessor = new BinlogCommandAccessor();
            accessor.setConnection(conn);

            while (true) {
                if (Thread.interrupted()) {
                    throw new InterruptedException("thread is interrupted while waiting for command success!");
                }

                BinlogCommandRecord record = accessor.getBinlogCommandRecordById(primaryKey).get(0);
                if (record.cmdStatus == READY.getValue()) {
                    logger.warn("waiting for command status success...");
                    Thread.sleep(1000);
                } else if (record.cmdStatus == SUCCESS.getValue()) {
                    logger.warn("command status become success, will stop wait.");
                    break;
                } else {
                    logger.warn("unexpected command:" + record);
                    throw new IllegalStateException("unexpected command status:" + record.cmdStatus);
                }
            }
        }
    }

    private boolean isCdcDisabled() {
        String CDC_STARTUP_MODE =
            MetaDbInstConfigManager.getInstance().getInstProperty(ConnectionProperties.CDC_STARTUP_MODE);
        return Integer.parseInt(CDC_STARTUP_MODE) == 0;
    }

    private void checkLength(String schemaName, String tableName) {
        if (StringUtils.isNotBlank(schemaName)) {
            LimitValidator.validateTableNameLength(schemaName);
        }
        if (StringUtils.isNotBlank(tableName)) {
            LimitValidator.validateTableNameLength(tableName);
        }
    }

    private void tryInjectTrouble(String storageInstId) {
        FailPoint.inject(FP_INJECT_FAILURE_TO_CDC_AFTER_ADD_NEW_GROUP, () -> {
            logger.warn("inject failure to cdc after add new group at storage " + storageInstId);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            Runtime.getRuntime().halt(1);
        });
    }

    @Data
    public static class MetaInfo {
        private LogicMeta.LogicDbMeta logicDbMeta;
        private LogicMeta.LogicTableMeta logicTableMeta;
    }
}
