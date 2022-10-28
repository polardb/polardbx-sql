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
import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.cdc.entity.LogicMeta;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineScheduler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandAccessor;
import com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord;
import com.alibaba.polardbx.gms.topology.DbInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.LockUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.metadata.InfoSchemaCommon;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.alibaba.polardbx.server.conn.InnerConnection;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.cdc.SQLHelper.FEATURES;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_STATUS_FAIL;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_STATUS_SUCCESS;
import static com.alibaba.polardbx.gms.metadb.cdc.PolarxCommandRecord.COMMAND_TYPE.CDC_START;

/**
 * Created by ziyang.lb
 **/
public class CommandScanner extends AbstractLifecycle {
    private final static Logger logger = LoggerFactory.getLogger(CommandScanner.class);
    public final static String TEST_FLAG = "FOR_UNIT_OR_QA_TEST";

    private final CdcManager cdcManager;
    private long lastScanTimestamp;

    CommandScanner(CdcManager cdcManager) {
        this.cdcManager = cdcManager;
        this.lastScanTimestamp = System.currentTimeMillis() - 3600 * 1000;//向前取一个小时
    }

    protected void scan() throws SQLException {
        if (!ExecUtils.hasLeadership(null)) {
            return;
        }

        List<PolarxCommandRecord> binlogCommandRecords = getBinlogCommandRecordsByTime().stream()
            .filter(c -> PolarxCommandRecord.COMMAND_TYPE.valueOf(c.cmdType).isForScanner()).collect(
                Collectors.toList());
        for (PolarxCommandRecord commandRecord : binlogCommandRecords) {
            try {
                if (CDC_START.getValue().equals(commandRecord.cmdType)) {
                    doCdcInit(commandRecord);
                }
                replyBinlogCommand(COMMAND_STATUS_SUCCESS, "", commandRecord.id);
            } catch (Throwable t) {
                replyBinlogCommand(COMMAND_STATUS_FAIL, Arrays.toString(t.getStackTrace()), commandRecord.id);
            }
        }
        lastScanTimestamp = System.currentTimeMillis();
    }

    private void doCdcInit(PolarxCommandRecord commandRecord) {
        logger.warn("cdc init begin.");
        try {
            long sleepTime = 10 * 1000;
            long round = 0;
            while (true) {
                round++;

                // 1. get pre ddl perform version
                final AtomicLong preVersion = new AtomicLong(0L);
                preVersion.set(DdlEngineScheduler.getInstance().getPerformVersion());

                // 2. get pre db list
                final Set<String> preDbs = new HashSet<>(getAllDbs());

                // 3. build meta data
                LogicMeta logicMeta = buildCdcInitData(commandRecord);
                pauseForTest(commandRecord);

                // 4. check optimistic lock and send instruction
                try (Connection metaDbLockConn = MetaDbDataSource.getInstance().getConnection()) {
                    // forbidden create/drop database ddl sql
                    metaDbLockConn.setAutoCommit(false);
                    try {
                        LockUtil.acquireMetaDbLockByForUpdate(metaDbLockConn);
                    } catch (Throwable ex) {
                        throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                            "Get metaDb lock timeout during cdc init");
                    }

                    Set<String> postDbs = getAllDbs();

                    final AtomicBoolean timeout = new AtomicBoolean(false);
                    final AtomicBoolean success = new AtomicBoolean(false);
                    try {
                        DdlEngineScheduler.getInstance().compareAndExecute(preVersion.get(), () -> {
                            if (postDbs.equals(preDbs)) {
                                cdcManager.sendInstruction(ICdcManager.InstructionType.CdcStart, "0",
                                    JSONObject.toJSONString(logicMeta));
                                success.set(true);
                            }
                            return null;
                        });
                    } catch (TimeoutException e) {
                        timeout.set(true);
                    }

                    // release meta db lock
                    LockUtil.releaseMetaDbLockByCommit(metaDbLockConn);

                    if (success.get()) {
                        break;
                    } else {
                        if (timeout.get()) {
                            logger.warn("ddl version check time out, will retry.");
                        } else {
                            logger.warn(String.format(
                                "optimistic lock failed in round %s. post version is %s,"
                                    + " preDbs is %s, postDbs is %s.",
                                round,
                                DdlEngineScheduler.getInstance().getPerformVersion(),
                                JSONObject.toJSON(preDbs),
                                JSONObject.toJSON(postDbs)));
                        }
                        Thread.sleep(sleepTime);
                    }
                }
            }
        } catch (Throwable ex) {
            logger.error("something goes wrong when do cdc init", ex);
            throw GeneralUtil.nestedException(ex);
        }

        logger.warn("cdc init finished.");
    }

    private LogicMeta buildCdcInitData(PolarxCommandRecord commandRecord) throws SQLException {
        LogicMeta logicMeta = new LogicMeta();
        logicMeta.setLogicDbMetas(new ArrayList<>());
        Set<String> databases = getDatabases();

        for (String db : databases) {
            try (Connection connection = new InnerConnection(db)) {
                try (Statement stmt = connection.createStatement()) {
                    Set<String> tables = getTables(connection, db);

                    Map<String, List<TargetDB>> tableParams = new HashMap<>();
                    for (String table : tables) {
                        tableParams.put(table, getTargetDbs(db, table));
                    }
                    LogicMeta.LogicDbMeta logicDbMeta = MetaBuilder.buildLogicDbMeta(db, tableParams);
                    for (LogicMeta.LogicTableMeta tableMeta : logicDbMeta.getLogicTableMetas()) {
                        String logicTableCreateSql = getCreateTableSql(stmt, db, tableMeta.getTableName());
                        tableMeta.setCreateSql(logicTableCreateSql);
                        trySetCreateSql4Phy(logicDbMeta, tableMeta, logicTableCreateSql);
                    }

                    logicMeta.getLogicDbMetas().add(logicDbMeta);
                    logger.warn("successfully build cdc init data for database :" + db);
                }
            }
        }

        return logicMeta;
    }

    private Set<String> getDatabases() {
        Set<String> databases = new HashSet<>();
        CobarConfig conf = CobarServer.getInstance().getConfig();
        Map<String, SchemaConfig> schemaConfigMap = conf.getSchemas();
        if (schemaConfigMap != null) {
            databases.addAll(schemaConfigMap.keySet());
            databases.remove(SystemDbHelper.CDC_DB_NAME);
            databases.remove(SystemDbHelper.DEFAULT_DB_NAME);
            databases.remove(SystemDbHelper.INFO_SCHEMA_DB_NAME);
        }
        return databases;
    }

    private Set<String> getTables(Connection connection, String db) throws SQLException {
        Set<String> tables = new HashSet<>();
        try (Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(String.format("show full tables from %s", db))) {
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                String tableType = resultSet.getString(2);
                if (InfoSchemaCommon.DEFAULT_TABLE_TYPE.equals(tableType)) {
                    tables.add(tableName.toLowerCase());
                }
            }
        }
        return tables;
    }

    private String getCreateTableSql(Statement stmt, String db, String table) throws SQLException {
        try (ResultSet resultSet = stmt.executeQuery(String
            .format("/* +TDDL:cmd_extra(SHOW_IMPLICIT_ID=true) */show create table `%s`.`%s`", db,
                MetaBuilder.escape(table)))) {
            if (resultSet.next()) {
                return resultSet.getString(2);
            }
        }
        throw new RuntimeException("can`t find the create table sql for table [" + db + "." + table + "]");
    }

    private List<TargetDB> getTargetDbs(String db, String table) {
        if (!DbInfoManager.getInstance().isNewPartitionDb(db)) {
            final OptimizerContext context = OptimizerContext.getContext(db);
            assert context != null;
            context.getLatestSchemaManager().getTable(table);
            ExecutionContext ec = new ExecutionContext(db);
            Map<String, Object> calcParams = new HashMap<>();
            calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
            return context.getRuleManager().shard(table, true, true, null, null, calcParams, ec);
        } else {
            PartitionInfo partitionInfo =
                OptimizerContext.getContext(db).getPartitionInfoManager().getPartitionInfo(table);
            Map<String, List<PhysicalPartitionInfo>> topology = partitionInfo.getPhysicalPartitionTopology(null);
            return PartitionPrunerUtils.buildTargetDbsByTopologyInfos(table, topology);

        }
    }

    private void pauseForTest(PolarxCommandRecord commandRecord) {
        try {
            if (StringUtils.isNotBlank(commandRecord.cmdRequest) && commandRecord.cmdRequest.startsWith(TEST_FLAG)) {
                updateBinlogCommandRequestById(TEST_FLAG + "|READY", commandRecord.id);

                while (true) {
                    PolarxCommandRecord newRecord = getBinlogCommandRecordsById(commandRecord.id);
                    if ("CONTINUE".equals(StringUtils.substringAfterLast(newRecord.cmdRequest, "|"))) {
                        return;
                    } else {
                        Thread.sleep(1000);
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("check for test error.", t);
        }
    }

    private void trySetCreateSql4Phy(LogicMeta.LogicDbMeta logicDbMeta, LogicMeta.LogicTableMeta tableMeta,
                                     String logicCreateSql) {
        String logicSchema = logicDbMeta.getSchema();
        String phyCreateSql = MetaBuilder.getPhyCreateSql(logicSchema, tableMeta);
        String createSql4Phy = tryBuildPhyCreateSql(logicCreateSql, phyCreateSql);
        tableMeta.setCreateSql4Phy(createSql4Phy);
    }

    private String tryBuildPhyCreateSql(String logicCreateSql, String phyCreateSql) {
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("try build phyCreateSql, logicCreateSql is {%s}, phyCreateSql is {%s}.",
                logicCreateSql, phyCreateSql));
        }

        try {
            List<SQLStatement> logicStatementList =
                SQLParserUtils.createSQLStatementParser(logicCreateSql, DbType.mysql, FEATURES).parseStatementList();
            MySqlCreateTableStatement logicCreateStmt = (MySqlCreateTableStatement) logicStatementList.get(0);
            List<String> logicColumns =
                logicCreateStmt.getColumnDefinitions().stream().map(c -> c.getColumnName().toLowerCase())
                    .collect(Collectors.toList());

            List<SQLStatement> phyStatementList =
                SQLParserUtils.createSQLStatementParser(phyCreateSql, DbType.mysql, FEATURES).parseStatementList();
            MySqlCreateTableStatement phyCreateStmt = (MySqlCreateTableStatement) phyStatementList.get(0);
            List<String> phyColumns =
                phyCreateStmt.getColumnDefinitions().stream().map(c -> c.getColumnName().toLowerCase())
                    .collect(Collectors.toList());

            //如果逻辑表和物理表的列序不一致，则需要额外提供以物理表列序为依据的建表sql
            if (!logicColumns.equals(phyColumns)) {
                logger.warn(
                    "logic table`s columns is different from phy tables`, table name : " + logicCreateStmt
                        .getTableName());
                phyCreateStmt.setTableName(logicCreateStmt.getTableName());
                return phyCreateStmt.toUnformattedString();
            }
        } catch (Throwable t) {
            logger.error("try build phy create sql error, logicCreateSql is " + logicCreateSql + ", phyCreateSql is "
                + phyCreateSql, t);
            throw t;
        }

        return null;
    }

    private Set<String> getAllDbs() {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            DbInfoAccessor accessor = new DbInfoAccessor();
            accessor.setConnection(connection);
            return accessor.getAllDbInfos().stream().map(d -> d.dbName).collect(Collectors.toSet());
        } catch (SQLException e) {
            throw GeneralUtil.nestedException("get all dbs failed.", e);
        }
    }

    protected PolarxCommandRecord getBinlogCommandRecordsById(long id) throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            PolarxCommandAccessor accessor = new PolarxCommandAccessor();
            accessor.setConnection(connection);
            List<PolarxCommandRecord> records = accessor.getBinlogCommandRecordById(id);
            if (records.size() == 0) {
                return null;
            }
            return records.get(0);
        }
    }

    protected List<PolarxCommandRecord> getBinlogCommandRecordsByTime() throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            PolarxCommandAccessor accessor = new PolarxCommandAccessor();
            accessor.setConnection(connection);
            return accessor.getBinlogCommandRecordsByTime(lastScanTimestamp / 1000);
        }
    }

    protected void replyBinlogCommand(int status, String reply, long id) throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            PolarxCommandAccessor accessor = new PolarxCommandAccessor();
            accessor.setConnection(connection);
            accessor.updateBinlogCommandStatusAndReply(status, reply, id);
        }
    }

    protected void updateBinlogCommandRequestById(String request, long id) throws SQLException {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            PolarxCommandAccessor accessor = new PolarxCommandAccessor();
            accessor.setConnection(connection);
            accessor.updateBinlogCommandRequestById(request, id);
        }
    }
}
