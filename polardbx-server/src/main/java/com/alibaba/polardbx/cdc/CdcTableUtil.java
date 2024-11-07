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
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcDdlRecord;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.cdc.entity.DDLExtInfo;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerTransManager;
import com.alibaba.polardbx.server.util.SysTableUtil;
import lombok.SneakyThrows;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * 关于Cdc系统库中系统表的初始化时机，有几个选择，做如下说明：
 * 1. 一个时机是在SystemDbHelper中，创建Cdc库的时候连带创建系统表，但这个位置太靠前，使用TConnection时会报错，所以放弃
 * 2. 一个时机是在CdcManager的doInit方法中，我们选择此方案，但要特别注意死锁问题，因为系统表初始化也会触发CDC打标
 **/
public class CdcTableUtil {
    private final static Logger logger = LoggerFactory.getLogger(CdcTableUtil.class);

    public static final String CDC_DDL_RECORD_TABLE = "__cdc_ddl_record__";
    public static final String CDC_INSTRUCTION_TABLE = "__cdc_instruction__";
    public static final String CDC_HEARTBEAT_TABLE = "__cdc_heartbeat__";
    public static final String CDC_TABLE_SCHEMA = SystemDbHelper.CDC_DB_NAME;
    /**
     * 需注意"SCHEMA_NAME列"和"TABLE_NAME列"的长度不能小于meta db中"db_info表"和"tables表"中对应列的长度
     */
    private final static String CREATE_CDC_DDL_RECORD_TABLE = String.format(
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `JOB_ID` BIGINT(20)  DEFAULT NULL,\n"
            + "  `SQL_KIND` VARCHAR(50) NOT NULL,\n"
            + "  `SCHEMA_NAME` VARCHAR(200) NOT NULL,\n"
            + "  `TABLE_NAME`  VARCHAR(200) DEFAULT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `DDL_SQL`  MEDIUMTEXT NOT NULL,\n"
            + "  `META_INFO` MEDIUMTEXT DEFAULT NULL,\n"
            + "  `VISIBILITY` BIGINT(10) NOT NULL,\n"
            + "  `EXT` TEXT DEFAULT NULL,\n"
            + "  PRIMARY KEY (`ID`),\n"
            + "  KEY idx_job_id(`JOB_ID`)"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 BROADCAST\n", CDC_DDL_RECORD_TABLE);

    private final static String CDC_ADD_INDEX = "alter table %s add index idx_job_id(`JOB_ID`)";

    /**
     * CDC通用指令表
     */
    private final static String CREATE_CDC_INSTRUCTION_TABLE = String.format(
        "CREATE TABLE IF NOT EXISTS `%s` (\n"
            + "  `ID` BIGINT(20) NOT NULL auto_increment,\n"
            + "  `INSTRUCTION_TYPE` VARCHAR(50) NOT NULL,\n"
            + "  `INSTRUCTION_CONTENT` MEDIUMTEXT NOT NULL,\n"
            + "  `GMT_CREATED` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `INSTRUCTION_ID` VARCHAR(50) NOT NULL,\n"
            + "  PRIMARY KEY (`ID`),\n"
            + "  UNIQUE KEY `uk_instruction_id_type` (`INSTRUCTION_TYPE`,`INSTRUCTION_ID`) \n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 BROADCAST\n", CDC_INSTRUCTION_TABLE);

    /**
     * Sql Template for ddl record insert
     */
    final static String INSERT_CDC_DDL_RECORD = String
        .format(
            "INSERT INTO `%s`(JOB_ID,SQL_KIND,SCHEMA_NAME,TABLE_NAME,GMT_CREATED,DDL_SQL,META_INFO,VISIBILITY,EXT)"
                + "VALUES(?,?,?,?,NOW(),?,?,?,?)",
            CDC_DDL_RECORD_TABLE);

    /**
     * 通过job id查询对应的ddl打标记录是否存在
     */
    final static String QUERY_CDC_DDL_RECORD_BY_JOBID =
        "SELECT JOB_ID,EXT FROM `" + CDC_DDL_RECORD_TABLE + "` WHERE JOB_ID = %s";

    final static String QUERY_CDC_DDL_RECORD_BY_SCHEMA_TABLE_SQL_KIND_DDL_SQL =
        "SELECT ID, JOB_ID, SQL_KIND, SCHEMA_NAME, TABLE_NAME, GMT_CREATED, DDL_SQL, META_INFO, VISIBILITY, EXT FROM `"
            + CDC_DDL_RECORD_TABLE + "` WHERE SCHEMA_NAME = ? AND TABLE_NAME = ? AND SQL_KIND = ? AND DDL_SQL LIKE ?";

    final static String QUERY_CDC_DDL_RECORD_BY_JOB_ID_SQL =
        "SELECT ID, JOB_ID, SQL_KIND, SCHEMA_NAME, TABLE_NAME, GMT_CREATED, DDL_SQL, META_INFO, VISIBILITY, EXT FROM `"
            + CDC_DDL_RECORD_TABLE + "` WHERE JOB_ID = ?";

    /**
     * Sql Template for cdc instruction insert
     */
    final static String INSERT_CDC_INSTRUCTION = String
        .format(
            "INSERT IGNORE INTO `%s`(INSTRUCTION_TYPE,INSTRUCTION_ID,INSTRUCTION_CONTENT,GMT_CREATED)VALUES(?,?,?,NOW())",
            CDC_INSTRUCTION_TABLE);

    /**
     * 通过type查询instruction
     */
    final static String QUERY_CDC_INSTRUCTION_COUNT_BY_TYPE_AND_ID =
        String
            .format("SELECT COUNT(ID) FROM %s WHERE INSTRUCTION_TYPE =? and INSTRUCTION_ID =?", CDC_INSTRUCTION_TABLE);

    public final static String QUERY_CDC_DDL_RECORD_LIMIT_1 =
        "SELECT JOB_ID,EXT FROM `" + CDC_DDL_RECORD_TABLE + "` LIMIT 1";

    private CdcTableUtil() {
    }

    public void prepareCdcSysTables() {
        boolean allReady = false;
        int errorTime = 0;

        while (!allReady) {
            try {
                List<String> readyTables = SysTableUtil.queryReadyTables(CDC_TABLE_SCHEMA);

                if (readyTables.contains(CDC_DDL_RECORD_TABLE) && readyTables.contains(CDC_INSTRUCTION_TABLE)) {
                    allReady = true;
                } else {
                    //只有leader节点才进行系统表的初始化，避免引入全局锁
                    if (!readyTables.contains(CDC_DDL_RECORD_TABLE) && ExecUtils.hasLeadership(null)) {
                        SysTableUtil.prepareTable(CDC_TABLE_SCHEMA, CDC_DDL_RECORD_TABLE, CREATE_CDC_DDL_RECORD_TABLE);
                    }
                    if (!readyTables.contains(CDC_INSTRUCTION_TABLE) && ExecUtils.hasLeadership(null)) {
                        SysTableUtil.prepareTable(CDC_TABLE_SCHEMA, CDC_INSTRUCTION_TABLE,
                            CREATE_CDC_INSTRUCTION_TABLE);
                    }
                }

                if (!allReady) {
                    logger.warn("cdc system tables are not ready yet ,will sleep and retry.");
                    Thread.sleep(500);
                }
                alterTable();
            } catch (Throwable t) {
                errorTime++;
                if (errorTime > 2) {
                    //最大允许重试3次，初始化过程中出现异常的概率很低，但为了更好的容错，我们增加3次异常重试
                    throw GeneralUtil.nestedException("init cdc system tables failed.", t);
                } else {
                    logger.error("init cdc system tables failed, will retry.", t);
                    // 自动Recover残留的CDC系统表ddl任务
                    SysTableUtil.autoRecover();
                    try {
                        Thread.sleep(15000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    public static CdcTableUtil getInstance() {
        return SysTableUtilHolder.INSTANCE;
    }

    public void insertInstruction(Connection connection, ICdcManager.InstructionType instructionType,
                                  String instructionId,
                                  String instructionContent) throws SQLException {
        InnerTransManager transManager = new InnerTransManager(connection);
        transManager.executeWithTransaction(() -> {
            try (PreparedStatement stmt = connection.prepareStatement(INSERT_CDC_INSTRUCTION)) {
                stmt.setObject(1, instructionType.name());
                stmt.setString(2, instructionId);
                stmt.setString(3, instructionContent);
                stmt.executeUpdate();
            }
        });
    }

    public List<CdcDdlRecord> queryDdlRecord(@NotNull Connection connection,
                                             @NotNull String schema,
                                             @NotNull String table,
                                             @NotNull String sqlKind,
                                             @NotNull Long versionId) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(
            QUERY_CDC_DDL_RECORD_BY_SCHEMA_TABLE_SQL_KIND_DDL_SQL)) {

            stmt.setObject(1, schema);
            stmt.setString(2, table);
            stmt.setString(3, sqlKind);
            stmt.setString(4, CdcMarkUtil.buildVersionIdHint(versionId) + "%");
            final ResultSet rs = stmt.executeQuery();
            final List<CdcDdlRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(CdcDdlRecord.fill(rs));
            }
            return result;
        }
    }

    public List<CdcDdlRecord> queryDdlRecordByJobId(@NotNull Connection connection,
                                                    @NotNull Long ddlJobId) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(
            QUERY_CDC_DDL_RECORD_BY_JOB_ID_SQL)) {

            stmt.setObject(1, ddlJobId);
            final ResultSet rs = stmt.executeQuery();
            final List<CdcDdlRecord> result = new ArrayList<>();
            while (rs.next()) {
                result.add(CdcDdlRecord.fill(rs));
            }
            return result;
        }
    }

    /**
     * @return 如果时TSO事务，返回insert的commitTso，该值意味着是该打标sql在binlog事件中的tso值
     */
    public long insertDdlRecord(Connection connection, Long jobId, String sqlKind, String schema, String tableName,
                                String ddlSql, String metaInfo, CdcDdlMarkVisibility visibility, String ext)
        throws SQLException {
        if (ddlSql.lastIndexOf(";") == (ddlSql.length() - 1)) {
            ddlSql = StringUtils.substringBeforeLast(ddlSql, ";");
        }

        InnerTransManager transManager = new InnerTransManager(connection);
        final String finalDdlSql = ddlSql;
        transManager.executeWithTransaction(() -> {
            try (PreparedStatement stmt = connection.prepareStatement(INSERT_CDC_DDL_RECORD)) {

                stmt.setObject(1, jobId);
                stmt.setString(2, sqlKind);
                stmt.setString(3, schema);
                stmt.setString(4, tableName);
                stmt.setString(5, finalDdlSql);
                stmt.setString(6, metaInfo);
                stmt.setObject(7, visibility.getValue());
                stmt.setString(8, ext);
                stmt.executeUpdate();
            }
        });
        if (connection instanceof InnerConnection) {
            return ((InnerConnection) connection).getCommitTso();
        } else {
            return -1L;
        }
    }

    /**
     * 老ddl引擎的job表为ddl_jobs，新ddl引擎的job表为ddl_engine，但jobId是一个全局的sequence
     * 老ddl引擎的幂等，直接通过jobId判断即可
     * 新ddl引擎的幂等，需要通过jobId和TaskId共同判断
     */
    public boolean isDdlRecordExistForJobId(Connection connection, Long jobId, Long taskId, Long taskSubSeq)
        throws SQLException {
        boolean result = false;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(String.format(QUERY_CDC_DDL_RECORD_BY_JOBID, jobId))) {
                while (rs.next()) {
                    long jobIdTemp = rs.getLong(1);
                    String extStr = rs.getString(2);
                    DDLExtInfo extInfo =
                        StringUtils.isBlank(extStr) ? null : JSONObject.parseObject(extStr, DDLExtInfo.class);

                    if (taskId != null) {
                        Assert.assertTrue(extInfo != null && extInfo.getTaskId() != null && extInfo.getTaskId() != 0L);
                        if (taskSubSeq != null) {
                            Assert.assertTrue(extInfo.getTaskSubSeq() != null && extInfo.getTaskSubSeq() > 0);
                            result |= (jobId == jobIdTemp && extInfo.getTaskId().equals(taskId)
                                && extInfo.getTaskSubSeq().equals(taskSubSeq));
                        } else {
                            result |= (jobId == jobIdTemp && extInfo.getTaskId().equals(taskId));
                        }
                    } else {
                        // extInfo如果为null，说明是之前老引擎的打标记录，包含新引擎的代码，extInfo一定不为null
                        result |= (jobId == jobIdTemp);
                    }
                }
            }
        }
        return result;
    }

    @SneakyThrows
    public boolean isFileStoreTable(String schemaName, String tableName) {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(metaDbConn);
            TablesRecord tablesRecord = tablesAccessor.query(schemaName, tableName, false);
            return tablesRecord != null && Engine.isFileStore(tablesRecord.engine)
                && !Engine.supportColumnar(Engine.of(tablesRecord.engine));
        }
    }

    public boolean isInstructionExists(ICdcManager.InstructionType instructionType,
                                       String instructionId) throws SQLException {
        try (Connection connection = new InnerConnection(SystemDbHelper.CDC_DB_NAME)) {
            try (PreparedStatement stmt = connection.prepareStatement(QUERY_CDC_INSTRUCTION_COUNT_BY_TYPE_AND_ID)) {
                stmt.setString(1, instructionType.name());
                stmt.setString(2, instructionId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        if (rs.getInt(1) > 0) {
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }

    private void alterTable() {
        // old version maybe not the index.
        try (Connection connection = new InnerConnection(SystemDbHelper.CDC_DB_NAME)) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(String
                    .format(CDC_ADD_INDEX, CdcTableUtil.CDC_DDL_RECORD_TABLE));
            }
        } catch (Throwable t) {
            //ignore
        }
    }

    private List<String> queryReadyTables() throws SQLException {
        List<String> list = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(metaDbConn);
            List<TablesRecord> tablesRecordList = tablesAccessor.query(CDC_TABLE_SCHEMA);
            for (TablesRecord tablesRecord : tablesRecordList) {
                if (tablesRecord.status == TableStatus.PUBLIC.getValue()) {
                    list.add(tablesRecord.tableName.toLowerCase());
                }
            }
        }
        return list;
    }

    private static class SysTableUtilHolder {
        private final static CdcTableUtil INSTANCE = new CdcTableUtil();
    }
}
