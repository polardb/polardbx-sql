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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.TrxIdGenerator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITDataSource;
import com.alibaba.polardbx.common.logical.ITStatement;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.RecycleBinManager.RecycleBinParam;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropTableHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.exception.SqlValidateException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.ddl.DropTable;
import org.apache.calcite.sql.SqlDropTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.RandomStringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RecycleBin extends AbstractLifecycle {
    public static final String tableName = GmsSystemTables.RECYCLE_BIN;
    public static final String PREFIX = "BIN_";
    public static final int SUFFIX_LENGTH = 20;
    public static final int NAME_LENGTH = PREFIX.length() + SUFFIX_LENGTH;
    public static final String FILE_STORAGE_PREFIX = "FILE_STORAGE_BIN_";
    public static final int FILE_STORAGE_BIN_NAME_LENGTH = FILE_STORAGE_PREFIX.length() + SUFFIX_LENGTH;
    private static final Logger logger = LoggerFactory.getLogger(RecycleBin.class);
    private String appName;
    private String schemaName;
    private ITDataSource dataSource;
    private DataSource metaDbDataSource;
    private Map<String, Object> cmds;

    public RecycleBin(String appName, String schemaName, ITDataSource dataSource, Map<String, Object> cmds) {
        this.appName = appName;
        this.schemaName = schemaName;
        this.dataSource = dataSource;
        this.cmds = cmds;
        this.metaDbDataSource = MetaDbDataSource.getInstance().getDataSource();
    }

    public static boolean isRecyclebinTable(String name) {
        return (name.toUpperCase().startsWith(PREFIX) && name.length() == NAME_LENGTH) ||
            (name.toUpperCase().startsWith(FILE_STORAGE_PREFIX) && name.length() == FILE_STORAGE_BIN_NAME_LENGTH);
    }

    @Override
    public void doInit() {
    }

    @Override
    public void doDestroy() {
        deleteAll();
    }

    private void initTable() {
    }

    public void flashback(String name, String renameTo) {
        RecycleBinParam param = get(name);
        if (param == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "can't find table in recycle bin");
        }

        String target = param.originalName;
        if (renameTo != null) {
            target = renameTo;
        }
        try {
            logger.info("flashback:" + appName + "," + name + "->" + renameTo);
            doExecuteUpdate(String.format("/*+TDDL:cmd_extra(FLASHBACK_RENAME=true)*/ RENAME TABLE `%s` TO `%s`", name,
                    TStringUtil.escape(target, '`', '`')),
                dataSource);
        } catch (SQLException e) {
            logger.error("flashback:" + name + "->" + renameTo, e);
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "execute rename error");
        }
        delete(name);
    }

    public void purge(boolean expired) {
        purge(expired, null, null);
    }

    public void purge(boolean expired, IRepository repo, ExecutionContext executionContext) {
        List<RecycleBinParam> result = getAll(expired);
        for (RecycleBinParam param : result) {
            purgeTable(param.name, false, repo, executionContext);
        }

    }

    public void purgeTable(String name, boolean check, IRepository repo, ExecutionContext executionContext) {
        RecycleBinParam param = get(name);

        if (check && param == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "can't find table in recycle bin");
        }

        String hint = "";
        if (param.name.startsWith(FILE_STORAGE_PREFIX)) {
            if (check) {
                hint = "/*+TDDL: " + ConnectionProperties.PURGE_FILE_STORAGE_TABLE + "=true */";
            } else {
                // background do not purge oss table
                return;
            }
        }

        try {
            logger.info("purge table:" + appName + "," + name + "," + check);

            if (repo != null) {
                // Execute a DROP TABLE under current context.
                if (TStringUtil.isNotEmpty(hint)) {
                    executionContext.getExtraCmds().put(ConnectionProperties.PURGE_FILE_STORAGE_TABLE, Boolean.TRUE);
                }
                LogicalDropTable logicalDropTable = genLogicalDropTablePlan(name, executionContext);
                String sql = String.format(" drop table IF EXISTS `%s` purge", name);
                executionContext.setOriginSql(sql);
                executeDropTable(logicalDropTable, repo, executionContext);
            } else {
                // Execute a DROP TABLE with a new TConnection.
                doExecuteUpdate(String.format(hint + " drop table IF EXISTS `%s` purge", name), dataSource);
            }

            delete(name);
        } catch (SQLException e) {
            logger.error("purge table:" + appName + "," + name + "," + check, e);
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "purge table error");
        } catch (SqlValidateException e) {
            logger.error("purge table:" + appName + "," + name + "," + check, e);
            // to fix, drop table if exists throw exception when table not exists
            if (e.getMessage().indexOf("ERR_CANNOT_FETCH_TABLE_META") > 0) {
                delete(name);
            }
        }
    }

    public List<RecycleBinParam> getAll(boolean expired) {
        String where = "where `schema_name` = '" + schemaName + "'";
        String sql = String
            .format("select `gmt_create`,`name`, `original_name` from %s %s order by id asc", tableName, where);
        if (expired) {
            long retainHours = GeneralUtil.getPropertyLong(cmds,
                ConnectionProperties.RECYCLEBIN_RETAIN_HOURS,
                TddlConstants.DEFAULT_RETAIN_HOURS);
            where = "`schema_name` = '" + schemaName + "' and";
            sql = String.format("select `gmt_create`,`name`, `original_name` from %s where %s `gmt_create` < "
                    + "DATE_SUB(now(), INTERVAL %d HOUR) order by id asc",
                tableName, where, retainHours);
        }
        try {
            return doExecuteQueryOnMeta(sql);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "get from recycle bin error", e);
        }
    }

    public RecycleBinParam get(String name) {
        String where = "`schema_name` = '" + schemaName + "' and";
        String sql = "select `gmt_create`,`name`, `original_name` from %s where %s `name`='%s'";
        try {
            List<RecycleBinParam> result =
                doExecuteQueryOnMeta(String.format(sql, tableName, where, name.toUpperCase()));
            if (result == null || result.size() == 0) {
                return null;
            } else {
                return result.get(0);
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "get from recycle bin error", e);
        }
    }

    public void delete(String name) {
        String where = "`schema_name` = '" + schemaName + "' and";
        String sql = "delete from %s where %s `name`='%s'";
        try {
            String deleteSql = String.format(sql, tableName, where, name.toUpperCase());
            DdlMetaLogUtil.logSql(deleteSql);
            doExecuteUpdate(deleteSql, metaDbDataSource);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "delete from recycle bin error", e);
        }
    }

    public void deleteAll() {
        String where = "`schema_name` = '" + schemaName + "' and";
        String sql = "delete from %s where %s 1 = 1";
        try {
            String deleteAllSql = String.format(sql, tableName, where);
            DdlMetaLogUtil.logSql(deleteAllSql);
            doExecuteUpdate(deleteAllSql, metaDbDataSource);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "delete from recycle bin error", e);
        }
    }

    public void add(String name, String originalName) {
        String schemaColumn = ", `schema_name`";
        String schemaValue = ", '" + schemaName + "'";
        String sql = "insert into %s (`gmt_create`,`name`, `original_name` %s) value (%s,'%s',%s %s)";
        try {
            logger.info("bind recycle table:" + appName + "," + name + "," + originalName);
            String insertSql = String.format(sql, tableName, schemaColumn, "now()",
                name.toUpperCase(), TStringUtil.quoteString(originalName), schemaValue);
            DdlMetaLogUtil.logSql(insertSql);
            doExecuteUpdate(insertSql, metaDbDataSource);
        } catch (SQLException e) {
            logger.error("bind recycle table:" + appName + "," + name + "," + originalName, e);
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "insert into recycle bin error", e);
        }
    }

    public String genName() {
        return PREFIX + RandomStringUtils.randomAlphanumeric(SUFFIX_LENGTH).toUpperCase();
    }

    public String genFileStorageBinName() {
        return FILE_STORAGE_PREFIX + RandomStringUtils.randomAlphanumeric(SUFFIX_LENGTH).toUpperCase();
    }

    public boolean hasForeignConstraint(String appName, String tableName) {
        ITConnection connection = null;
        ITStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            connection.setTrxId(TrxIdGenerator.getInstance().nextId());
            String sql =
                "select `table_name` from information_schema.key_column_usage where referenced_table_name = %s and "
                    + "table_schema= %s and referenced_table_schema = %s";
            ResultSet resultSet = statement.executeQuery(String
                .format(sql, TStringUtil.quoteString(tableName), TStringUtil.quoteString(appName),
                    TStringUtil.quoteString(appName)));
            List<String> names = new ArrayList<>();
            while (resultSet.next()) {
                names.add(resultSet.getString("table_name"));
            }
            resultSet.close();
            boolean found = false;
            for (String name : names) {
                if (!isRecyclebinTable(name)) {
                    found = true;
                    break;
                }
            }
            return found;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "get table foreign constraint error", e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    //do nothing
                }
            }
        }
    }

    private List<RecycleBinParam> doExecuteQueryOnMeta(String sql) throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = metaDbDataSource.getConnection();
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            List<RecycleBinParam> result = new ArrayList<>();
            while (resultSet.next()) {
                RecycleBinParam param = new RecycleBinParam(resultSet.getString("name"),
                    resultSet.getString("original_name"),
                    resultSet.getTimestamp("gmt_create"));
                result.add(param);
            }
            resultSet.close();
            return result;
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void doExecuteUpdate(String sql, ITDataSource dataSource)
        throws SQLException, TddlNestableRuntimeException {
        ITConnection connection = null;
        ITStatement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void doExecuteUpdate(String sql, DataSource dataSource)
        throws SQLException, TddlNestableRuntimeException {
        Connection connection = null;
        Statement statement = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            statement.executeUpdate(sql);
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void executeDropTable(LogicalDropTable logicalDropTable, IRepository repo,
                                  ExecutionContext executionContext) {
        LogicalDropTableHandler logicalDropTableHandler = new LogicalDropTableHandler(repo);
        logicalDropTableHandler.handle(logicalDropTable, executionContext);
    }

    private LogicalDropTable genLogicalDropTablePlan(String logicalTableName, ExecutionContext executionContext) {
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlIdentifier logicalTableNameNode = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);

        SqlDropTable sqlDropTable = new SqlDropTable(SqlParserPos.ZERO, true, logicalTableNameNode, true);
        sqlDropTable = (SqlDropTable) sqlDropTable.accept(visitor);

        RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        DropTable dropTable = DropTable.create(cluster, sqlDropTable, logicalTableNameNode);
        executionContext.setOriginSql(dropTable.toString());

        LogicalDropTable logicalDropTable = LogicalDropTable.create(dropTable);
        logicalDropTable.setSchemaName(schemaName);

        return logicalDropTable;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public ITDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(ITDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Map<String, Object> getCmds() {
        return cmds;
    }

    public void setCmds(Map<String, Object> cmds) {
        this.cmds = cmds;
    }
}
