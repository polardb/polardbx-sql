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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterDatabaseStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterDatabaseSetOption;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.DbStatusManager;
import com.alibaba.polardbx.executor.common.DbStatusManager.DbReadOnlyStatus;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

/**
 * @author shicai.xsc 2018/9/3 下午11:36
 * @since 5.0.0.0
 */
public class DbStatusHandler extends AbstractLifecycle {

    private static int MAX_WAIT_FINISH_TIME = 5;
    private static final Logger logger = LoggerFactory.getLogger(DbStatusManager.class);

    public static void handle(String sql, ManagerConnection mc) {
        if (mc.getSchema() == null) {
            mc.writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        }

        SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(mc.getSchema());
        TDataSource dataSource = schema.getDataSource();

        long affectedRows = 0;
        List<SQLStatement> stmtList = FastsqlUtils.parseSql(sql, SQLParserFeature.IgnoreNameQuotes);
        try {
            if (!dataSource.isInited()) {
                dataSource.init();
            }

            if (stmtList != null && stmtList.size() > 0) {
                for (SQLStatement stmt : stmtList) {
                    if (stmt instanceof SQLAlterDatabaseStatement) {
                        affectedRows = handle((SQLAlterDatabaseStatement) stmt, dataSource);
                    }
                }
            }

            OkPacket ok = new OkPacket();
            ok.packetId = mc.getNewPacketId();
            ok.affectedRows = affectedRows;
            ok.serverStatus = 2;
            ok.write(PacketOutputProxyFactory.getInstance().createProxy(mc));
        } catch (Throwable e) {
            logger.error("Failed in DbStatusHandler", e);
            mc.writeErrMessage(ErrorCode.ER_SET_TABLE_READONLY, e.getMessage());
        }
    }

    private static long handle(SQLAlterDatabaseStatement x, TDataSource dataSource) throws SQLException {
        long affectedRows = 0;
        if (x.getItem() != null && x.getItem() instanceof MySqlAlterDatabaseSetOption) {
            MySqlAlterDatabaseSetOption item = (MySqlAlterDatabaseSetOption) x.getItem();
            if (item.getOptions() != null && item.getOptions().size() > 0) {
                String database = x.getName().getSimpleName();
                // currently we do not support lock group
                // String group = item.getOn() != null
                // ?item.getOn().getSimpleName() : "";

                if (!StringUtils.equalsIgnoreCase(database, dataSource.getSchemaName())) {
                    throw new SQLException("The database being locked should be the same as the connection: "
                        + dataSource.getSchemaName());
                }

                SQLAssignItem readonlyItem = item.getOptions().get(0);
                int statusValue = ((SQLIntegerExpr) readonlyItem.getValue()).getNumber().intValue();
                DbReadOnlyStatus status = getDbReadOnlyStatus(statusValue);

                int time = -1;
                if (item.getOptions().size() > 1) {
                    SQLAssignItem timeItem = item.getOptions().get(1);
                    time = ((SQLIntegerExpr) timeItem.getValue()).getNumber().intValue();
                }

                // set databse status
                affectedRows = DbStatusManager.getInstance().setDbStatus(database, status, time, null);
                if (status == DbReadOnlyStatus.READONLY_PREPARE) {
                    // wait for all writing finish
                    // and also make sure the lock not released
                    int waitTime = time > 0 ? Math.min(time, MAX_WAIT_FINISH_TIME) : MAX_WAIT_FINISH_TIME;
                    if (!waitConnectionsFinish(database, waitTime)
                        || !DbStatusManager.getInstance().isDbReadOnly(database)) {
                        String error = String.format("Failed to wait all executing write sqls to finish in %d seconds",
                            waitTime);
                        throw new SQLException(error);
                    }
                }
            }
        }

        return affectedRows;
    }

    private static boolean waitConnectionsFinish(String dbToReadOnly, int waitTime) {
        Calendar expireTime = Calendar.getInstance();
        expireTime.add(Calendar.SECOND, waitTime);

        while (Calendar.getInstance().before(expireTime)) {
            boolean executingWrite = false;
            for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
                for (FrontendConnection fc : p.getFrontends().values()) {
                    if (fc instanceof ServerConnection) {
                        ServerConnection serverConnion = (ServerConnection) fc;
                        if (serverConnion.isStatementExecuting().get()
                            && checkConnectionDoingWrite(serverConnion, dbToReadOnly)) {
                            executingWrite = true;
                            break;
                        }
                    }
                }
            }

            if (!executingWrite) {
                return true;
            }

            try {
                Thread.sleep(100);
            } catch (Exception e) {
                logger.error("Failed in waitConnectionsFinish", e);
                return false;
            }
        }

        return false;
    }

    private static boolean checkConnectionDoingWrite(ServerConnection serverConnection, String dbToReadOnly) {
        TConnection tConnection = serverConnection.getTddlConnection();
        if (tConnection != null && tConnection.getExecutionContext() != null) {
            List<PrivilegeVerifyItem> items = tConnection.getExecutionContext().getPrivilegeVerifyItems();
            if (items != null && items.size() > 0) {
                for (PrivilegeVerifyItem item : items) {
                    if (item.getPrivilegePoint() == PrivilegePoint.SELECT) {
                        continue;
                    }

                    String db = item.getDb();
                    if (StringUtils.isBlank(db)) {
                        db = serverConnection.getSchema();
                    }
                    db = SQLUtils.normalizeNoTrim(db);

                    // only check for the db being set read-only
                    if (StringUtils.equalsIgnoreCase(db, dbToReadOnly)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private static DbReadOnlyStatus getDbReadOnlyStatus(int statusValue) {
        DbReadOnlyStatus status;
        switch (statusValue) {
        case 0:
            status = DbReadOnlyStatus.WRITEABLE;
            break;
        case 1:
            status = DbReadOnlyStatus.READONLY_PREPARE;
            break;
        case 2:
            status = DbReadOnlyStatus.READONLY_COMMIT;
            break;
        default:
            status = DbReadOnlyStatus.WRITEABLE;
            break;
        }

        return status;
    }
}
