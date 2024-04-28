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

package com.alibaba.polardbx.server.util;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.metadb.table.TablesRecord;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.server.conn.InnerConnection;
import com.alibaba.polardbx.server.conn.InnerTransManager;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class SysTableUtil {

    private static final String SHOW_DDL = "show ddl";
    private static final String RECOVER_JOB = "recover ddl %s";
    private static final String JOB_STATE_RUNNING = "RUNNING";
    private static final String JOB_STATE_PENDING = "PENDING";

    private final static Logger logger = LoggerFactory.getLogger(SysTableUtil.class);

    public static List<String> queryReadyTables(String sysSchema) throws SQLException {
        List<String> list = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            TablesAccessor tablesAccessor = new TablesAccessor();
            tablesAccessor.setConnection(metaDbConn);
            List<TablesRecord> tablesRecordList = tablesAccessor.query(sysSchema);
            for (TablesRecord tablesRecord : tablesRecordList) {
                if (tablesRecord.status == TableStatus.PUBLIC.getValue()) {
                    list.add(tablesRecord.tableName.toLowerCase());
                }
            }
        }
        return list;
    }

    public static void prepareTable(String sysSchema, String tableName, String createSql) {
        try (Connection connection = new InnerConnection(sysSchema)) {
            boolean needCreate = true;
            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("show tables")) {
                    while (rs.next()) {
                        String value = rs.getString(1);
                        if (StringUtils.equalsIgnoreCase(value, tableName)) {
                            needCreate = false;
                            break;
                        }
                    }
                }
            }

            if (needCreate) {
                InnerTransManager transManager = new InnerTransManager(connection);
                transManager.executeWithTransaction(() -> {
                    try (Statement stmt = connection.createStatement()) {
                        logger.warn("Prepare to create " + sysSchema + " system table :" + tableName);
                        stmt.executeUpdate(createSql);
                        logger.warn("Successfully Created system table: " + tableName + " in " + sysSchema);
                    }
                });
            }
        } catch (Throwable t) {
            throw GeneralUtil.nestedException("init " + sysSchema + " system tables failed.", t);
        }
    }

    /**
     * CDC系统表在创建过程中如果有DN节点不可用, Job未执行成功进程便退出
     * 再次启动时上次残留的job还未清理, 会导致本地启动失败
     * 可以使用recover ddl来恢复残留的PENDING Job
     */
    public static void autoRecover() {
        try (Connection connection = new InnerConnection(SystemDbHelper.CDC_DB_NAME)) {
            try (Statement stmt = connection.createStatement()) {
                ResultSet rs = stmt.executeQuery(SHOW_DDL);
                while (rs.next()) {
                    long jobId = rs.getLong(1);
                    String objectSchema = rs.getString(2);
                    String jobState = rs.getString(6);

                    if (objectSchema.equals(SystemDbHelper.CDC_DB_NAME)) {
                        // CDC系统表Job任务残留，使用recover ddl语句恢复
                        if (jobState.equals(JOB_STATE_PENDING)) {
                            logger.warn("DDL job is pending, try to recover ddl job");
                            String recoverDDL = String.format(RECOVER_JOB, jobId);
                            stmt.executeQuery(recoverDDL);
                        } else if (jobState.equals(JOB_STATE_RUNNING)) {
                            logger.warn("DDL job is running, wait for the ddl job to complete");
                        } else {
                            logger.warn("DDL job state: " + jobState + ", will sleep and retry");
                        }
                    } else {
                        logger.error("Unexpected DDL job, objectSchema: " + objectSchema);
                    }
                }
            }
        } catch (SQLException throwables) {
            logger.error("SQL Exception in AutoRecover", throwables);
        }
    }
}
