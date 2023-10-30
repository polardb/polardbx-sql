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

package com.alibaba.polardbx.manager.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.sync.DdlRequestSyncAction;
import com.alibaba.polardbx.executor.sync.ddl.RemoteDdlTaskSyncAction;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager.MetaDbConfigSyncAction;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.manager.ManagerConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.matrix.jdbc.utils.TDataSourceInitUtils;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.server.executor.utils.ResultSetUtil;
import com.alibaba.polardbx.server.response.privileges.AbstractAuthorizeSyncAction;
import org.apache.commons.lang.StringUtils;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author agapple 2015年3月26日 下午7:05:19
 * @since 5.1.19
 */
public class SyncHandler {

    public static void handle(String sql, ManagerConnection c, int offset) {
        String str = sql.substring(offset).trim();
        String schema = StringUtils.substringBefore(str, " ");
        String data = StringUtils.substringAfter(str, " ");

        int length = schema.length();
        if (length > 0) {
            if (schema.charAt(0) == '`' && schema.charAt(length - 1) == '`') {
                schema = schema.substring(1, length - 1);
            }
        }

        Object obj = JSON.parse(data);
        if (obj instanceof IGmsSyncAction) {
            IGmsSyncAction action = (IGmsSyncAction) obj;

            ResultSet rs = null;
            boolean actionDone = false;

            if (ConfigDataMode.isPolarDbX() && action instanceof MetaDbConfigSyncAction) {
                MetaDbConfigSyncAction configAction = (MetaDbConfigSyncAction) action;
                String dbInfoDataId = MetaDbDataIdBuilder.getDbInfoDataId();
                if (TStringUtil.equalsIgnoreCase(configAction.getDataId(), dbInfoDataId)) {
                    ResultCursor rc = (ResultCursor) configAction.sync();
                    rs = rc != null ? new TResultSet(rc, null) : null;
                    actionDone = true;
                }
            }

            SchemaConfig schemaConfig = CobarServer.getInstance().getConfig().getSchemas().get(schema);
            if (schemaConfig == null) {
                c.writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + schema + "'");
                return;
            }

            TDataSource dataSource = schemaConfig.getDataSource();
            // 如果为授权相关的同步action需要显示的初始化数据源
            if (isAuthorizeRelatedSyncAction(action) || isDdlJobRequest(action)) {
                if (dataSource != null) {
                    TDataSourceInitUtils.initDataSource(dataSource);
                }
            }
            if ((dataSource.isInited() || dataSource.isDefaultDb()) && !actionDone) {
                // 如果当前server已经启动了这个dataSource
                ExecutorContext.setContext(schema, dataSource.getConfigHolder().getExecutorContext());
                OptimizerContext.setContext(dataSource.getConfigHolder().getOptimizerContext());
                ResultCursor rc = (ResultCursor) action.sync();

                if (rc != null) {
                    rs = new TResultSet(rc, null);
                } else {
                    rs = null;
                }
            }

            // 返回ok包
            IPacketOutputProxy buffer;

            if (rs == null) {
                PacketOutputProxyFactory.getInstance().createProxy(c).writeArrayAsPacket(OkPacket.OK);
            } else {
                try {
                    buffer = ResultSetUtil.resultSetToPacket(rs, c.getCharset(), c, new AtomicLong(), null,
                        ResultSetUtil.NO_SQL_SELECT_LIMIT);
                    int statusFlags = 2;
                    ResultSetUtil.eofToPacket(buffer, c, statusFlags);
                } catch (Exception e) {
                    throw GeneralUtil.nestedException(e);
                }

            }
        } else {
            c.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement : " + data);
        }
    }

    /**
     * 是否为授权相关的同步action
     */
    private static boolean isAuthorizeRelatedSyncAction(IGmsSyncAction action) {
        return action instanceof AbstractAuthorizeSyncAction;
    }

    /**
     * DDL Job Engine relies on leader node to perform a job, so we should
     * trigger the leader initialization in case it is still inactive.
     */
    private static boolean isDdlJobRequest(IGmsSyncAction action) {
        if (ConfigDataMode.isPolarDbX()) {
            return action instanceof DdlRequestSyncAction
                || action instanceof RemoteDdlTaskSyncAction;
        }
        return false;
    }
}
