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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.util.TimeUtil;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class ShowConnectionSyncAction implements ISyncAction {

    private String user;

    private String schema;

    public ShowConnectionSyncAction(String user, String schema) {
        this.user = user;
        this.schema = schema;
    }

    public ShowConnectionSyncAction() {

    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("RULE");
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("HOST", DataTypes.StringType);
        result.addColumn("PORT", DataTypes.IntegerType);
        result.addColumn("LOCAL_PORT", DataTypes.IntegerType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("CHARSET", DataTypes.StringType);
        result.addColumn("NET_IN", DataTypes.LongType);
        result.addColumn("NET_OUT", DataTypes.LongType);
        result.addColumn("ALIVE_TIME(S)", DataTypes.LongType);
        result.addColumn("LAST_ACTIVE", DataTypes.LongType);
        result.addColumn("CHANNELS", DataTypes.IntegerType);
        result.addColumn("TRX", DataTypes.IntegerType);
        result.addColumn("NEED_RECONNECT", DataTypes.IntegerType);
        result.addColumn("PARTITION_HINT", DataTypes.StringType);

        for (NIOProcessor p : CobarServer.getInstance().getProcessors()) {
            for (FrontendConnection fc : p.getFrontends().values()) {
                if (fc instanceof ServerConnection) {
                    int count = 0;
                    int trx = 0;
                    int needReconnect = 0;
                    ServerConnection sc = (ServerConnection) fc;
                    if (sc.getTddlConnection() != null) {
                        if (sc.getTddlConnection().getConnectionHolder() == null) {
                            count = 0;
                        } else {
                            count = sc.getTddlConnection().getConnectionHolder().getAllConnection().size();
                            if (sc.getTddlConnection().getTrx() != null
                                && !sc.getTddlConnection().getTrx().isClosed()) {
                                // 检查下事务是否关闭
                                trx = 1;
                            }
                        }
                    }
                    if (fc.isNeedReconnect()) {
                        needReconnect = 1;
                    }

                    result.addRow(
                        new Object[] {
                            fc.getId(), fc.getHost(), fc.getPort(), fc.getLocalPort(), fc.getSchema(),
                            fc.getResultSetCharset(), fc.getNetInBytes(), fc.getNetOutBytes(),
                            (TimeUtil.currentTimeMillis() - fc.getStartupTime()) / 1000L,
                            (System.nanoTime() - sc.getLastActiveTime()) / (1000 * 1000), count, trx,
                            needReconnect, sc.getPartitionHint()});

                }
            }
        }

        return result;
    }
}
