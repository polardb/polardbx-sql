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

package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.ServerInfoAccessor;
import com.alibaba.polardbx.gms.topology.ServerInfoRecord;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class ServerInfoHelper {

    public static List<ServerInfoRecord> getServerInfoByMetaDb(String localIp, Integer serverPort, String instId) {
        ServerInfoAccessor serverInfoAcc = new ServerInfoAccessor();
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            serverInfoAcc.setConnection(connection);
            List<ServerInfoRecord> serverInfos = new ArrayList<>();
            if (instId != null) {
                serverInfos = serverInfoAcc.getServerInfoByAddrAndInstId(localIp, serverPort, instId);
            } else {
                serverInfos = serverInfoAcc.getServerInfoByAddr(localIp, serverPort);
            }
            return serverInfos;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }
}
